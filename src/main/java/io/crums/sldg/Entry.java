/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.function.UnaryOperator;

import io.crums.io.buffer.BufferUtils;

/**
 * Source of a row's {@linkplain Row#inputHash() input hash}. Note this may not
 * always be practical. For example, if the source is the contents of a very large file,
 * then an instance may not comfortably fit in memory.
 * <p>
 * Instances are immutable and safe under concurrent access.
 * </p>
 * 
 * @see #identityHash(ByteBuffer, long)
 * @see #ruleBased(ByteBuffer, long, UnaryOperator)
 */
public class Entry {
  
  private final long rowNumber;
  private final ByteBuffer contents;
  
  /**
   * Base constructor. <em>Note the data is <b>not</b> defensively copied.</em>
   * 
   * @param contents non-zero remaining bytes,
   *                contents not-to-be-modified since it's not copied
   * @param rowNumber &ge; 1
   */
  public Entry(ByteBuffer contents, long rowNumber) {
    this.rowNumber = rowNumber;
    this.contents = BufferUtils.readOnlySlice(
        Objects.requireNonNull(contents, "null contents"));
    
    if (rowNumber <= 0)
      throw new IllegalArgumentException("row number " + rowNumber);
    
    if (!this.contents.hasRemaining())
      throw new IllegalArgumentException("empty contents");
  }
  
  
  public Entry(ByteBuffer contents, EntryInfo info) {
    this.rowNumber = info.rowNumber();
    this.contents = BufferUtils.readOnlySlice(
        Objects.requireNonNull(contents, "null contents"));
    
    if (this.contents.remaining() != info.size())
      throw new IllegalArgumentException(
          "contents remaining / info size mistmatch: " + this.contents.remaining() + " , " + info);
  }
  
  
  /**
   * Copy constructor. Copy constructors are more efficeint. Their primary
   * use is in {@linkplain #reNumber(long)}-ing.
   * 
   * @param copy non-null
   * @param rowNumber &ge; 1
   */
  public Entry(Entry copy, long rowNumber) {
    this.rowNumber = rowNumber;
    this.contents = copy.contents;
    
    if (rowNumber <= 0)
      throw new IllegalArgumentException("row number " + rowNumber);
  }
  
  /**
   * Returns a read-only view of the contents.
   */
  public final ByteBuffer contents() {
    return contents.asReadOnlyBuffer();
  }
  
  
  /**
   * Returns the entry's row number in the ledger.
   */
  public final long rowNumber() {
    return rowNumber;
  }
  
  
  /**
   * Returns this entry, but with a possibly new row number.
   * 
   * @param rowNumber &ge; 1
   * 
   * @return a new instance of this class if {@code rowNumber} is not the
   * same as this instance; this instance, otherwise.
   */
  public Entry reNumber(long rowNumber) {
    return rowNumber == this.rowNumber ? this : new Entry(this, rowNumber);
  }
  
  
  /**
   * Returns the 32-byte hash of the {@linkplain #contents() contents}.
   * This may be a straight digest of it may be somehow structured and is left
   * open for subclasses. 
   * 
   * @return a read-only, 32-byte wide buffer
   */
  public ByteBuffer hash() {
    MessageDigest digest = SldgConstants.DIGEST.newDigest();
    digest.update(contents());
    return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
  }
  
  
  
  
  
  // - - -  S T A T I C   M E M B E R S  - - -
  
  
  /**
   * Creates a new {@linkplain IdentityHash} instance. (Less verbose than {@code new}).
   * 
   * @param contents non-zero remaining bytes,
   *                contents not-to-be-modified since it's not copied
   * @param rowNumber &ge; 1
   */
  public static Entry identityHash(ByteBuffer hash, long rowNumber) {
    return new IdentityHash(hash, rowNumber);
  }
  
  
  /**
   * Creates new {@linkplain RuleBased} instance. (Less verbose than {@code new}).
   * 
   * 
   * @param contents non-zero remaining bytes,
   *                contents not-to-be-modified since it's not copied
   * @param rowNumber &ge; 1
   * @param hashOp hashing operator always returns a 32-byte sequence
   */
  public static Entry ruleBased(ByteBuffer contents, long rowNumber, UnaryOperator<ByteBuffer> hashOp) {
    return new RuleBased(contents, rowNumber, hashOp);
  }
  
  
  /**
   * Creates new {@linkplain RuleBased} instance. (Less verbose than {@code new}).
   * 
   * 
   * @param contents non-zero remaining bytes,
   *                contents not-to-be-modified since it's not copied
   * @param rowNumber &ge; 1
   * @param hashOp hashing operator always returns a 32-byte sequence
   */
  public static Entry ruleBased(ByteBuffer contents, EntryInfo info, UnaryOperator<ByteBuffer> hashOp) {
    return new RuleBased(contents, info, hashOp);
  }
  
  
  
  
  
  
  
  
  /**
   * Entry with rule-based hashing.
   */
  public static class RuleBased extends Entry {
    
    private final UnaryOperator<ByteBuffer> hashOp;
    
    /**
     * Base constructor.
     * 
     * @param contents the non-zero remaining bytes
     * @param rowNumber &ge; 1
     * @param hashOp
     */
    public RuleBased(ByteBuffer contents, long rowNumber, UnaryOperator<ByteBuffer> hashOp) {
      super(contents, rowNumber);
      this.hashOp = Objects.requireNonNull(hashOp, "null hashOp");
    }
    
    /**
     * Base constructor.
     * 
     * @param contents the non-zero remaining bytes
     * @param rowNumber &ge; 1
     * @param hashOp
     */
    public RuleBased(ByteBuffer contents, EntryInfo info, UnaryOperator<ByteBuffer> hashOp) {
      super(contents, info);
      this.hashOp = Objects.requireNonNull(hashOp, "null hashOp");
    }
    
    /**
     * Copy constructor, sort of.
     */
    public RuleBased(RuleBased copy, long rowNumber) {
      super(copy, rowNumber);
      this.hashOp = copy.hashOp;
    }
    
    
    @Override
    public ByteBuffer hash() {
      return hashOp.apply(contents());
    }
    
    @Override
    public RuleBased reNumber(long rowNumber) {
      return rowNumber == rowNumber() ? this : new RuleBased(this, rowNumber);
    }
  }
  
  /**
   * The entry is the hash itself (remains opaque).
   */
  public static class IdentityHash extends Entry {

    /**
     * Validating constructor.
     * 
     * @param hash exactly 32 bytes remaining
     * @param rowNumber
     */
    public IdentityHash(ByteBuffer hash, long rowNumber) {
      super(hash, rowNumber);
      checkRemaining(hash);
    }

    /**
     * Validating constructor.
     * 
     * @param hash exactly 32 bytes remaining
     * @param rowNumber
     */
    public IdentityHash(ByteBuffer hash, EntryInfo info) {
      super(hash, info);
      checkRemaining(hash);
    }
    
    
    
    private void checkRemaining(ByteBuffer hash) {
      if (hash.remaining() != SldgConstants.HASH_WIDTH)
        throw new IllegalArgumentException(
            "contents must be 32 bytes (the hash width), actual: " + hash);
    }
    
    

    /**
     * Copy constructor, sort of.
     */
    public IdentityHash(IdentityHash copy, long rowNumber) {
      super(copy, rowNumber);
    }
    

    @Override
    public final ByteBuffer hash() {
      return contents();
    }
    
    @Override
    public IdentityHash reNumber(long rowNumber) {
      return rowNumber == rowNumber() ? this : new IdentityHash(this, rowNumber);
    }
  }

}

  
  
  
  
  
  
  
  
  
  
  
  