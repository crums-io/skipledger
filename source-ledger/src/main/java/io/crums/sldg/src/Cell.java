/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.salt.TableSalt;

/**
 * A cell in a "tabular" row.
 */
public abstract class Cell {
  
  /** No implementations outside this file. */
  private Cell() {  }
  
  /**
   * Returns the cell hash.
   * 
   * @return {@code hash( DIGEST.newDigest() )}
   * @see #hash(MessageDigest)
   */
  public ByteBuffer hash() {
    return hash(DIGEST.newDigest());
  }
  
  /**
   * Returns the cell hash, using the given work digest to calculate it,
   * if necessary. If the cell is not salted, then this is just the hash of
   * the {@linkplain #data() data}; if it is salted ({@linkplain #hasSalt()})
   * then it is hash of the concatenation of the {@linkplain #salt() salt}
   * followed by the {@linkplain #data() data}.
   * 
   * @param digest      SHA-256 digester
   * 
   * @return    32-byte hash
   */
  public abstract ByteBuffer hash(MessageDigest digest);
  
  
  /**
   * Determines whether the cell's data is <em>not redacted</em>.
   */
  public abstract boolean hasData();
  
  
  /**
   * Returns the number of bytes in the cell's {@linkplain #data() data}
   * (may be zero), if it {@linkplain #hasData() has any}; -1, otherwise.
   * 
   * @return &ge; -1
   */
  public abstract int dataSize();
  
  /**
   * Determines whether the cell value is redacted.
   * Redacted cells only have {@linkplain #hash() hash}.
   * 
   * @return {@code !hasData()}
   */
  public final boolean isRedacted() {
    return !hasData();
  }
  
  
  /**
   * Returns the cell's value in raw form. Whatever its semantics (type, etc.),
   * this is the byte sequence used to represent the value, which figures in
   * the calculation of the cell's hash.
   * 
   * @return a read-only buffer
   * 
   * @throws NoSuchElementException  if {@linkplain #hasData()} returns {@code false}
   * 
   * @see #getData()
   * @see #hasData()
   */
  public abstract ByteBuffer data() throws NoSuchElementException;
  
  
  /**
   * Returns the cell value as an optional.
   * 
   * @see #data()
   * @see #hasData()
   */
  public Optional<ByteBuffer> getData() {
    return hasData() ? Optional.of(data()) : Optional.empty();
  }
  
  
  /**
   * Determines whether cell value has salt. Redacted cells do not
   * have salt.
   */
  public abstract boolean hasSalt();
  
  
  /**
   * Returns cell salt (if any), or any empty buffer, otherwise.
   * 
   * @see #hasSalt()
   */
  public abstract ByteBuffer salt();
  
  
  
  /**
   * Returns a redacted version of this instance; {@code this}
   * instance, if already redacted.
   * 
   * @return {@code this} or a {@linkplain Redacted} instance
   */
  public final Cell redact() {
    return hasData() ? new Redacted(hash(), false) : this;
  }
  
  
  
  /**
   * Defined because we lazy-load stuff in such things as lists
   * of cells ({@code List<Cell>}): the {@code List} interface
   * specifies consistent equality semantics.
   * 
   * @return {@code true} iff the argument is an instance of this
   * class with the same information (data / salt / hash, etc.).
   */
  public final boolean equals(Object o) {
    
    return o == this ||
        o instanceof Cell other &&
        dataSize() == other.dataSize() &&
        hasSalt() == other.hasSalt() &&
        hash().equals(other.hash());
  }
  
  
  /**
   * Consistent with {@linkplain #equals(Object)}. This method is
   * defined only for completeness. <em>Avoid using in hash tables / sets</em>:
   * this operation is expensive.
   */
  @Override
  public final int hashCode() {
    return hash().hashCode();
  }
  
  
  
  
  
  
  
  
  /**
   * Redacted cell. This only records the cell's hash.
   */
  public final static class Redacted extends Cell {
    
    /**
     * Loads and returns a redacted cell instance.
     * On return, the buffer's position is advanced by 32 bytes.
     * 
     * @param in        buffer with at least 32 bytes remaining:
     *                  <em>do not modify contents!</em>
     */
    public static Cell load(ByteBuffer in) {
      return new Redacted(BufferUtils.slice(in, HASH_WIDTH), false);
    }
    
    private final ByteBuffer hash;
    
    
    Redacted(ByteBuffer hash, boolean dummy) {
      this.hash = hash;
    }
    
    /**
     * 
     * @param hash      with exactly 32-bytes remaining. Sliced, not copied;
     *                  <em>do not modify contents!</em>
     */
    public Redacted(ByteBuffer hash) {
      this.hash = hash.slice();
      if (this.hash.remaining() != HASH_WIDTH)
        throw new IllegalArgumentException(
            "argument must have exactly " + HASH_WIDTH + " remaining bytes: " +
            hash);
    }
    /** @return the pre-computed hash */
    @Override
    public ByteBuffer hash() {
      return hash.asReadOnlyBuffer();
    }
    /**
     * @return {@code hash()}
     */
    @Override
    public ByteBuffer hash(MessageDigest digest) {
      return hash();
    }
    
    /**
     * Redacted instances have no data.
     * 
     * @return false
     */
    @Override
    public boolean hasData() {
      return false;
    }
    
    /**
     * Returns -1, per the spec.
     * @return -1
     */
    @Override
    public int dataSize() {
      return -1;
    }
    /**
     * @throws NoSuchElementException on invocation
     */
    @Override
    public ByteBuffer data() throws NoSuchElementException {
      throw new NoSuchElementException("redacted instance");
    }
    /**
     * Redacted instances do not expose salt,
     * even if the {@linkplain #hash()} was computed with salt.
     * 
     * @return false
     */
    @Override
    public boolean hasSalt() {
      return false;
    }
    /**
     * Returns an empty buffer,
     * even if the {@linkplain #hash()} was computed with salt.
     */
    @Override
    public ByteBuffer salt() {
      return BufferUtils.NULL_BUFFER;
    }
    
  }
  
  
  
  
  
  
  
  
  private static abstract class Revealed extends Cell {

    /** @return true */
    @Override
    public final boolean hasData() {
      return true;
    }
    
  }
  
  
  private static abstract class SaltedReveal extends Revealed {

    /** @return true */
    @Override
    public final boolean hasSalt() {
      return true;
    }
    
  }
  
  private static abstract class Null extends Revealed {

    private final static ByteBuffer ZERO = ByteBuffer.allocate(1).asReadOnlyBuffer();
    
    private final static ByteBuffer UNSALTED_HASH;
    
    static {
      var digest = DIGEST.newDigest();
      digest.update((byte) 0);
      UNSALTED_HASH = ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }
    
    ByteBuffer unsaltedHash() {
      return UNSALTED_HASH.duplicate();
    }
    
    @Override
    public int dataSize() {
      return 1;
    }

    @Override
    public ByteBuffer data() {
      return ZERO.duplicate();
    }
    
  }
  
  /** The null, unsalted cell. */
  public final static Cell UNSALTED_NULL =
      new Null() {
    
        @Override
        public ByteBuffer hash() {
          return unsaltedHash();
        }

        @Override
        public ByteBuffer hash(MessageDigest digest) {
          return unsaltedHash();
        }

        @Override
        public boolean hasSalt() {
          return false;
        }

        @Override
        public ByteBuffer salt() {
          return BufferUtils.NULL_BUFFER;
        }
        
      };
  
  /** Salted null cell, sans row salt. */
  public final static class SaltedNull extends Null {
    
    /** Loads an instance, reading the cell salt (32-bytes). */
    public static Cell load(ByteBuffer in) {
      return new SaltedNull(BufferUtils.slice(in, HASH_WIDTH));
    }
    
    private final ByteBuffer salt;
    
    private SaltedNull(ByteBuffer salt) {
      this.salt = salt;
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      digest.reset();
      digest.update(salt());
      digest.update((byte) 0);
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }

    @Override
    public boolean hasSalt() {
      return true;
    }

    @Override
    public ByteBuffer salt() {
      return salt.asReadOnlyBuffer();
    }
    
  }
  
  /**
   * Revealed salted cell, sans row salt.
   */
  public final static class SaltedCell extends SaltedReveal {
    
    
    /**
     * 
     */
    public static Cell load(ByteBuffer in, int dataSize) {
      if (dataSize < 0)
        throw new IllegalArgumentException("negative dataSize: " + dataSize);
      return new SaltedCell(BufferUtils.slice(in, HASH_WIDTH + dataSize), false);
    }
    
    
    private final ByteBuffer saltData;
    
    private ByteBuffer saltData() {   return saltData.asReadOnlyBuffer(); }

    /**
     * @param data      positioned at zero, limit == remaining
     */
    private SaltedCell(ByteBuffer saltData, boolean dummy) {
      this.saltData = saltData;
      assert saltData.remaining() == saltData.capacity();
    }
    
    /**
     * @param saltData  sliced, not copied. <em>Do not modify contents!</em>
     */
    public SaltedCell(ByteBuffer saltData) {
      this.saltData = saltData.slice();
      if (this.saltData.remaining() < HASH_WIDTH)
        throw new IllegalArgumentException(
            "too few remaining bytes: " + saltData);
    }
    

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      digest.reset();
      digest.update(saltData());
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }
    /** @return &ge; 0 (since it the instance has data) */
    @Override
    public int dataSize() {
      return saltData.remaining() - HASH_WIDTH;
    }

    @Override
    public ByteBuffer data() {
      return saltData().position(HASH_WIDTH).slice();
    }

    /** @return a 32-byte buffer */
    @Override
    public ByteBuffer salt() {
      return saltData().limit(HASH_WIDTH).slice();
    }
    
  }
  

  static abstract class RowSaltedReveal extends SaltedReveal {
    
    final int index;
    private final ByteBuffer rowSalt;
    
    RowSaltedReveal(int index, ByteBuffer rowSalt) {
      this.index = index;
      this.rowSalt = rowSalt;
    }
    
    protected ByteBuffer rowSalt() {  return rowSalt.asReadOnlyBuffer(); }
    
    
    public byte[] salt(MessageDigest digest) {
      return TableSalt.cellSalt(rowSalt(), index, digest);
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      byte[] salt = salt(digest);
      digest.reset();
      digest.update(salt);
      digest.update(data());
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer salt() {
      return ByteBuffer.wrap(salt(DIGEST.newDigest())).asReadOnlyBuffer();
    }
    
    
  }
  
  
  public final static class RowSaltedNull extends RowSaltedReveal {
    
    private final static ByteBuffer Z = ByteBuffer.wrap(new byte[1]).asReadOnlyBuffer();
    
    RowSaltedNull(ByteBuffer rowSalt, int index) {
      super(index, rowSalt);
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      byte[] salt = salt(digest);
      digest.reset();
      digest.update(salt);
      digest.update((byte) 0);
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }

    @Override
    public int dataSize() {
      return 1;
    }

    @Override
    public ByteBuffer data() {
      return Z.duplicate();
    }
  }
  
  
  
  public final static class RowSaltedCell extends RowSaltedReveal {
    
    
    private final ByteBuffer data;
    
    
    
    

    RowSaltedCell(ByteBuffer rowSalt, int index, ByteBuffer data, boolean dummy) {
      super(index, rowSalt);
      this.data = data;
      assert checkArgs(rowSalt, index, data);
    }
    
    
    public RowSaltedCell(ByteBuffer rowSalt, int index, ByteBuffer data) {
      this(rowSalt.slice(), index, data.slice(), false);
      if (!checkArgs(rowSalt(), index, data())) {
        throw new IllegalArgumentException(
            "illegal arguments rowSalt: %s, index: %d, data: %s"
            .formatted(rowSalt, index, data));
      }
    }
    
    private boolean checkArgs(
        ByteBuffer rowSalt, int index, ByteBuffer data) {
      return
          rowSalt.remaining() == HASH_WIDTH &&
          rowSalt.capacity() == HASH_WIDTH &&
          index >= 0 &&
          data.remaining() == data.capacity();
    }
    
    
    @Override
    public ByteBuffer data() {
      return data.asReadOnlyBuffer();
    }
    
//    public byte[] salt(MessageDigest digest) {
//      return TableSalt.cellSalt(rowSalt(), index, digest);
//    }
//
//    @Override
//    public ByteBuffer hash(MessageDigest digest) {
//      byte[] salt = salt(digest);
//      digest.reset();
//      digest.update(salt);
//      digest.update(data());
//      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
//    }
//
//    @Override
//    public ByteBuffer salt() {
//      return ByteBuffer.wrap(salt(DIGEST.newDigest())).asReadOnlyBuffer();
//    }



    @Override
    public int dataSize() {
      return data.remaining();
    }
    
  }
  
  
  public final static class UnsaltedReveal extends Revealed {
    
    public static Cell load(ByteBuffer in, int size) {
      var data = BufferUtils.slice(in, size);
      return new UnsaltedReveal(data, false);
    }
    
    private final ByteBuffer data;
    
    /**
     * @param data      positioned at zero, limit == remaining
     */
    UnsaltedReveal(ByteBuffer data, boolean dummy) {
      this.data = data;
      assert data.remaining() == data.capacity();
    }
    
    /**
     * 
     * @param data      sliced, not copied. <em>Do not modify contents!</em>
     */
    public UnsaltedReveal(ByteBuffer data) {
      this.data = data.slice();
    }

    @Override
    public final ByteBuffer hash(MessageDigest digest) {
      digest.reset();
      digest.update(data.duplicate());
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }

    @Override
    public int dataSize() {
      return data.remaining();
    }

    @Override
    public ByteBuffer data() {
      return data.asReadOnlyBuffer();
    }

    /** @return {@code false} */
    @Override
    public boolean hasSalt() {
      return false;
    }

    /** No salt. Returns an empty buffer. */
    @Override
    public ByteBuffer salt() {
      return BufferUtils.NULL_BUFFER;
    }
    
  }
  
  

}






















