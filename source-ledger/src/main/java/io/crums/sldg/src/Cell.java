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
   * if necessary. If the cell is <em>not salted</em>, then this is just the hash of
   * the concatenation of the {@linkplain #dataType()} (one byte) and the
   * {@linkplain #data() data}; if it is salted ({@linkplain #hasSalt()}),
   * then it is hash of the concatenation of the {@linkplain #salt() salt}
   * followed by {@linkplain #dataType()}, then {@linkplain #data() data}.
   * 
   * @param digest      SHA-256 digester
   * 
   * @return    32-byte hash
   */
  public abstract ByteBuffer hash(MessageDigest digest);


  /**
   * Returns the cell's data type.
   */
  public abstract DataType dataType();
  
  
  /**
   * Determines whether the cell's data is <em>not redacted</em>.
   * 
   * @see #isRedacted()
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
   * @see #hasData()
   */
  public final boolean isRedacted() {
    return !hasData();
  }


  /**
   * Returns the cell's cannonical value as a Java object, iff the instance
   * {@linkplain #hasData() has data}; otherwise, throws an exception. Depending
   * on {@linkplain #dataType()}, this returns an instance of either
   * <ol>
   * <li>{@linkplain String}</li>
   * <li>{@linkplain Long}</li>
   * <li>{@linkplain java.math.BigDecimal}</li>
   * <li>{@linkplain java.math.BigInteger}</li>
   * <li>{@linkplain ByteBuffer}</li>
   * <li>or {@code null} (!)</li>
   * </ol>
   * <p>
   * Note, since {@code null} is a first class object in this model (following
   * the SQL DDL), unlike with {@linkplain #data()} and {@linkplain #getData()},
   * this method has <em>no corresponding
   * {@code public Optional<Object> getValue()} method </em>.
   * </p>
   */
  public Object value() throws NoSuchElementException {
    if (isRedacted())
      throw new NoSuchElementException("redacted cells do not expose value()");
    return dataType().toValue(data());
  }
  
  
  /**
   * Returns the cell's value in raw form. Whatever its semantics (type, etc.),
   * this is the byte sequence used to represent the value, which figures in
   * the calculation of the cell's hash.
   * 
   * @return a read-only buffer
   * 
   * @throws NoSuchElementException
   *    if {@linkplain #hasData()} returns {@code false}
   * 
   * @see #getData()
   * @see #hasData()
   */
  public abstract ByteBuffer data() throws NoSuchElementException;
  
  
  /**
   * Returns the data buffer as an optional. Unredacted cells always have
   * data. That is, even {@code null} values are represented by some byte
   * sequence.
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


    public DataType dataType() {
      return DataType.HASH;
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

    void checkDataSize(DataType type, int dataSize) {
      if (dataSize < type.minimumSize())
        throw new IllegalArgumentException(
            "data too small for %s: %d < minimum %d"
            .formatted(type, dataSize, type.minimumSize()));
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

    private final static ByteBuffer ZERO =
        ByteBuffer.allocate(DataType.NULL.size() + 1).asReadOnlyBuffer();
    
    private final static ByteBuffer UNSALTED_HASH;
    
    static {
      var digest = DIGEST.newDigest();
      digest.update(ZERO.duplicate());
      UNSALTED_HASH = ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }


    @Override
    public DataType dataType() {
      return DataType.NULL;
    }
    
    ByteBuffer unsaltedHash() {
      return UNSALTED_HASH.duplicate();
    }

    ByteBuffer zero() {
      return ZERO.duplicate();
    }
    
    @Override
    public int dataSize() {
      return DataType.NULL.size();
    }

    @Override
    public ByteBuffer data() {
      return zero().position(1);
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
      digest.update(zero());
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
    public static Cell load(DataType type, ByteBuffer in, int dataSize) {
      if (dataSize < 0)
        throw new IllegalArgumentException("negative dataSize: " + dataSize);
      return new SaltedCell(type, BufferUtils.slice(in, HASH_WIDTH + dataSize), false);
    }
    
    
    private final byte type;
    private final ByteBuffer saltData;
    
    private ByteBuffer saltData() {   return saltData.asReadOnlyBuffer(); }

    /**
     * @param data      positioned at zero, limit == remaining
     */
    private SaltedCell(DataType type, ByteBuffer saltData, boolean dummy) {
      this.type = (byte) type.ordinal();
      this.saltData = saltData;
      assert saltData.remaining() == saltData.capacity();
      checkDataSize(type, saltData.remaining() - HASH_WIDTH);
    }
    
    /**
     * @param saltData  sliced, not copied. <em>Do not modify contents!</em>
     */
    public SaltedCell(ByteBuffer saltData, DataType type) {
      this(type, saltData.slice(), false);
      if (this.saltData.remaining() < HASH_WIDTH)
        throw new IllegalArgumentException(
            "too few remaining bytes: " + saltData);
    }
    

    @Override
    public DataType dataType() {
      return DataType.forOrdinal(0xff & type);
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      digest.reset();
      var sd = saltData();
      digest.update(sd.limit(HASH_WIDTH));
      digest.update(type);
      digest.update(sd.clear().position(HASH_WIDTH));
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
    public ByteBuffer salt() {
      return ByteBuffer.wrap(salt(DIGEST.newDigest())).asReadOnlyBuffer();
    }
    
    
  }
  
  
  public final static class RowSaltedNull extends RowSaltedReveal {
    
    private final static ByteBuffer Z =
        ByteBuffer.wrap(new byte[DataType.NULL.size()]).asReadOnlyBuffer();
    
    public RowSaltedNull(ByteBuffer rowSalt, int index) {
      super(index, rowSalt);
    }


    @Override
    public DataType dataType() {
      return DataType.NULL;
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      byte[] salt = salt(digest);
      digest.reset();
      digest.update(salt);
      digest.update((byte) DataType.NULL.ordinal());  // (zero)
      digest.update(data());
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }

    @Override
    public int dataSize() {
      return DataType.NULL.size();
    }

    @Override
    public ByteBuffer data() {
      return Z.duplicate();
    }
    
  }
  
  
  
  public final static class RowSaltedCell extends RowSaltedReveal {
    
    
    private final ByteBuffer data;
    private final byte type;
    
    
    

    RowSaltedCell(ByteBuffer rowSalt, int index, DataType type, ByteBuffer data, boolean dummy) {
      super(index, rowSalt);
      this.data = data;
      this.type = (byte) type.ordinal();
      assert checkArgs(rowSalt, index, data);
      checkDataSize(type, data.remaining());
    }
    
    
    public RowSaltedCell(ByteBuffer rowSalt, int index, DataType type, ByteBuffer data) {
      this(rowSalt.slice(), index, type, data.slice(), false);
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
    public DataType dataType() {
      return DataType.forOrdinal(type);
    }
    
    @Override
    public ByteBuffer data() {
      return data.asReadOnlyBuffer();
    }


    @Override
    public ByteBuffer hash(MessageDigest digest) {
      byte[] salt = salt(digest);
      digest.reset();
      digest.update(salt);
      digest.update(type);
      digest.update(data());
      return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
    }


    @Override
    public int dataSize() {
      return data.remaining();
    }
    
  }
  
  
  public final static class UnsaltedReveal extends Revealed {
    
    public static Cell load(DataType type, ByteBuffer in, int size) {
      var data = BufferUtils.slice(in, size);
      return new UnsaltedReveal(type, data, false);
    }
    
    private final byte type;
    private final ByteBuffer data;
    
    /**
     * @param data      positioned at zero, limit == remaining
     */
    UnsaltedReveal(DataType type, ByteBuffer data, boolean dummy) {
      this.type = (byte) type.ordinal();
      this.data = data;
      assert data.remaining() == data.capacity();
      checkDataSize(type, data.remaining());
    }
    
    /**
     * 
     * @param data      sliced, not copied. <em>Do not modify contents!</em>
     */
    public UnsaltedReveal(DataType type ,ByteBuffer data) {
      this(type, data.slice(), false);
    }


    @Override
    public DataType dataType() {
      return DataType.forOrdinal(0xff & type);
    }

    @Override
    public final ByteBuffer hash(MessageDigest digest) {
      digest.reset();
      digest.update(type);
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






















