/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.HASH_WIDTH;

import java.nio.ByteBuffer;

import io.crums.util.Strings;

/**
 * Classification of data value types for cells in a row. For the most part,
 * this concerns (and defines) the byte-sequence used to represent a cell value.
 * The only use for this byte-sequence is in the calculation of the cell's hash.
 * So, while a source ledger's data types may be more rich than this taxanomy
 * (e.g. SQL types), the few base types defined here should be suffice.
 * <p>
 * Note, there are <em>no floating point</em> types. The main rational for
 * disallowing it is that it's complicated (implementation specific,
 * multiple values such as 0, 0+, multiple standards, etc.). When constructing
 * a ledger view, <em>fractional values must be promoted to integers</em> (e.g.
 * multiplying by 100, perhaps rounding, when the decimal point marks cents).
 * </p>
 */
public enum DataType {
  
  /** UTF-8 string. Variable size. */
  STRING(0),
  /** Signed big endian long. 8 bytes. */
  LONG(8),
  /** UTC. This is a qualified {@linkplain #LONG}. */
  DATE(8),
  /** Boolean value. 1 byte.  */
  BOOL(1),
  /** Blob of bytes. (Untyped). Variable size. */
  BYTES(0),
  /**
   * A SHA-256 hash of some other value. (Untyped).
   * By convention, redacted cells are of this type, but
   * this is seldom important.
   */
  HASH(HASH_WIDTH),
  /**
   * A null value is specially marked. This is a qualified {@linkplain #BOOL}
   * (always {@code 0}).
   */
  NULL(1);
  
  
  private final static DataType[] SET = values();
  
  
  /**
   * Returns the type by the zero-based ordinal no.
   */
  public static DataType forOrdinal(int ordinal)
      throws IndexOutOfBoundsException {
    return SET[ordinal];
  }
  
  private final int size;
  
  private DataType(int size) {
    this.size = size;
  }
  

  /**
   * Determines whether the type's byte-size, is fixed.
   * 
   * @return {@code size() != 0}
   * 
   * @see #size()
   * @see #isVarSize()
   */
  public boolean isFixedSize() {
    return size != 0;
  }
  
  /**
   * Determines whether the type's byte-size, is variable.
   * 
   * @return {@code size() == 0}
   * 
   * @see #size()
   * @see #isFixedSize()
   */
  public boolean isVarSize() {
    return size == 0;
  }
  
  
  /**
   * Returns the type's byte-size, if fixed; zero, otherwise.
   * 
   * @return &ge; 0
   */
  public int size() {
    return size;
  }
  
  /** Determines whether this is the {@linkplain #NULL} instance. */
  public boolean isNull() {
    return this == NULL;
  }
  
  
  /**
   * Determines whether this is the {@linkplain #HASH} instance.
   * Because {@code HASH} instances are not supposed to be rehashed
   */
  public boolean isHash() {
    return this == HASH;
  }
  
  
  public boolean isNumber() {
    return this == LONG || this == DATE;
  }
  
  
  /**
   * Returns the given {@code input} buffer as a typed value.
   * Note, the {@linkplain #NULL} instance <em>returns null!</em>
   * 
   * @param input       content assumed not to change, with exact amount
   *                    of remaining bytes for this {@code DatatType}. Note,
   *                    the positional state of the argument is untouched
   *                    (only absolute bulk operations)
   *                    
   * @return either a {@code java.lang.String, java.lang.Long, java.lang.Boolean,
   *                   java.nio.ByteBuffer, } or {@code null}
   */
  public Object toValue(ByteBuffer input) {
    
    if (isFixedSize()) {
      int fixedInputSize = this == NULL ? 0 : size();
      if (fixedInputSize != input.remaining())
        throw new IllegalArgumentException(
            "expected input size type %s is %d; input arg %s"
            .formatted(this, size(), input));
    }
    
    switch (this) {
    case STRING:        return Strings.utf8String(input);
    case LONG:
    case DATE:          return input.getLong(input.position());
    case BOOL:
      {
        byte b = input.get(input.position());
        if ((b & 1) != b)
          throw new IllegalArgumentException(
              "input byte (" + Integer.toHexString(b & 0xff) +
              ") must be either 0 or 1");
        return b == 0 ? Boolean.FALSE : Boolean.TRUE;
      }
    case BYTES:
    case HASH:          return input.slice();
    case NULL:          return null;
    default:
      throw new RuntimeException("Unaccount enum type " + this);
    }
  }

}













