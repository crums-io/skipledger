/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.HASH_WIDTH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

import io.crums.util.Strings;

/**
 * Classification of data value types for cells in a row. For the most part,
 * this concerns (and defines) the byte-sequence used to represent a cell value.
 * The only use for this byte-sequence is in the calculation of the cell's hash.
 * So, while a source ledger's data types may be more rich than this taxanomy
 * (e.g. SQL types), the few base types defined here should suffice.
 * <p>
 * Note, there are <em>no floating point</em> types. The main rational for
 * disallowing it is that it complicates the implementation
 * (multiple values such as 0, 0+, multiple standards, etc.).
 * </p>
 * <h2>The Role of NULL</h2>
 * <p>
 * Finally, note the {@linkplain #NULL} type is anamolous to the other types in
 * the following respects:
 * </p><ol>
 * <li>It is <em>not used in column type declarations</em>.</li>
 * <li>If a read-in typed value is {@code null}, its type then falls back to
 * the {@linkplain #NULL} type.</li>
 * </ol>
 */
public enum DataType {
  
  /**
   * A null value is specially marked. It is a zeroed 5-byte value.
   */
  NULL(5),
  /** UTF-8 string. Variable size. */
  STRING(0),
  /** Signed big endian long. 8 bytes. */
  LONG(8),
  /** {@linkplain BigInteger}. Variable size; 1-byte, minimum. */
  BIG_INT(0, 1),
  /**
   * {@linkplain BigDecimal}. Variable size; 2-byte, minimum.
   * Scale must be within the range [-128, 127], inclusive.
   */
  BIG_DEC(0, 2),
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
  HASH(HASH_WIDTH);
  
  
  private final static DataType[] SET = values();

  private final static ByteBuffer NULL_DATA_BUFFER =
    ByteBuffer.wrap(new byte[NULL.size()]).asReadOnlyBuffer();

  private ByteBuffer nullDataBuffer() {
    return NULL_DATA_BUFFER.duplicate();
  }
  

  /**
   * Guesses and returns the enum instance corresponding to the given value.
   *
   * <table border="1">
   *   <caption>Java type → DataType mapping</caption>
   *   <tr><th>Java type</th><th>Returns</th><th>Notes</th></tr>
   *   <tr><td>{@code null}</td><td>{@link #NULL}</td><td></td></tr>
   *   <tr><td>{@link CharSequence} (incl. {@code String})</td><td>{@link #STRING}</td><td></td></tr>
   *   <tr><td>{@link Long}, {@link Integer}, {@link Short}, {@link Byte}</td><td>{@link #LONG}</td><td></td></tr>
   *   <tr><td>{@link java.util.Date}</td><td>{@link #DATE}</td><td></td></tr>
   *   <tr><td>{@link java.math.BigInteger}</td><td>{@link #BIG_INT}</td><td></td></tr>
   *   <tr><td>{@link java.math.BigDecimal}</td><td>{@link #BIG_DEC}</td><td></td></tr>
   *   <tr><td>{@link Boolean}</td><td>{@link #BOOL}</td><td></td></tr>
   *   <tr><td>{@link java.nio.ByteBuffer} (32 remaining bytes)</td><td>{@link #HASH}</td><td>exactly 32 bytes remaining (SHA-256 width)</td></tr>
   *   <tr><td>{@link java.nio.ByteBuffer} (other)</td><td>{@link #BYTES}</td><td></td></tr>
   *   <tr><td>{@code byte[]} (length 32)</td><td>{@link #HASH}</td><td>exactly 32 bytes (SHA-256 width)</td></tr>
   *   <tr><td>{@code byte[]} (other)</td><td>{@link #BYTES}</td><td></td></tr>
   * </table>
   *
   * @throws IllegalArgumentException if {@code value} is a type not listed above
   */
  public static DataType guessType(Object value) {
    if (value == null)
      return NULL;
    if (value instanceof CharSequence)
      return STRING;
    if (value instanceof Long ||
        value instanceof Integer ||
        value instanceof Short ||
        value instanceof Byte)
      return LONG;
    if (value instanceof Date)
      return DATE;
    if (value instanceof BigInteger)
      return BIG_INT;
    if (value instanceof BigDecimal)
      return BIG_DEC;
    if (value instanceof Boolean)
      return BOOL;
    if (value instanceof ByteBuffer b)
      return b.remaining() == HASH_WIDTH ? HASH : BYTES;
    if (value instanceof byte[] b)
      return b.length == HASH_WIDTH ? HASH : BYTES;
    
    throw new IllegalArgumentException(
        "value %s (class %s)".formatted(value, value.getClass()));
  }
  
  /**
   * Returns the type by the zero-based ordinal no.
   */
  public static DataType forOrdinal(int ordinal)
      throws IndexOutOfBoundsException {
    return SET[ordinal];
  }
  
  private final int size;
  private final int minimumSize;
  
  private DataType(int size) {
    this(size, size);
  }


  private DataType(int size, int minimumSize) {
    this.size = size;
    this.minimumSize = minimumSize;
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
    return !isFixedSize();
  }


  /**
   * Returns the minimum byte-size. For fixed-size types, this is the same as
   * {@linkplain #size()}; for variable-size types {@linkplain #BIG_INT} and
   * {@linkplain #BIG_DEC}, the returned value is non-zero.
   */
  public int minimumSize() {
    return minimumSize;
  }
  
  
  /**
   * Returns the type's byte-size, if fixed; zero, otherwise. The only exception
   * to this rule is {@linkplain #NULL}.
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
   * {@code HASH} instances are not supposed to be rehashed.
   */
  public boolean isHash() {
    return this == HASH;
  }
  
  /**
   * Returns {@code true} iff this type is a number. Note,
   * {@linkplain #BOOL} is <em>not</em> considered a number.
   */
  public boolean isNumber() {
    switch (this) {
      case LONG:
      case DATE:
      case BIG_INT:
      case BIG_DEC:
        return true;
    
      default:
        return false;
    }
  }

  /**
   * Returns {@code true} iff this is the {@linkplain #STRING} insstance.
   */
  public boolean isString() {
    return this == STRING;
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
   *                   java.math.BigInteger, java.math.BigDecimal,
   *                   java.nio.ByteBuffer}, or {@code null}
   */
  public Object toValue(ByteBuffer input) {
    
    if (isFixedSize()) {
      int fixedInputSize = size();
      if (fixedInputSize != input.remaining())
        throw new IllegalArgumentException(
            "expected input size for type %s is %d; actual was %d"
            .formatted(this, fixedInputSize, input.remaining()));
    } else if (input.remaining() < minimumSize()) {
      throw new IllegalArgumentException(
            "at least %d bytes expected for type %s; actual was %d"
            .formatted(minimumSize(), this, input.remaining()));
    }
    
    switch (this) {
    case STRING:        return Strings.utf8String(input);
    case LONG:
    case DATE:          return input.getLong(input.position());
    case BIG_INT:
      {
        byte[] array = new byte[input.remaining()];
        input.get(input.position(), array);
        return new BigInteger(array);
      }
    case BIG_DEC:
      {
        int pos = input.position();
        int size = input.remaining();
        int scale = input.get(pos);
        byte[] unscaledVal = new byte[size - 1];
        input.get(pos + 1, unscaledVal);
        return new BigDecimal(new BigInteger(unscaledVal), scale);
      }
    case BOOL:
      {
        byte b = input.get(input.position());
        if ((b & 1) != b)
          throw new IllegalArgumentException(
              "input byte for type BOOL (" + Integer.toHexString(b & 0xff) +
              ") must be either 0 or 1");
        return b == 0 ? Boolean.FALSE : Boolean.TRUE;
      }
    case BYTES:
    case HASH:          return input.slice();
    case NULL:
      for (int pos = input.position(); pos < input.limit(); ++pos)
        if (input.get(pos) != 0)
          throw new IllegalArgumentException(
              "type NULL expected zeroed byte at index %d; actual was 0x%s"
              .formatted(pos, Integer.toHexString(0xff & input.get(pos))));
      return null;
    default:
      throw new RuntimeException("Unaccount enum type " + this);
    }
  }



  /**
   * Converts the given value to a byte sequence suitable for hashing depending
   * on the enum instance.
   * 
   * @param value permissable types depend on the enum
   */
  public ByteBuffer toByteBuffer(Object value) {
    if (value == null)
      return nullDataBuffer();
    return switch (this) {
      case STRING   -> toStringBuffer(value);
      case LONG     -> toLongBuffer(value);
      case BIG_INT  -> toBigIntBuffer(value);
      case BIG_DEC  -> toBigDecBuffer(value);
      case DATE     -> toDateBuffer(value);
      case BOOL     -> toBoolBuffer(value);
      case BYTES    -> toByteSequenceBuffer(value);
      case HASH     -> toHashBuffer(value);
      case NULL     -> throw new IllegalArgumentException(
          "type NULL cannot convert non-null value (quoted) '" + value + "'' to null");
    };
  }

  private ByteBuffer toHashBuffer(Object value) {
    var buf = toByteSequenceBuffer(value);
    if (buf.remaining() != HASH_WIDTH)
      throw new IllegalArgumentException(
          "expected %d bytes for HASH type; actual was %d"
          .formatted(HASH_WIDTH, buf.remaining()));
    return buf;
  }

  private ByteBuffer toByteSequenceBuffer(Object value) {
    if (value instanceof ByteBuffer buf)
      return buf.slice();
    else if (value instanceof byte[] array)
      return ByteBuffer.wrap(array);
    else
      throw new IllegalArgumentException(
          conversionErrorMsg(value, ByteBuffer.class));
  }


  private ByteBuffer toBoolBuffer(Object value) {
    if (value instanceof Boolean b) {
      byte[] single = new byte[] { (byte) (Boolean.TRUE.equals(value) ? 1 : 0)};
      return ByteBuffer.wrap(single);
    }
    throw new IllegalArgumentException(
        conversionErrorMsg(value, Boolean.class));
  }


  private String conversionErrorMsg(Object value, Class<?> targetClass) {
    return
        "type %s cannot convert class %s (%s) to %s"
        .formatted(
            this,
            value.getClass().getSimpleName(),
            value, targetClass.getSimpleName());
  }


  private ByteBuffer toBigDecBuffer(Object value) {
    BigDecimal bigDec;
    if (value instanceof BigDecimal d) {
      bigDec = d;
    } else if (value instanceof Integer ||
        value instanceof Long ||
        value instanceof Short ||
        value instanceof Byte) {
      bigDec = BigDecimal.valueOf(((Number) value).longValue());
    } else
      throw new IllegalArgumentException(
          conversionErrorMsg(value, BigDecimal.class));

    byte scale = (byte) bigDec.scale();
    if (scale != bigDec.scale())
      throw new IllegalArgumentException(
        "%s: out-of-bounds scale (%d) for value ~ %s"
        .formatted(
            this, bigDec.scale(), bigDec.doubleValue()));
    byte[] unscaled = bigDec.unscaledValue().toByteArray();
    return
        ByteBuffer.allocate(1 + unscaled.length)
        .put(scale).put(unscaled).flip();
  }

  private ByteBuffer toBigIntBuffer(Object value) {
    BigInteger bigInt;
    if (value instanceof BigInteger i) {
      bigInt = i;
    } else if (value instanceof Integer ||
        value instanceof Long ||
        value instanceof Short ||
        value instanceof Byte) {
      bigInt = BigInteger.valueOf(((Number) value).longValue());
    } else
      throw new IllegalArgumentException(
          conversionErrorMsg(value, BigInteger.class));

    return ByteBuffer.wrap(bigInt.toByteArray());
  }


  private ByteBuffer toDateBuffer(Object value) {
    long utc;
    if (value instanceof Long d) {
      utc = d;
    } else if (value instanceof Date d) {
      utc = d.getTime();
    } else
      throw new IllegalArgumentException(
          conversionErrorMsg(value, Long.class));

    return ByteBuffer.allocate(8).putLong(utc).flip();
  }

  private ByteBuffer toLongBuffer(Object value) {
    if (value instanceof Integer ||
        value instanceof Long ||
        value instanceof Short ||
        value instanceof Byte) {

      Number n = (Number) value;
      return ByteBuffer.allocate(8).putLong(n.longValue()).flip();
    
    } else {
      throw new IllegalArgumentException(
          conversionErrorMsg(value, Long.class));
    }
  }


  private ByteBuffer toStringBuffer(Object value) {
    if (value instanceof CharSequence)
      return Strings.utf8Buffer(value.toString());

    throw new IllegalArgumentException(
        conversionErrorMsg(value, String.class));
  }





}













