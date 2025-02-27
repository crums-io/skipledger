/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.SldgConstants.DIGEST;
import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.model.Constants;
import io.crums.util.BigShort;

/**
 * A table-cell value. Since we usually speak of rows and columns (as when we
 * talk of a row's <em>column-value</em>), this class is uses the <em>Column</em>
 * moniker in its name.
 * 
 * <h2>Salting</h2>
 * <p>
 * TODO: document how this works (or reference document that does).
 * </p>
 */
public abstract class ColumnValue implements Serial {
  
  
  /**
   * The maximum byte size of a column's value.
   */
  public final static int MAX_BYTE_SIZE = BigShort.MAX_VALUE;
  
  
  /**
   * Returns an unsalted instance. Generally not a good idea. This may be marked
   * deprecated in a future version. For now included for closure.
   * 
   * @deprecated
   */
  public static ColumnValue toInstance(Object obj) {
    return toInstance(obj, BufferUtils.NULL_BUFFER);
  }
  
  
  /**
   * Converts and returns the given object as an instance of this class using the given {@code salt},
   * if any. If the object is already an instance of this class, then it is returned as-is;
   * If the given salt buffer is empty, then an unsalted instance is returned.
   * 
   * <h4>Supported Object Types</h4>
   * 
   * <p>The following object types are recognized and have special hashing rules:</p>
   * <ul>
   * <li>{@code byte[]}. Maximum 64kB.</li>
   * <li>{@code ByteBuffer}. Maximum 64kB.</li>
   * <li>{@code CharSequence}. Maximum 64kB in UTF-8.</li>
   * <li>{@code Number}. Whether floating-point or integral, represented by 8-byte values (doubles and longs).
   * Note if a value falls outside the range of a {@code double} or {@code long}, then undefined behavior (bad)
   * ensues.</li>
   * <li>{@code null}. Nulls are mapped to a special type and are usually salted.</li>
   * </ul>
   * <p>
   * Note in addition to the special handling for numbers, the byte-types {@code byte[]} and @code ByteBuffer}
   * specially interpret 32-byte values as hashes. Such hashes are converted to {@linkplain HashValue} instances.
   * </p>
   * 
   * @param obj  either {@code null}, or one of the allowed object types
   */
  public static ColumnValue toInstance(Object obj, ByteBuffer salt) {
    if (obj == null) {
      return NullValue.nullInstance(salt);
    
    } else if (obj instanceof byte[]) {
      ByteBuffer bytes = ByteBuffer.wrap((byte[]) obj);
      return bytes.remaining() == HASH_WIDTH ? new HashValue(bytes) : new BytesValue(bytes, salt);
    
    } else if (obj instanceof ByteBuffer) {
      ByteBuffer bytes = (ByteBuffer) obj;
      return bytes.remaining() == HASH_WIDTH ? new HashValue(bytes) : new BytesValue(bytes, salt);
    
    } else if (obj instanceof CharSequence) {
      return new StringValue(obj.toString(), salt);
    
    } else if (obj instanceof Number) {
      Number num = (Number) obj;
      if (num instanceof Float || num instanceof Double)
        return new DoubleValue(num.doubleValue(), salt);
      else
        return new LongValue(num.longValue(), salt);
      
    } else if (obj instanceof Date) {
        Date date = (Date) obj;
        return new DateValue(date.getTime(), salt);
    
    } else if (obj instanceof ColumnValue) {
      return (ColumnValue) obj;
    
    } else
      throw new IllegalArgumentException("unrecognized object: " + obj);
  }
  
  
  public static ColumnValue loadValue(ByteBuffer in) {
    int type = in.get();
    ByteBuffer salt;
    if (type < 0) {
      salt = BufferUtils.slice(in, HASH_WIDTH);
      type = -type;
    } else {
      assert type != 0;
      salt = BufferUtils.NULL_BUFFER;
    }
    // this double switching is hopefully jitted out under load
    switch (ColumnType.forCode(type)) {
    case NULL:    return NullValue.nullInstance(salt);
    case HASH:    assert !salt.hasRemaining();
                  return HashValue.loadHash(in);
    case BYTES:   return BytesValue.loadBytes(in, salt);
    case STRING:  return StringValue.loadString(in, salt);
    case LONG:    return LongValue.loadLong(in, salt);
    case DOUBLE:  return DoubleValue.loadDouble(in, salt);
    case DATE:    return DateValue.loadDate(in, salt);
    }
    
    throw new RuntimeException("unaccounted type: " + ColumnType.forCode(type));
  }
  
  
  
  
  
  
  
  private final ColumnType type;
  private final ByteBuffer salt;
  
  /**
   * 
   */
  ColumnValue(ColumnType type) {
    this(type, BufferUtils.NULL_BUFFER);
  }

  /**
   * 
   */
  ColumnValue(ColumnType type, ByteBuffer salt) {
    this.type = Objects.requireNonNull(type, "null type");
    this.salt = BufferUtils.readOnlySlice(salt);
    if (this.salt.hasRemaining() && this.salt.remaining() != Constants.HASH_WIDTH)
      throw new IllegalArgumentException(
          "salt must either be empty or have exactly 32 bytes remaining: " + salt);
  }
  
  
  /**
   * Returns the column type for this column value.
   */
  public final ColumnType getType() {
    return type;
  }
  
  
  /**
   * Determines if an instance is salted.
   * 
   * @return {@code getSalt().hasRemaining()}
   */
  public final boolean isSalted() {
    return salt.hasRemaining();
  }
  
  
  
  /**
   * Writes either 1 or 33 bytes to the given {@code out} buffer encoding
   * both the type, whether it is salted, and if so, the value of the salt.
   */
  protected final ByteBuffer writeTypeAndSalt(ByteBuffer out) {
    if (salt.hasRemaining())
      out.put((byte) - type.code()).put(getSalt());
    else
      out.put(type.code());
    return out;
  }
  
  
  /**
   * @return {@code isSalted() ? HASH_WIDTH + 1 : 1}
   */
  protected final int headSize() {
    return salt.hasRemaining() ? SALTED_HEAD_SIZE : 1;
  }
  
  private final static int SALTED_HEAD_SIZE = HASH_WIDTH + 1;
  

  /**
   * Returns the salt.
   * 
   * @return non-null, but possibly empty
   */
  public final ByteBuffer getSalt() {
    return BufferUtils.readOnlySlice(salt);
  }
  
  
  
  
  /**
   * Returns the cell-value's hash.
   * 
   * @return {@code getHash(DIGEST.newDigest())}
   */
  public ByteBuffer getHash() {
    return getHash(DIGEST.newDigest());
  }
  
  
  public ByteBuffer getHash(MessageDigest digest) {
    Objects.requireNonNull(digest, "null digest");
    ByteBuffer hash = unsaltedHash(digest);
    if (isSalted())
      hash = ByteBuffer.wrap(saltHash(getSalt(), hash, digest));
    return hash;
  }
  
  
  
  public static byte[] saltHash(ByteBuffer salt, byte[] unsaltedHash, MessageDigest digest) {
    digest.reset();
    digest.update(salt);
    digest.update(unsaltedHash);
    return digest.digest();
  }
  
  
  
  public static byte[] saltHash(ByteBuffer salt, ByteBuffer unsaltedHash, MessageDigest digest) {
    digest.reset();
    digest.update(salt);
    digest.update(unsaltedHash);
    return digest.digest();
  }
  
  
  /**
   * Returns the column (cell) value's hash without accounting for the
   * salt (which may or may not be present).
   * 
   * @param digest a SHA-256 <em>work</em> digest
   */
  public abstract ByteBuffer unsaltedHash(MessageDigest digest);
  

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder(64).append(type.symbol()).append('[');
    appendValue(s);
    return s.append(']').toString();
  }
  
  
  /**
   * Returns the value of this column as a string. The returned value does not
   * contain any type information.
   */
  public String getStringValue() {
    StringBuilder s = new StringBuilder();
    appendValue(s);
    return s.toString();
  }
  
  
  /**
   * Returns the value.
   * 
   * @return  null <b>iff</b> the instance {@linkplain #getType() type} is
   *          {@linkplain ColumnType#NULL}.
   * 
   */
  public abstract Object getValue();
  
  
  /**
   * Appends the value of this column into the given buffer. The appended string
   * does not contain any type information.
   * 
   * @param s the buffer appended to
   */
  public abstract void appendValue(StringBuilder s);


  public static int MAX_TO_STRING_CHARS = 480;
  
  
  /**
   * 
   */
  public final static ColumnValue NULL_VALUE = NullValue.UNSALTED_NULL;

}
