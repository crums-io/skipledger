/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.io.buffer.BufferUtils;

/**
 * Salted null column value.
 */
public final class NullValue extends ColumnValue {
  
  /**
   * Unsalted null value. Salting should be the rule, rather than the exception, so
   * this here is just for completeness.
   */
  public final static NullValue UNSALTED_NULL = new NullValue(BufferUtils.NULL_BUFFER);
  
  
  /**
   * Returns a salted instance, if the given salt is not empty; {@linkplain #UNSALTED_NULL} o.w.
   * 
   * @param salt either zero (no salt) or 32-bytes
   */
  static NullValue nullInstance(ByteBuffer salt) {
    return salt.hasRemaining() ? new NullValue(salt) : UNSALTED_NULL;
  }

  /**
   * Constructs a salted instance.
   * 
   * @param salt either 32-bytes remaining or empty (unsalted)
   * 
   * @see #UNSALTED_NULL
   */
  public NullValue(ByteBuffer salt) {
    super(ColumnType.NULL, salt);
  }
  
  
  /**
   * Returns <b>{@code null}</b>.
   */
  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public int serialSize() {
    return headSize();
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    byte code = ColumnType.NULL.code();
    if (isSalted()) {
      code = (byte) -code;
      out.put(code);
      out.put(getSalt());
    } else {
      out.put(code);
    }
    return out;
  }

  /**
   * Returns a hash of zeroes.
   * 
   * @return {@code DIGEST.sentinelHash()}
   */
  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
    return DIGEST.sentinelHash();
  }

  @Override
  public void appendValue(StringBuilder s) {  }

}
