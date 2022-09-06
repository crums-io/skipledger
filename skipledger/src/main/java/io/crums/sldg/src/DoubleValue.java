/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.util.hash.Digest.bufferDigest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * All floating point values are represented as 8-byte {@code double}s.
 * Avoid this type if possible: mixed-precision arithmetic can be
 * problematic for proof-statements.
 */
public final class DoubleValue extends ColumnValue {
  
  
  static DoubleValue loadDouble(ByteBuffer in, ByteBuffer salt) {
    return new DoubleValue(in.getDouble(), salt);
  }
  
  
  private final double value;

  /**
   * @param value not a NaN
   * @param salt  not null; either zero or 32 remaining bytes exactly
   */
  public DoubleValue(double value,  ByteBuffer salt) {
    super(ColumnType.DOUBLE, salt);
    this.value = value;
  }
  
  
  @Override
  public Double getValue() {
    return value;
  }
  
  
  public double getNumber() {
    return value;
  }

  @Override
  public int serialSize() {
    return headSize() + 8;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    return writeTypeAndSalt(out).putDouble(value);
  }

  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
    ByteBuffer dblBuff = ByteBuffer.allocate(8).putDouble(value).flip();
    digest.reset();
    digest.update(dblBuff);
    return bufferDigest(digest);
  }

  @Override
  public void appendValue(StringBuilder s) {
    s.append(value);
  }

}
