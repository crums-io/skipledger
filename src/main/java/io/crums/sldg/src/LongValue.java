/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.util.hash.Digest.bufferDigest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.io.buffer.BufferUtils;

/**
 * All integral values are expressed as 8-byte {@code long}s.
 * Prefer this over {@linkplain DoubleValue}. (So if doing fixed
 * precision arithmetic, first convert to integral units.)
 */
public class LongValue extends ColumnValue {
  
  
  static LongValue loadLong(ByteBuffer in, ByteBuffer salt) {
    return new LongValue(in.getLong(), salt);
  }
  
  private final long number;
  
  
  
  public LongValue(long number) {
    this(number, BufferUtils.NULL_BUFFER);
  }
    
  public LongValue(long number, ByteBuffer salt) {
    this(ColumnType.LONG, number, salt);
  }
  
  
  LongValue(ColumnType type, long number, ByteBuffer salt) {
    super(type, salt);
    this.number = number;
  }
  
  
  
  @Override
  public final Long getValue() {
    return number;
  }
  
  
  public final long getNumber() {
    return number;
  }
  

  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
    ByteBuffer lngBuff = ByteBuffer.allocate(8).putLong(number).flip();
    digest.reset();
    digest.update(lngBuff);
    return bufferDigest(digest);
  }




  @Override
  public int serialSize() {
    return headSize() + 8;
  }




  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    return writeTypeAndSalt(out).putLong(number);
  }
  
  @Override
  public void appendValue(StringBuilder s) {
    s.append(number);
  }

  
}