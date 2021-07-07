/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.SldgConstants;

/**
 * A value represented by its hash. Since this is a precomputed hash, instances are never
 * salted ({@linkplain #isSalted()} returns {@code false}).
 * 
 * @see #getHash(MessageDigest)
 */
public final class HashValue extends BytesValue {

  static HashValue loadHash(ByteBuffer in) {
    ByteBuffer bytes = BufferUtils.slice(in, SldgConstants.HASH_WIDTH);
    return new HashValue(bytes);
  }

  public HashValue(ByteBuffer bytes) {
    super(bytes, ColumnType.HASH, BufferUtils.NULL_BUFFER);
    if (size() != HASH_WIDTH)
      throw new IllegalArgumentException("bytes not expected hash-width: " + bytes);
  }
  
  
  /**
   * Unlike the other column value types, this involves no computation (since it's already a hash).
   * 
   * @return {@code getBytes()}
   * @see #getBytes()
   */
  @Override
  public ByteBuffer getHash(MessageDigest digest) {
    return getBytes();
  }
  
  @Override
  public int serialSize() {
    return 1 + SldgConstants.HASH_WIDTH;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    return out.put(getType().code()).put(getBytes());
  }
  
}