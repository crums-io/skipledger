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
 * A value represented by its hash. Since this is a precomputed hash, instances are <em>never
 * salted</em> ({@linkplain #isSalted()} always returns {@code false}).
 * <p>
 * There are 2 use cases for this class:
 * </p>
 * <ol>
 * <li>When a column value takes too many bytes to be packaged in a morsel.
 * (Actually, a better strategy is to define a {@code BytesValue} column that is understood to be
 * the hash of the blob. That way, its hash can remain salted.)</li>
 * <li>When a column value is redacted from its row. <em>This is its main use.</em></li>
 * </ol>
 * 
 * @see #getHash(MessageDigest)
 */
public final class HashValue extends BytesValue {

  static HashValue loadHash(ByteBuffer in) {
    ByteBuffer bytes = BufferUtils.slice(in, SldgConstants.HASH_WIDTH);
    return new HashValue(bytes);
  }

  
  /**
   * @param bytes 32-bytes
   */
  public HashValue(byte[] bytes) {
    this(ByteBuffer.wrap(bytes));
  }

  
  /**
   * @param bytes 32-bytes
   */
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

  /**
   * Unlike the other column value types, this involves no computation (since it's already a hash).
   * 
   * @return {@code getBytes()}
   * @see #getBytes()
   */
  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
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