/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;

import static io.crums.util.hash.Digest.bufferDigest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import io.crums.io.buffer.BufferUtils;
import io.crums.util.BigShort;
import io.crums.util.IntegralStrings;

/**
 * A blob of bytes no greater 16,777,215 bytes.
 */
public class BytesValue extends ColumnValue {
  
  static BytesValue loadBytes(ByteBuffer in, ByteBuffer salt) {
    int len = BigShort.getBigShort(in);
    ByteBuffer bytes = BufferUtils.slice(in, len);
    return new BytesValue(bytes, salt);
  }
  
  private final ByteBuffer bytes;
  
  public BytesValue(ByteBuffer bytes) {
    this(bytes, ColumnType.BYTES, BufferUtils.NULL_BUFFER);
  }
  
  public BytesValue(ByteBuffer bytes, ByteBuffer salt) {
    this(bytes, ColumnType.BYTES, salt);
  }
  
  public BytesValue(byte[] bytes, ByteBuffer salt) {
    this(ByteBuffer.wrap(bytes), ColumnType.BYTES, salt);
  }
  
  
  BytesValue(ByteBuffer bytes, ColumnType type, ByteBuffer salt) {
    super(type, salt);
    this.bytes = BufferUtils.readOnlySlice(bytes);
    if (bytes.remaining() > MAX_BYTE_SIZE)
      throw new IllegalArgumentException(
          "byte-length greater than capacity (" + MAX_BYTE_SIZE + "): " + bytes);
  }
  
  
  @Override
  public final ByteBuffer getValue() {
    return getBytes();
  }
  
  
  public final ByteBuffer getBytes() {
    return bytes.asReadOnlyBuffer();
  }
  
  
  public final int size() {
    return bytes.remaining();
  }


  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
    int count = bytes.remaining();
    ByteBuffer intBuff = ByteBuffer.allocate(4).putInt(count).flip();
    digest.reset();
    digest.update(intBuff);
    digest.update(getBytes());
    return bufferDigest(digest);
  }

  @Override
  public int serialSize() {
    return headSize() + BigShort.BYTES + size();
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    writeTypeAndSalt(out);
    BigShort.putBigShort(out, size());
    return out.put(getBytes());
  }

  @Override
  public void appendValue(StringBuilder s) {
    ByteBuffer b = getBytes();
    if (bytes.remaining() > MAX_TO_STRING_CHARS / 2)
      b.limit(-1 + MAX_TO_STRING_CHARS / 2);
    IntegralStrings.appendHex(b, s).append("..");
  }
  
}