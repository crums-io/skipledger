/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.util.hash.Digest.bufferDigest;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.util.BigShort;
import io.crums.util.Strings;

/**
 * A table cell value represented as a string. Note per the JDBC driver spec,
 * any value in a result-set can be coerced to be a string: i.e. it's a fallback
 * for the SQL types that we haven't explicitly mapped.
 */
public final class StringValue extends ColumnValue {
  
  static StringValue loadString(ByteBuffer in, ByteBuffer salt) {
    int len = BigShort.getBigShort(in);
    byte[] bytes = new byte[len];
    in.get(bytes);
    return new StringValue(new String(bytes, Strings.UTF_8), bytes, salt);
  }
  
  private final String string;
  private final byte[] bytes;
  
  
  
  
  /**
   * Salted constructor.
   */
  public StringValue(String string, ByteBuffer salt) {
    this(string, Objects.requireNonNull(string, "null string argument").getBytes(Strings.UTF_8), salt);
    if (bytes.length > MAX_BYTE_SIZE)
      throw new IllegalArgumentException(
          "string byte-length greater than capacity (" + MAX_BYTE_SIZE + "): " + bytes.length);
  }
  
  
  private StringValue(String string, byte[] bytes, ByteBuffer salt) {
    super(ColumnType.STRING, salt);
    this.string = string;
    this.bytes = bytes;
  }
  
  
  
  @Override
  public String getValue() {
    return string;
  }
  
  
  /**
   * Returns the string value.
   */
  public String getString() {
    return string;
  }
  

  @Override
  public ByteBuffer unsaltedHash(MessageDigest digest) {
    ByteBuffer intBuff = ByteBuffer.allocate(4).putInt(bytes.length).flip();
    digest.reset();
    digest.update(intBuff);
    digest.update(bytes);
    return bufferDigest(digest);
  }


  @Override
  public int serialSize() {
    return headSize() + BigShort.BYTES + bytes.length;
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    writeTypeAndSalt(out);
    BigShort.putBigShort(out, bytes.length);
    return out.put(bytes);
  }


  @Override
  public void appendValue(StringBuilder s) {
    s.append(string);
  }
  

}


