/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.entry;

import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.util.Strings;



/**
 * UTF-8 encoded text entry. The hashing operation remains the
 * {@linkplain TextEntry#hash() default}.
 */
public class TextEntry extends Entry {
  
  
  /**
   * Returns the given text a UTF-8 encoded byte sequence. The
   * returned buffer is <em>not read-only</em>.
   */
  public static ByteBuffer toBuffer(String text) {
    return ByteBuffer.wrap(toBytes(text));
  }
  
  
  /**
   * Returns {@code text} as a byte array in UTF-8 encoding.
   */
  public static byte[] toBytes(String text) {
    if (Objects.requireNonNull(text, "null text").isEmpty())
      throw new IllegalArgumentException("empty text");
    return text.getBytes(Strings.UTF_8);
  }
  
  
  
  /**
   * Returns {@code bytes} a UTF-8 encoded string.
   */
  public static String toString(byte[] bytes) {
    if (Objects.requireNonNull(bytes, "null bytes").length == 0)
      throw new IllegalArgumentException("empty bytes array");
    return new String(bytes, Strings.UTF_8);
  }
  
  
  /**
   * Returns the given buffer as a UTF-8 encoded string. On return, the
   * {@code buffer} argument has no remaining bytes.
   */
  public static String toString(ByteBuffer buffer) {
    int byteCount = Objects.requireNonNull(buffer, "null buffer").remaining();
    if (byteCount == 0)
      throw new IllegalArgumentException("empty buffer");
    byte[] bytes = new byte[byteCount];
    buffer.get(bytes);
    return toString(bytes);
  }
  
  
  
  
  

  private final String text;
  
  
  public TextEntry(String text, long rowNumber) {
    super(toBuffer(text), rowNumber);
    this.text = text;
  }
  
  
  public TextEntry(String text, EntryInfo info) {
    super(toBuffer(text), info);
    this.text = text;
  }

  
  public TextEntry(TextEntry copy, long rowNumber) {
    super(copy, rowNumber);
    this.text = copy.text;
  }
  
  
  /**
   * @param contents UTF-8 encoded string
   */
  public TextEntry(ByteBuffer contents, long rowNumber) {
    super(contents, rowNumber);
    this.text = toString(contents.duplicate());
  }
  
  
  /**
   * @param contents UTF-8 encoded string
   */
  public TextEntry(ByteBuffer contents, EntryInfo info) {
    super(contents, info);
    this.text = toString(contents.duplicate());
  }
  
  
  
  @Override
  public TextEntry reNumber(long rowNumber) {
    return rowNumber == rowNumber() ? this : new TextEntry(this, rowNumber);
  }
  
  
  /**
   * Returns the entry text.
   */
  public final String text() {
    return text;
  }

}
