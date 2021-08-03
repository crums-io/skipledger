/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import java.nio.ByteBuffer;

/**
 * A Date/Time value (eg an SQL DATE or TIMESTAMP type). This is represented (and
 * {@linkplain #getHash(java.security.MessageDigest) hashes}) the same way as a
 * {@linkplain LongValue}, but is just marked with a different {@linkplain #getType() type}.
 */
public class DateValue extends LongValue {
  

  static DateValue loadDate(ByteBuffer in, ByteBuffer salt) {
    return new DateValue(in.getLong(), salt);
  }

  
  
  public DateValue(long utc, ByteBuffer salt) {
    super(ColumnType.DATE, utc, salt);
  }
  
  
  /**
   * Returns the date in UTC millis.
   */
  public long getUtc() {
    return getNumber();
  }

}
