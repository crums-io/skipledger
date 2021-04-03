/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;

import io.crums.sldg.SldgException;


/**
 * When nonsensical binary data is encountered, this exception is thrown.
 */
@SuppressWarnings("serial")
public class ByteFormatException extends SldgException {

  public ByteFormatException(String message) {
    super(message);
  }

  public ByteFormatException(Throwable cause) {
    super(cause);
  }

  public ByteFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  public ByteFormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  @Override
  public synchronized ByteFormatException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  
  

}
