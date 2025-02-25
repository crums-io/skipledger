/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;

/**
 * Indicates an error condition involving a {@linkplain SourcePack},
 * usually when it is being deserialized.
 */
@SuppressWarnings("serial")
public class SourcePackException extends RuntimeException {

  public SourcePackException() {
  }

  public SourcePackException(String message) {
    super(message);
  }

  public SourcePackException(Throwable cause) {
    super(cause);
  }

  public SourcePackException(String message, Throwable cause) {
    super(message, cause);
  }

  public SourcePackException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  
  @Override
  public synchronized SourcePackException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  

}







