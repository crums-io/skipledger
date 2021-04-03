/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

/**
 * 
 */
@SuppressWarnings("serial")
public class HashConflictException extends SldgException {

  public HashConflictException(String message) {
    super(message);
  }

  public HashConflictException(Throwable cause) {
    super(cause);
  }

  public HashConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  public HashConflictException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
