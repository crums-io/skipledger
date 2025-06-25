/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;

import java.io.File;

/**
 * Signifies a bad header in an <em>.lgl</em> file.
 * <p>
 * TODO: work out exception class hierarchy.
 * </p>
 */
@SuppressWarnings("serial")
public class BadHeaderException extends RuntimeException {
  
  
  public BadHeaderException(File file) {
    this("expected header bytes not found: " + file);
  }

  public BadHeaderException() {
  }

  public BadHeaderException(String message) {
    super(message);
  }

  public BadHeaderException(Throwable cause) {
    super(cause);
  }

  public BadHeaderException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadHeaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
