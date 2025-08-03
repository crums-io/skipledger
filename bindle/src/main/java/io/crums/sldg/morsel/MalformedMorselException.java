/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

/**
 * Base of "malformed" exception hierarchy. Indicates a malformed
 * morsel, or morsel element. These exceptions are used both to indicate
 * illegal arguments in constructors and also for misconfigured,
 * <em>lazy</em>-loaded objects.
 */
@SuppressWarnings("serial")
public class MalformedMorselException extends RuntimeException {

  public MalformedMorselException() {
  }

  public MalformedMorselException(String s) {
    super(s);
  }

  public MalformedMorselException(Throwable cause) {
    super(cause);
  }

  public MalformedMorselException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public synchronized MalformedMorselException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }

}
