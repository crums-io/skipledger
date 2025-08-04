/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

/**
 * Base of "malformed" exception hierarchy. Indicates a malformed
 * bindle, or bindle element. These exceptions are used both to indicate
 * illegal arguments in constructors and also for misconfigured,
 * <em>lazy</em>-loaded objects.
 */
@SuppressWarnings("serial")
public class MalformedBindleException extends RuntimeException {

  public MalformedBindleException() {
  }

  public MalformedBindleException(String s) {
    super(s);
  }

  public MalformedBindleException(Throwable cause) {
    super(cause);
  }

  public MalformedBindleException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public synchronized MalformedBindleException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }

}
