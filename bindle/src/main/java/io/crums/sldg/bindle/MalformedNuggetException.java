/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

/**
 * Indicates a malformed {@linkplain Nugget}.
 */
@SuppressWarnings("serial")
public class MalformedNuggetException extends MalformedBindleException {

  public MalformedNuggetException() {
  }

  public MalformedNuggetException(String s) {
    super(s);
  }

  public MalformedNuggetException(Throwable cause) {
    super(cause);
  }

  public MalformedNuggetException(String message, Throwable cause) {
    super(message, cause);
  }

}

