/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

/**
 * Indicates a malformed (cross-nugget) {@linkplain Reference reference}
 * in a bindle.
 */
@SuppressWarnings("serial")
public class MalformedReferenceException extends MalformedBindleException {

  public MalformedReferenceException() {
  }

  public MalformedReferenceException(String s) {
    super(s);
  }

  public MalformedReferenceException(Throwable cause) {
    super(cause);
  }

  public MalformedReferenceException(String message, Throwable cause) {
    super(message, cause);
  }

}
