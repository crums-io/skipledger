/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

import io.crums.sldg.bindle.tc.NotarizedRow;

/**
 * Indicates a malformed {@linkplain NotarizedRow notarized row} in a bindle.
 */
@SuppressWarnings("serial")
public class MalformedNotarizedRowException extends MalformedBindleException {

  public MalformedNotarizedRowException() {
  }

  public MalformedNotarizedRowException(String s) {
    super(s);
  }

  public MalformedNotarizedRowException(Throwable cause) {
    super(cause);
  }

  public MalformedNotarizedRowException(String message, Throwable cause) {
    super(message, cause);
  }

}
