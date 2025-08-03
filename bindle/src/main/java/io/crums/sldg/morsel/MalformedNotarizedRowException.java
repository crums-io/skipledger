/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

import io.crums.sldg.morsel.tc.NotarizedRow;

/**
 * Indicates a malformed {@linkplain NotarizedRow notarized row} in a morsel.
 */
@SuppressWarnings("serial")
public class MalformedNotarizedRowException extends MalformedMorselException {

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
