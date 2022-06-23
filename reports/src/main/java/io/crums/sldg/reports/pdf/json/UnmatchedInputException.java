/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import io.crums.util.json.JsonParsingException;

/**
 * Missing input (required for JSON parsing). Subclassed in order to
 * distinguish from proper parsing, since these may ultimately be
 * generated from bad user input.
 */
@SuppressWarnings("serial")
public class UnmatchedInputException extends JsonParsingException {
  
  public UnmatchedInputException(String message) {
    super(message);
  }
  
  public UnmatchedInputException(String message, Throwable cause) {
    super(message, cause);
  }

}
