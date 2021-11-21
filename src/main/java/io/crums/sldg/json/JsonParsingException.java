/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;

/**
 * Unchecked exception for illegal JSON input.
 */
@SuppressWarnings("serial")
public class JsonParsingException extends RuntimeException {

  /**
   * 
   */
  public JsonParsingException() {
  }

  /**
   * @param s
   */
  public JsonParsingException(String s) {
    super(s);
  }

  /**
   * @param cause
   */
  public JsonParsingException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public JsonParsingException(String message, Throwable cause) {
    super(message, cause);
  }

}
