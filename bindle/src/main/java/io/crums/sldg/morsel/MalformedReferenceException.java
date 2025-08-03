/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;

/**
 * Indicates a malformed reference in a morsel. This is thrown at
 * the last stage of the validation / construction of a morsel
 * {@linkplain Reference reference} when the cross-ledeger entry
 * fails to meet the required criteria.
 */
@SuppressWarnings("serial")
public class MalformedReferenceException extends MalformedMorselException {

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
