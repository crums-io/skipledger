/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;

/**
 * Signifies an error with an "indexed" log file. This can happen if
 * the log is edited, or if the grammar is modified.
 * <p>
 * TODO: work out exception class hierarchy.
 * </p>
 */
@SuppressWarnings("serial")
public class SourceIndexException extends IllegalStateException {

  public SourceIndexException() {
  }

  public SourceIndexException(String s) {
    super(s);
  }

  public SourceIndexException(Throwable cause) {
    super(cause);
  }

  public SourceIndexException(String message, Throwable cause) {
    super(message, cause);
  }

}

