/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


/**
 * Signifies a misaligned offset recorded in an <em>alf</em> file.
 * <p>
 * TODO: work out exception class hierarchy.
 * </p>
 */
@SuppressWarnings("serial")
public class AlfException extends RuntimeException {

  /**
   * 
   */
  public AlfException() {
    // TODO Auto-generated constructor stub
  }

  public AlfException(String message) {
    super(message);
  }

  public AlfException(Throwable cause) {
    super(cause);
  }

  public AlfException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlfException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
