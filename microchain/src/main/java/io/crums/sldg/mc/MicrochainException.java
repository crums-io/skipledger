/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc;

/**
 * Indicates an error in a {@linkplain Microchain}.
 */
@SuppressWarnings("serial")
public class MicrochainException extends RuntimeException {

  public MicrochainException() {  }

  public MicrochainException(String message) {
    super(message);
  }

  public MicrochainException(Throwable cause) {
    super(cause);
  }

  public MicrochainException(String message, Throwable cause) {
    super(message, cause);
  }

  

}
