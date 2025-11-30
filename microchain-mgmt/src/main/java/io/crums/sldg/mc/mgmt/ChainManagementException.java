/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;

/**
 * Exception thrown by this module.
 */
@SuppressWarnings("serial")
public class ChainManagementException extends RuntimeException {

  public ChainManagementException() {
  }

  public ChainManagementException(String message) {
    super(message);
  }

  public ChainManagementException(Throwable cause) {
    this("internal error caused by: " + cause, cause);
  }

  public ChainManagementException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public synchronized ChainManagementException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  
  

}
