/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


/**
 * Indicates a failure caused by concurrent modifications to the backing
 * database.
 */
@SuppressWarnings("serial")
public class ConcurrentChainModificationException extends ChainManagementException {

  public ConcurrentChainModificationException() {
  }

  public ConcurrentChainModificationException(String message) {
    super(message);
  }

  public ConcurrentChainModificationException(Throwable cause) {
    super(cause);
  }

  public ConcurrentChainModificationException(String message, Throwable cause) {
    super(message, cause);
  }

}
