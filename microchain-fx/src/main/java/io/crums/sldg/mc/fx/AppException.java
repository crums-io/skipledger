/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

/**
 * 
 */
@SuppressWarnings("serial")
public class AppException extends RuntimeException {

  public AppException() {
  }

  public AppException(String message) {
    super(message);
  }

  public AppException(Throwable cause) {
    super(cause);
  }

  public AppException(String message, Throwable cause) {
    super(message, cause);
  }

  
  @Override
  public synchronized AppException fillInStackTrace() {
    super.fillInStackTrace();
    return this;
  }
  
  
  

}
