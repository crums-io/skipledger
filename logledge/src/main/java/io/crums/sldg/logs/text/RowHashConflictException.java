/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import io.crums.sldg.HashConflictException;

/**
 * 
 */
@SuppressWarnings("serial")
public class RowHashConflictException extends HashConflictException {

  private final long rn;
  
  
  public RowHashConflictException(long rn) {
    this(rn, "hash conflict at row [" + rn + "]");
  }
  
  public RowHashConflictException(long rn, String message) {
    super(message);
    this.rn = rn;
    if (rn < 1)
      throw new RuntimeException("rn " + rn);
  }
  
  
  public final long rowNumber() {
    return rn;
  }

}
