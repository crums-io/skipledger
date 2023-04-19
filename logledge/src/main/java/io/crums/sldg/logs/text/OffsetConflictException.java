/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import io.crums.sldg.SldgException;

/**
 * Thrown when a recorded (row) offset conflicts with that in a replay
 * of the source. Provides extra info about where the conflict took
 * place. Presently, we record where a row ends (EOL), not where it begins.
 */
@SuppressWarnings("serial")
public class OffsetConflictException extends SldgException {

  private final long rowNumber;
  private final long expectedOffset;
  private final long actualOffset;
  
  
  public OffsetConflictException(long rowNumber, long expectedOffset, long actualOffset) {
    this(
        rowNumber, expectedOffset, actualOffset,
        "expected offset %d; actual was %d (row [%d])"
        .formatted(expectedOffset, actualOffset, rowNumber));
  }


  public OffsetConflictException(
      long rowNumber, long expectedOffset, long actualOffset, String message) {
    super(message);
    
    this.rowNumber = rowNumber;
    this.expectedOffset = expectedOffset;
    this.actualOffset = actualOffset;
    
    if (rowNumber < 1 || expectedOffset < rowNumber || actualOffset < 0)
      throw new RuntimeException(
          "%d:%d:%d".formatted(rowNumber, expectedOffset, actualOffset));
  }


  
  public final long rowNumber() {
    return rowNumber;
  }


  public final long getExpectedOffset() {
    return expectedOffset;
  }


  public final long getActualOffset() {
    return actualOffset;
  }

}
