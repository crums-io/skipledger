/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


/**
 * Indicates a line number conflict at a specific row number.
 */
@SuppressWarnings("serial")
public class LineNoConflictException extends OffsetConflictException {
  
  
  private static String makeMessage(
      long rowNumber,
      long expectedOffset, long actualOffset,
      long expectedLineNo, long actualLineNo) {
    
    String what =
        expectedOffset == actualOffset ? "line no." : (
            expectedLineNo == actualLineNo ?
                "EOL offset" : "EOL offset and line no."
            );
    return
        "%s conflict at row [%d] (expected/actual): line (%d/%d); EOL (%d/%d)"
        .formatted(
            what,
            rowNumber,
            expectedLineNo, actualLineNo,
            expectedOffset, actualOffset);
  }
  
  private final long expectedLineNo;
  private final long actualLineNo;


  
  public LineNoConflictException(State expected, State actual) {
    this(
        expected.rowNumber(),
        expected.eolOffset(),
        actual.eolOffset(),
        expected.lineNo(),
        actual.lineNo());
    
    assertSameRn(expected, actual);
  }
  
  public LineNoConflictException(State expected, State actual, String message) {
    this(
        expected.rowNumber(),
        expected.eolOffset(),
        actual.eolOffset(),
        expected.lineNo(),
        actual.lineNo(),
        message);
    
    assertSameRn(expected, actual);
  }
  

  private void assertSameRn(State expected, State actual) {
    if (expected.rowNumber() != actual.rowNumber())
      throw new RuntimeException("assertion failed: %d:%d"
          .formatted(expected.rowNumber(), actual.rowNumber()));
  }

  public LineNoConflictException(
      long rowNumber,
      long expectedOffset, long actualOffset,
      long expectedLineNo, long actualLineNo) {
    
    this(
        rowNumber, expectedOffset, actualOffset, expectedLineNo, actualLineNo,
        makeMessage(rowNumber, expectedOffset, actualOffset, expectedLineNo, actualLineNo));
  }

  
  /**
   * Full constructor.
   */
  public LineNoConflictException(
      long rowNumber,
      long expectedOffset, long actualOffset,
      long expectedLineNo, long actualLineNo,
      String message) {
    
    super(rowNumber, expectedOffset, actualOffset, message);
    this.expectedLineNo = expectedLineNo;
    this.actualLineNo = actualLineNo;
    
    if (expectedLineNo < rowNumber || actualLineNo < 0)
      throw new RuntimeException("assertion failed: %d:%d:%d"
          .formatted(rowNumber, expectedLineNo, actualLineNo));
  }
  
  
  

  public final long getExpectedLineNo() {
    return expectedLineNo;
  }

  public final long getActualLineNo() {
    return actualLineNo;
  }

}
