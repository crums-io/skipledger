/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import static io.crums.util.ledger.SkipLedger.*;
import static org.junit.Assert.*;

import  org.junit.Test;

/**
 * 
 */
public class SkipLedgerTest {

  

  @Test
  public void testSkipCount() {
    assertEquals(1, skipCount(1));
    assertEquals(2, skipCount(2));
    assertEquals(1, skipCount(3));
    assertEquals(3, skipCount(4));
    assertEquals(1, skipCount(5));
    assertEquals(2, skipCount(6));
    assertEquals(1, skipCount(7));
    assertEquals(4, skipCount(8));
    assertEquals(1, skipCount(9));
    assertEquals(2, skipCount(10));
    assertEquals(1, skipCount(11));
    assertEquals(3, skipCount(12));
    assertEquals(1, skipCount(13));
    assertEquals(2, skipCount(14));
    assertEquals(1, skipCount(15));
    assertEquals(5, skipCount(16));
    assertEquals(1, skipCount(17));
  }
  

  @Test
  public void testIndex() {
    assertEquals(0, cellNumber(1));
    
    for (int i = 1; i < 1027; ++i)
      testIndex(i);
  }

  private void testIndex(long rowNumber) {
    int sum = 0;
    for (int row = 1; row < rowNumber; ++row)
      sum += 1 + skipCount(row);
    
    assertEquals(sum, cellNumber(rowNumber));
  }
  
  
  
  @Test
  public void testMaxRows() {
    // making this value large doesn't make much difference in execution time;
    // it gets jit'ed away
    // LOL
    final int maxCount = 100025;
    for (int c = 0; c < maxCount; ++c)
      testMaxRows(c);
    
    for (long c = Integer.MAX_VALUE, count = maxCount; count-- > 0; ++c)
      testMaxRows(c);
  }
  
  
  private void testMaxRows(long cells) {
    long rows = maxRows(cells);
    assertTrue(cells >= cellNumber(rows + 1));
    assertTrue(cells < cellNumber(rows + 2));
  }
  
  
  
  
}














