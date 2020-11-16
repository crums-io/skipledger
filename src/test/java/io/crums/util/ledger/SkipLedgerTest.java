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
  
}














