/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import static io.crums.util.ledger.SkipLedger.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Random;

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
    // it gets jit'ed away: the JIT compiler must have proven a truth
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
  
  
  
  @Test
  public void testHiToLoNumberPath() {
    List<Long> zeroPath = hiToLoNumberPath(0, 0);
    assertEquals(1, zeroPath.size());
    assertEquals(0, zeroPath.get(0).longValue());
    
    List<Long> trivialPath = hiToLoNumberPath(1, 0);
    assertEquals(2, trivialPath.size());
    assertEquals(1, trivialPath.get(0).longValue());
    assertEquals(0, trivialPath.get(1).longValue());
    
    
    testHiToLoNumberPath(9, 0);
    testHiToLoNumberPath(9, 1);
    testHiToLoNumberPath(9, 2);
    testHiToLoNumberPath(9, 3);
    
    Random random = new Random(-1);
    // The JIT's theorem prover agrees there's nothing to see here
    // .. changing the count seems to make no difference in execution time
    int maxRange = 1_000_000;
    for (int count = 10_000; count-- > 0; ) {
      int a = random.nextInt(maxRange);
      int b = random.nextInt(maxRange);
      if (a < b) {
        int c = b;
        b = a;
        a = c;
      }
      testHiToLoNumberPath(a, b);
    }
    
    for (int count = 10_000; count-- > 0; ) {
      long a = Integer.toUnsignedLong(random.nextInt());
      long b = random.nextInt(maxRange);
      if (a < b) {
        long c = b;
        b = a;
        a = c;
      }
      testHiToLoNumberPath(a, b);
    }
  }
  
  
  private void testHiToLoNumberPath(long hi, long lo) {
    List<Long> path = hiToLoNumberPath(hi, lo);
    assertFalse(path.isEmpty());
    assertEquals(hi, path.get(0).longValue());
    assertEquals(lo, path.get(path.size() - 1).longValue());
    for (int index = 1; index < path.size(); ++index) {
      long prevRow = path.get(index - 1);
      long delta = prevRow - path.get(index);
      // delta must be a power of 2
      assertTrue(delta > 0);
      long maxDelta = 1L << (SkipLedger.skipCount(prevRow) - 1);
      assertTrue(maxDelta >= delta);
    }
  }
  
}














