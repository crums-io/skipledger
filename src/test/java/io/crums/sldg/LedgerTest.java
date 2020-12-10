/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.Ledger.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Random;

import  org.junit.Test;

import io.crums.util.Lists;

/**
 * 
 */
public class LedgerTest {
  

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
  public void testSkipPathNumbers() {
    
//    List<Long> zeroPath = hiToLoNumberPath(0, 0);
//    assertEquals(1, zeroPath.size());
//    assertEquals(0, zeroPath.get(0).longValue());
//    
    List<Long> trivialPath = skipPathNumbers(1, 1);
    assertEquals(1, trivialPath.size());
    assertEquals(1, trivialPath.get(0).longValue());
//    
//    
//    testSkipPathNumbers(9, 0);
    testSkipPathNumbers(9, 1);
    testSkipPathNumbers(9, 2);
    testSkipPathNumbers(9, 3);
    
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
      testSkipPathNumbers(a, b);
    }
    
    for (int count = 10_000; count-- > 0; ) {
      long hi = Integer.toUnsignedLong(random.nextInt());
      long lo = random.nextInt(maxRange);
      if (hi < lo) {
        long c = lo;
        lo = hi;
        hi = c;
      }
      testSkipPathNumbers(hi, lo);
    }
  }
  
  
  private void testSkipPathNumbers(long hi, long lo) {
    // the reversing below is an artifact of the fact in the initial
    // design this was ordered in from hi row number to lo. 
    List<Long> path = Lists.reverse(skipPathNumbers(lo, hi));
    assertFalse(path.isEmpty());
    assertEquals(hi, path.get(0).longValue());
    assertEquals(lo, path.get(path.size() - 1).longValue());
    for (int index = 1; index < path.size(); ++index) {
      long prevRow = path.get(index - 1);
      long delta = prevRow - path.get(index);
      // delta must be a power of 2
      assertTrue(delta > 0);
      long maxDelta = 1L << (skipCount(prevRow) - 1);
      assertTrue(maxDelta >= delta);
    }
  }

}






