/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SkipLedger.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import  org.junit.Test;

import io.crums.util.Lists;

/**
 * 
 */
public class SkipLedgerStaticsTest {
  

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
      assertEquals(delta, Long.highestOneBit(delta));
      long maxDelta = 1L << (skipCount(prevRow) - 1);
      assertTrue(maxDelta >= delta);
    }
  }
  
  
  @Test
  public void testStitch() {
    List<Long> path = skipPathNumbers(3, 1021);
    assertEquals(path, stitch(path));
    int gaps = 3;
    int chainLength = path.size() / (gaps + 1);
    
    ArrayList<Long> withGaps = new ArrayList<>();
    
    withGaps.add(path.get(0));
    for (int index = 1; index < path.size() - 1; ++index) {
      Long rn = path.get(index);
      if (index % chainLength != 0)
        withGaps.add(rn);
    }
    withGaps.add(path.get(path.size() - 1));
    
    assertEquals(path, stitch(withGaps));
  }
  
  
  @Test
  public void testStitchPath() {
    TreeSet<Long> known = new TreeSet<>();
    List<Long> pathA = skipPathNumbers(11, 513);
    
    // test some trivial things first..
    known.addAll(pathA);
    Optional<List<Long>> pathOpt = stitchPath(known, pathA.get(0), pathA.get(pathA.size() - 1));
    assertTrue(pathOpt.isPresent());
    assertEquals(pathA, pathOpt.get());
    
    List<Long> expected = Lists.asReadOnlyList(256L, 512L, 513L);
    
    pathOpt = stitchPath(known, 256, 513);
    assertTrue(pathOpt.isPresent());
    assertEquals(expected, pathOpt.get());
    
    // now make the known set a bit more rich..
    List<Long> pathB = skipPathNumbers(499, 513);
    known.addAll(pathB);
    
    // assert we can still do the simple
    pathOpt = stitchPath(known, pathB.get(0), pathB.get(pathB.size() - 1));
    assertTrue(pathOpt.isPresent());
    assertEquals(pathB, pathOpt.get());
    
    // assert there's no path [11:499]
    pathOpt = stitchPath(known, 11, 499);
    assertTrue(pathOpt.isEmpty());
    
    // assert subpath of pathB
    expected = Lists.asReadOnlyList(500L , 504L, 512L);

    pathOpt = stitchPath(known, 500, 512);
    assertTrue(pathOpt.isPresent());
    assertEquals(expected, pathOpt.get());

    pathOpt = stitchPath(known, expected);
    assertTrue(pathOpt.isPresent());
    assertEquals(expected, pathOpt.get());
    
    // make it more interesting.. add enough rows for a camel-like path
    
    known.addAll(skipPathNumbers(514, 557));
    pathOpt = stitchPath(known, 11, 557);
    assertTrue(pathOpt.isPresent());
    List<Long> stitched = pathOpt.get();
    
    assertLinked(stitched, 11, 557);
    
    
    List<Long> targets = Lists.asReadOnlyList(11L, 512L, 513L, 514L, 556L);
    pathOpt = stitchPath(known, targets);
    assertTrue(pathOpt.isPresent());
    stitched = pathOpt.get();
    assertTrue(stitched.containsAll(targets));
    assertLinked(stitched, 11, 556);
  }
  
  
  private void assertLinked(List<Long> path, long lo, long hi) {
    assertEquals(lo, path.get(0).longValue());
    assertEquals(hi, path.get(path.size() - 1).longValue());
    for (int index = path.size(); index-- > 1; ) {
      long first = path.get(index - 1);
      long second = path.get(index);
      assertTrue(second > first);
      assertTrue(first > 0);
      assertTrue(rowsLinked(first, second));
    }
  }
  
  
  
  @Test
  public void testRefOnlyCoverage() {
    List<Long> targets = Lists.asReadOnlyList(11L, 512L, 513L, 514L, 556L);
    SortedSet<Long> coverage = SkipLedger.coverage(targets);
    assertTrue(coverage.containsAll(targets));
  }
  
}





