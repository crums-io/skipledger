/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import static io.crums.out_of_box_test.PathTest.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import  org.junit.jupiter.api.Test;

import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.bags.RowBag;

/**
 * Template test for implementation
 */
public abstract class RowBagTest {
  
  
  
  protected abstract RowBag newBag(SkipLedger ledger, List<Long> rowNumbers);
  
  
  
  @Test
  public void testSmallest() {
    final int size = 1;
    SkipLedger ledger = newRandomLedger(size);
    Path state = ledger.statePath();
    assertEquals(1, state.rows().size());
    RowBag bag = newBag(ledger, state.rowNumbers());
    assertPath(state, bag);
    
  }
  
  

  @Test
  public void test2() {
    testAllToHi(2);
  }
  
  
  @Test
  public void testExhaustiveDozen() {
    for (int size = 3; size < 15; ++size)
      testAllToHi(size);
  }
  

  @Test
  public void testComboBagWith2Paths() {
    final int size = 1027;
    SkipLedger ledger = newRandomLedger(size);
    Path a = ledger.skipPath(1, 1027);
    Path b = ledger.skipPath(22, 1019);
    List<Long> unionRns = mergeRowNumbers(a, b);
    RowBag bag = newBag(ledger, unionRns);
    
    assertPath(a, bag);
    assertPath(b, bag);
  }
  

  @Test
  public void testComboBagWith3Paths() {
    final int size = 1027;
    SkipLedger ledger = newRandomLedger(size);
    Path a = ledger.skipPath(1, 1027);
    Path b = ledger.skipPath(22, 1019);
    Path c = ledger.skipPath(513, 600);
    
    List<Long> unionRns = mergeRowNumbers(mergeRowNumbers(a, b), c.rowNumbers());
    RowBag bag = newBag(ledger, unionRns);
    
    assertPath(a, bag);
    assertPath(b, bag);
    assertPath(c, bag);
  }
  
  

  private List<Long> mergeRowNumbers(Path a, Path b) {
    return mergeRowNumbers(a.rowNumbers(), b.rowNumbers());
  }
  
  
  private List<Long> mergeRowNumbers(List<Long> a, List<Long> b) {
    TreeSet<Long> set = new TreeSet<>(a);
    set.addAll(b);
    ArrayList<Long> merged = new ArrayList<>(set.size());
    merged.addAll(set);
    return merged;
  }
  
  
  
  private void testAllToHi(int size) {
    SkipLedger ledger = newRandomLedger(size);
    final int hi = size;
    for (int lo = 1; lo <= hi; ++lo) {
      testSkipPath(ledger, lo, hi);
    }
  }
  
  
  private void testSkipPath(SkipLedger ledger, int lo, int hi) {
    Path path = ledger.skipPath(lo, hi);
    RowBag bag = newBag(ledger, path.rowNumbers());
    assertPath(path, bag);
  }



  private void assertPath(Path path, RowBag bag) {
    int index = 0;
    for (Row row : path.rows()) {
      long rn = row.rowNumber();
      Row out = bag.getRow(rn);
      
      if (!out.equals(row))
        fail("at index " + index + ": expect " + row + " ; actual " + out);
      
      ++index;
    }
  }
  

}

