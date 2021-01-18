/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.RowIntersect.*;
import static io.crums.sldg.PathTest.newRandomLedger;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

/**
 * 
 */
public class PathIntersectorTest extends SelfAwareTestCase {

  
  
  @Test
  public void testTiny() {
    Object label = new Object() { };
    
    int lo = 9;
    int hi = 12;
    int expected = 8;
    
    
    testTiny(label, lo, hi, expected, REFNUM_REFNUM);
  }
  
  
  @Test
  public void testTiny2() {
    Object label = new Object() { };
    
    int lo = 18;
    int hi = 32;
    int expected = 16;
    
    testTiny(label, lo, hi, expected, REFNUM_REFNUM);
  }
  
  
  @Test
  public void testTiny3() {
    Object label = new Object() { };
    
    int lo = 8;
    int hi = 10;
    int expected = 8;
    
    testTiny(label, lo, hi, expected, NUM_REFNUM);
  }
  
  
  @Test
  public void testTiny4() {
    Object label = new Object() { };
    
    int lo = 11;
    int hi = 12;
    int expected = 10;
    
    testTiny(label, lo, hi, expected, REFNUM_REFNUM);
  }
  
  
  
  private void testTiny(Object label, int lo, int hi, int expected, RowIntersect type) {

    int rows = hi;
    System.out.println();
    System.out.println(method(label) + ": " + rows + " rows");

    Ledger ledger = newRandomLedger(rows);
    
    Path a = new Path(Collections.singletonList(ledger.getRow(lo)));
    Path b = new Path(Collections.singletonList(ledger.getRow(hi)));
    
    PathIntersector i = new PathIntersector(a, b);
    
    assertTrue(i.hasNext());
    
    RowIntersection ri = i.next();
    assertEquals(expected, ri.rowNumber());
    assertEquals(type, ri.type());
    
    
    printUnique(i);
  }

  
  @Test
  public void testSmall() {
    
    final Object label = new Object() { };
    final int rows = 129;
    System.out.println(method(label) + ": " + rows + " rows");
    
    Ledger ledger = newRandomLedger(rows);
    
    final long lo = 34;
    final long hi = 101;
    
    final long lo2 = 33;
    final long hi2 = rows;
    
    Path path1 = ledger.skipPath(lo, hi);
    Path path2 = ledger.skipPath(lo2, hi2);
    
    PathIntersector iter = new PathIntersector(path1, path2);
    while (iter.hasNext())
      System.out.println(iter.next());
    
    System.out.println();
    printUnique(iter);
    
  }
  
  
  private void printUnique(PathIntersector iter) {
    new UniquePairIntersector(iter).forEach(i -> System.out.println(i));
  }

  

}
