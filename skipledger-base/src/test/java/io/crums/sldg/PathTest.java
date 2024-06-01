/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class PathTest {
  
  
  
  
  
  public static SkipLedger newRandomLedger(int rows) {
    if (rows <= 0)
      throw new IllegalArgumentException("rows " + rows);
    
    SkipLedger ledger = newLedger();
    
    Random random = new Random(rows);
    
    addRandomRows(ledger, random, rows);
    
    assertEquals(rows, ledger.size());
    return ledger;
  }
  
  
  public static SkipLedger newLedger() {
    return new CompactSkipLedger(new VolatileTable());
  }
  
  
  public static void addRandomRows(SkipLedger ledger, Random random, int count) {

    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      ledger.appendRows(mockHashBuffer);
    }
  }
  
  
  
  
  
  

  
  
  @Test
  public void testRowHash() {
    
    final int rows = 99091;
    
    final long rowNumber = 0x18300;
    
    SkipLedger ledger = newRandomLedger(rows);
    
    Row row = ledger.getRow(rowNumber);
    
    int ptrs = SkipLedger.skipCount(rowNumber);
    for (int i = 0; i < ptrs; ++i) {
      long delta = 1L << i;
      long refNum = rowNumber - delta;
      ByteBuffer hash = row.hash(refNum);
      if (refNum == 0) {
        while (hash.hasRemaining())
          assertEquals(0, hash.get());
        continue;
      }
      Row refRow = refNum == row.rowNumber() ? row : ledger.getRow(refNum);
      assertEquals(refRow.hash(), hash);
    }
  }



  @Test
  public void testBogusPath() {
    var randA = new Random(56);
    var randB = new Random(10);

    var ledgerA = newLedger();
    var ledgerB = newLedger();

    addRandomRows(ledgerA, randA, 100);
    addRandomRows(ledgerB, randB, 100);

    Path pathA = ledgerA.getPath(1L, 5L);
    Path pathB = ledgerB.getPath(6L);
    var rows = new ArrayList<>(pathA.rows());
    rows.add(pathB.getRowByNumber(6L));
    try {
      new Path(rows);
      fail();
    } catch (HashConflictException expected) {  }

    rows.clear();
    rows.addAll(pathA.rows());
    rows.add(ledgerA.getRow(8L));

    try {
      new Path(rows);
      fail();
    } catch (IllegalArgumentException expected) {  }
  }


  @Test
  public void testSubPath() {
    Path path = newRandomLedger(9).statePath();
    var subPath = path.subPath(1, 4);
    assertEquals(2L, subPath.loRowNumber());
    assertEquals(8L, subPath.hiRowNumber());
  }



  @Test
  public void testEquals() {
    var ledger = newRandomLedger(24);
    Path path = ledger.statePath().pack().path();
    assertEquals(path, ledger.statePath());

    var subPath = path.subPath(1, path.rows().size() - 1);
    var subPath2 = path.subPath(0, path.rows().size() -2);
    assertFalse(subPath.equals(subPath2));

    Path path2 = newRandomLedger(25).getPath(1L, 24L);
    assertEquals(path.rowNumbers(), path2.rowNumbers());
    assertFalse(path.equals(path2));
  }


  @Test
  public void testTailPath() {
    var ledger = newRandomLedger(25);
    final Path path = ledger.statePath().pack().path();

    Path tail = path.tailPath(1L);
    assertEquals(path, tail);

    tail = path.tailPath(2L);
    assertEquals(2L, tail.loRowNumber());
    assertEquals(path.length() - 1, tail.length());
    assertEquals(path.hiRowNumber(), tail.hiRowNumber());

    tail = path.tailPath(3L);
    assertEquals(4L, tail.loRowNumber());
    assertEquals(path.length() - 2, tail.length());
    assertEquals(path.hiRowNumber(), tail.hiRowNumber());

    assertEquals(tail, path.tailPath(4L));

    tail = path.tailPath(24L);
    assertEquals(2, tail.length());

    try {
      path.tailPath(26);
      fail();
    } catch (IllegalArgumentException expected) { }
  }


  @Test
  public void testHeadPath() {
    var ledger = newRandomLedger(52);
    final Path path = ledger.statePath().pack().path();

    Path head = path.headPath(path.hiRowNumber() + 1L);
    assertEquals(path, head);

    try {
      path.headPath(1L);
      fail();
    } catch (IllegalArgumentException expected) {  }

    head = path.headPath(2L);
    assertEquals(1, head.length());
    assertEquals(1L, head.hiRowNumber());
    assertEquals(1L, head.loRowNumber());

    head = path.headPath(3L);
    assertEquals(2, head.length());
    assertEquals(2L, head.hiRowNumber());
    assertEquals(1L, head.loRowNumber());

    assertEquals(head, path.headPath(4L));
  }


  @Test
  public void testHighestCommonNo() {
    var ledger = newRandomLedger(52);
    final Path path = ledger.statePath().pack().path();

    Path path2 = ledger.getPath(1L, 17L);

    assertEquals(16L, path.highestCommonNo(path2));

    path2 = ledger.getPath(2L, 22L);
    assertEquals(16L, path.highestCommonNo(path2));
  }



  @Test
  public void testHasRowCovered() {
    var ledger = newRandomLedger(52);
    final Path path = ledger.statePath().pack().path();

    for (long rn = 0L; rn < 5L; ++rn)
      assertTrue(path.hasRowCovered(rn));
    assertFalse(path.hasRowCovered(5L));
    for (long rn = 6L; rn < 9L; ++rn)
      assertTrue(path.hasRowCovered(rn));
    for (long rn = 9L; rn < 12L; ++rn)
      assertFalse(path.hasRowCovered(rn));
    assertTrue(path.hasRowCovered(12L));
    assertFalse(path.hasRowCovered(13L));
    for (long rn = 14L; rn < 17L; ++rn)
      assertTrue(path.hasRowCovered(rn));
    for (long rn = 17L; rn < 24L; ++rn)
      assertFalse(path.hasRowCovered(rn));
    assertTrue(path.hasRowCovered(24L));
    for (long rn = 25L; rn < 28L; ++rn)
      assertFalse(path.hasRowCovered(rn));
    assertTrue(path.hasRowCovered(28L));
    assertFalse(path.hasRowCovered(29L));
    for (long rn = 30L; rn < 33L; ++rn)
      assertTrue(path.hasRowCovered(rn));
    for (long rn = 33L; rn < 40L; ++rn)
      assertFalse(path.hasRowCovered(rn));
    assertTrue(path.hasRowCovered(40L));
    for (long rn = 41L; rn < 44L; ++rn)
      assertFalse(path.hasRowCovered(rn));
    assertTrue(path.hasRowCovered(44L));
    assertFalse(path.hasRowCovered(45L));
    for (long rn = 46L; rn < 49L; ++rn)
      assertTrue(path.hasRowCovered(rn));
    assertFalse(path.hasRowCovered(49L));
    for (long rn = 50L; rn < 53L; ++rn)
      assertTrue(path.hasRowCovered(rn));

  }

}




