/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import io.crums.sldg.mem.VolatileTable;

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

}















