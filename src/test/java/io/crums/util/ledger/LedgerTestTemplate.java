/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import static io.crums.util.ledger.SkipLedger.SHA256_WIDTH;
import static io.crums.util.ledger.SkipLedger.skipCount;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.IoTestCase;

/**
 * Factored out test code so it can be used against multiple implementations.
 * 
 * @see #newLedger(int, Object)
 */
public abstract class LedgerTestTemplate<L extends SkipLedger> extends IoTestCase {
  
  public final static int HASH_WIDTH = SHA256_WIDTH;
  private final ByteBuffer SENTINEL_HASH = ByteBuffer.allocate(HASH_WIDTH).asReadOnlyBuffer();

  
  @Test
  public void testEmpty() throws Exception {
    
    int cells = 0;
    
//    ByteBuffer mem = ByteBuffer.allocate(cells * HASH_WIDTH);
    
    L ledger = newLedger(cells, new Object() { });
    
    assertEquals(0, ledger.size());
    
    close(ledger);
    
//    try {
//      ledger.appendNextRow(ByteBuffer.allocate(HASH_WIDTH));
//      fail();
//    } catch (RuntimeException expected) {
//      System.out.println("Expected exception: " + expected);
//    }
  }
  
  
  @Test
  public void testOne() throws Exception {
    
    int cells = 2;
    Random random = new Random(cells);
    
//    ByteBuffer mem = ByteBuffer.allocate(cells * HASH_WIDTH);
    
    L ledger = newLedger(cells, new Object() { });
    
    assertEquals(0, ledger.size());
//    assertEquals(1, ledger.maxRows());
    
    byte[] mockHash = new byte[HASH_WIDTH];
    random.nextBytes(mockHash);
    
    long size = ledger.appendNextRow(ByteBuffer.wrap(mockHash));
    assertEquals(1, size);
    assertEquals(1, ledger.size());
    
    ByteBuffer firstRow = ledger.getRow(1);
    
    int expectedCells = 1 + skipCount(1);
    assertEquals(2, expectedCells);
    assertEquals(expectedCells * HASH_WIDTH, firstRow.remaining());
    
    firstRow = firstRow.slice();
    firstRow.limit(HASH_WIDTH);
    assertEquals(ByteBuffer.wrap(mockHash), firstRow);
    
    firstRow.clear().position(HASH_WIDTH);
    assertEquals(SENTINEL_HASH, firstRow);
    
    close(ledger);
  }
  

  
  

  
  @Test
  public void testTwo() throws Exception {

    final int rows = 2;
    Object label = new Object() { };
    testImpl(rows, label);
  }
  
  
  
  @Test
  public void test1027() throws Exception {

    int rows = 1027;
    Object label = new Object() { };
    testImpl(rows, label);
  }
  
  
  
  @Test
  public void test1M_plus33() throws Exception {

    final int rows = 1024*1024 + 33;
    Object label = new Object() { };
    testImpl(rows, label);
  }
  
  
  protected void testImpl(final int rows, Object label) throws Exception {
    String method = method(label);
    System.out.print("== " + method + ":");
//    System.out.println();
    
    int cells = 3 * rows;
    Random random = new Random(cells);
    
    L ledger = newLedger(cells, label);
    
    byte[][] mHashes = new byte[rows][];
    for (int i = 0; i < rows; ++i) {
      mHashes[i] = new byte[HASH_WIDTH];
      random.nextBytes(mHashes[i]);
      final long rowNum = ledger.appendNextRow(ByteBuffer.wrap(mHashes[i]));
      assertEquals(i + 1, rowNum);
      assertEquals(rowNum, ledger.size());
      final int skipPtrCount = skipCount(rowNum);
      ByteBuffer row = ledger.getRow(rowNum);
      if (row.remaining() != row.capacity())
        row = row.slice();
      int expectedBytes = (1 + skipPtrCount) * HASH_WIDTH;
      assertEquals(expectedBytes, row.remaining());
      
      assertEquals(ByteBuffer.wrap(mHashes[i]), row.duplicate().limit(HASH_WIDTH));
      for (int p = 0; p < skipPtrCount; ++p) {
        long referencedRowNum = rowNum - (1 << p);
        int pos = (1 + p) * HASH_WIDTH;
        int limit = pos + HASH_WIDTH;
        ByteBuffer hashPtr = row.duplicate().position(pos).limit(limit);
        ByteBuffer referencedRowHash = ledger.rowHash(referencedRowNum);
        assertEquals(referencedRowHash, hashPtr);
      }
    }
    close(ledger);
    System.out.println(" [DONE]");
  }
  
  
  /**
   * 
   * @param cellsCapacity the maximum capacity of the 
   * @param lablel
   * @return
   */
  protected abstract L newLedger(int cellsCapacity, Object label) throws Exception;
  
  /**
   * Override if any resources owned by the ledger need to be closed.
   */
  protected void close(L ledger) throws Exception {
    
  }

}

