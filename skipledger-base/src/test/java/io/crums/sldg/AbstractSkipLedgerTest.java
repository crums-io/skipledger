/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.testing.IoTestCase;
import io.crums.util.Lists;

/**
 * 
 */
public abstract class AbstractSkipLedgerTest extends IoTestCase {
  
  /** System property name. */
  public final static String TEST_ALL = "testAll";

  private final static ByteBuffer SENTINEL_HASH = DIGEST.sentinelHash();

  

  
  @Test
  public void testEmpty() throws Exception {
    
    final Object label = new Object() {  };
    
    try (SkipLedger ledger = newLedger(label)) {
      
      assertEquals(0, ledger.size());
    }
  }

  
  
  
  
  @Test
  public void testOne() throws Exception {
    
    final Object label = new Object() {  };
    
    try (SkipLedger ledger = newLedger(label)) {
      
      Random random = new Random(1);

      byte[] mockHash = new byte[HASH_WIDTH];
      random.nextBytes(mockHash);
      
      long size = ledger.appendRows(ByteBuffer.wrap(mockHash));
      
      assertEquals(1, size);
      assertEquals(1, ledger.size());
      Row row = ledger.getRow(1);
      
      assertEquals(1L, row.no());
      assertEquals(ByteBuffer.wrap(mockHash), row.inputHash());
      
      assertEquals(SkipLedger.skipCount(1L), row.prevLevels());
      assertEquals(SENTINEL_HASH, row.prevHash(0));
    }
  }
  
  
  @Test
  public void testOneWithTestHarness() throws Exception {
    
    final Object label = new Object() {  };
    
    testImpl(1, label);
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
  public void test1027EnBloc() throws Exception {

    int rows = 1027;
    Object label = new Object() { };
    testAppendEnBloc(rows, label, rows);
  }
  
  @Test
  public void test128k_in_4k_blocks() throws Exception {

    int rows = 128 * 1024 + 5;
    int blockSize = 4 * 1024;
    Object label = new Object() { };
    testAppendEnBloc(rows, label, blockSize);
  }
  
  
  
  
  
  
  @Test
  public void test1M_plus33() throws Exception {

    
    final int rows = 1024*1024 + 33;
    final int blockSize = 16 * 1024;
    Object label = new Object() { };
    
    if (checkEnabled(TEST_ALL, label, true))
      testAppendEnBloc(rows, label, blockSize);
  }
  
  
  
  protected void testImpl(final int rows, Object label) throws Exception {

    String method = method(label);
    System.out.print("== " + method + ":");
    System.out.flush();
    
    try (SkipLedger ledger = newLedger(label)) {

      Random random = new Random(rows);

      byte[][] mHashes = new byte[rows][];
      for (int i = 0; i < rows; ++i) {
        mHashes[i] = new byte[HASH_WIDTH];
        random.nextBytes(mHashes[i]);
        final long rowNum = ledger.appendRows(ByteBuffer.wrap(mHashes[i]));
        assertEquals(i + 1, rowNum);
        assertEquals(rowNum, ledger.size());
        assertRowPointers(ledger, rowNum, ByteBuffer.wrap(mHashes[i]));
      }
    }

    System.out.println(" [DONE]");
  }
  

  protected void testAppendEnBloc(final int rows, Object label, int blockSize) throws Exception {
    
    String method = method(label);
    System.out.print("== " + method + ": " + rows + " rows, " + blockSize + " rows per block ");
    System.out.flush();

    
    try (SkipLedger ledger = newLedger(label)) {

      Random random = new Random(rows);

      byte[][] mHashes = new byte[rows][];
      for (int i = 0; i < rows; ++i) {
        mHashes[i] = new byte[HASH_WIDTH];
        random.nextBytes(mHashes[i]);
      }

      
      int wholeBlocks = rows / blockSize;
      ByteBuffer entryBlock = ByteBuffer.allocate(blockSize * HASH_WIDTH);
      for (int b = 0; b < wholeBlocks; ++b) {
        entryBlock.clear();
        int zeroIndex = b * blockSize;
        for (int i = 0; i < blockSize; ++i) {
          entryBlock.put(mHashes[zeroIndex + i]);
        }
        entryBlock.flip();
        ledger.appendRows(entryBlock);
      }
      
      entryBlock.clear();
      for (int i = wholeBlocks * blockSize; i < rows; ++i)
        entryBlock.put(mHashes[i]);
      
      if (entryBlock.flip().hasRemaining())
        ledger.appendRows(entryBlock);

      for (int index = 0; index < rows; ++index) {
        int rowNumber = index + 1;
        ByteBuffer entryHash = ByteBuffer.wrap(mHashes[index]);
        assertRowPointers(ledger, rowNumber, entryHash);
      }
    } catch (Throwable t) {
      t.printStackTrace();
      throw t;
    }

    System.out.println(" [DONE]");
  }
  
  
  
  private void assertRowPointers(SkipLedger ledger, long rowNumber, ByteBuffer entryHash) {
    
    Row row = ledger.getRow(rowNumber);
    
    assertEquals(rowNumber, row.no());
    assertEquals(entryHash, row.inputHash());
    

    final int skipPtrCount = SkipLedger.skipCount(rowNumber);
    assertEquals(skipPtrCount, row.prevLevels());

    var prevHashes = Lists.functorList(
        skipPtrCount,
        level -> ledger.rowHash(rowNumber - (1L << level)));

    var expectedHash = SkipLedger.rowHash(rowNumber, entryHash, prevHashes);
    assertEquals(expectedHash, row.hash());
  }
  
  
  
  
  
  
  protected SkipLedger newLedger(Object methodLabel) throws Exception {
    SkipTable table = newTable(methodLabel);
    boolean fast = table instanceof VolatileTable;
    return new CompactSkipLedger(table, null, fast);
  }
  
  
  protected abstract SkipTable newTable(Object methodLabel) throws Exception;
  
  
}
