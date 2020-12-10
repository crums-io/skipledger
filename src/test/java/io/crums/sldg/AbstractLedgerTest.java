/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.IoTestCase;

/**
 * 
 */
public abstract class AbstractLedgerTest extends IoTestCase {
  
  private final static ByteBuffer SENTINEL_HASH = DIGEST.sentinelHash();

  

  
  @Test
  public void testEmpty() throws Exception {
    
    final Object label = new Object() {  };
    
    try (Ledger ledger = newLedger(label)) {
      
      assertEquals(0, ledger.size());
    }
  }
  
  
  
  
  @Test
  public void testOne() throws Exception {
    
    final Object label = new Object() {  };
    
    try (Ledger ledger = newLedger(label)) {
      
      Random random = new Random(1);

      byte[] mockHash = new byte[HASH_WIDTH];
      random.nextBytes(mockHash);
      
      long size = ledger.appendRows(ByteBuffer.wrap(mockHash));
      
      assertEquals(1, size);
      assertEquals(1, ledger.size());
      Row row = ledger.getRow(1);
      
      assertEquals(1L, row.rowNumber());
      assertEquals(ByteBuffer.wrap(mockHash), row.inputHash());
      
      assertEquals(Ledger.skipCount(1L), row.prevLevels());
      assertEquals(SENTINEL_HASH, row.prevHash(0));
      
      ByteBuffer expectedData = ByteBuffer.allocate(2* HASH_WIDTH);
      expectedData.put(mockHash).put(SENTINEL_HASH.duplicate()).flip();
      assertEquals(expectedData, row.data());
      
      ByteBuffer expectedHash;
      {
        MessageDigest digest = DIGEST.newDigest();
        // this taxes the lazy-loading implementation
        digest.update(row.data());
        expectedHash = ByteBuffer.wrap(digest.digest());
      }
      assertEquals(expectedHash, row.hash());
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
  public void test1M_plus33() throws Exception {

    final int rows = 1024*1024 + 33;
    final int blockSize = 16 * 1024;
    Object label = new Object() { };
    testAppendEnBloc(rows, label, blockSize);
  }
  
  
  
  protected void testImpl(final int rows, Object label) throws Exception {

    String method = method(label);
    System.out.print("== " + method + ":");
    
    try (Ledger ledger = newLedger(label)) {

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

    
    try (Ledger ledger = newLedger(label)) {

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
    }

    System.out.println(" [DONE]");
  }
  
  
  
  private void assertRowPointers(Ledger ledger, long rowNumber, ByteBuffer entryHash) {
    
    Row row = ledger.getRow(rowNumber);
    
    assertEquals(rowNumber, row.rowNumber());
    assertEquals(entryHash, row.inputHash());
    

    final int skipPtrCount = Ledger.skipCount(rowNumber);
    assertEquals(skipPtrCount, row.prevLevels());
    
    ByteBuffer expectedData = ByteBuffer.allocate((1 + skipPtrCount) * HASH_WIDTH);
    expectedData.put(entryHash.duplicate());
    
    for (int p = 0; p < skipPtrCount; ++p) {
      long referencedRowNum = rowNumber - (1 << p);
      ByteBuffer hashPtr = row.prevHash(p);
      ByteBuffer referencedRowHash = ledger.rowHash(referencedRowNum);
      assertEquals(referencedRowHash, hashPtr);
      expectedData.put(referencedRowHash);
    }
    
    assertFalse(expectedData.hasRemaining());
    expectedData.flip();
    
    
    MessageDigest digest = DIGEST.newDigest();
    digest.update(expectedData);
    
    ByteBuffer expectedRowHash = ByteBuffer.wrap(digest.digest());
    assertEquals(expectedRowHash, row.hash());
  }
  
  
  
  
  protected Ledger newLedger(Object methodLabel) throws Exception {
    Table table = newTable(methodLabel);
    return new CompactLedger(table);
  }
  
  
  protected abstract Table newTable(Object methodLabel) throws Exception;
  

}










