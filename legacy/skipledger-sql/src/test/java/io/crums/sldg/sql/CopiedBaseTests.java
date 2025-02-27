/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg.sql;

import static io.crums.sldg.SldgConstants.DIGEST;
import static io.crums.sldg.SldgConstants.HASH_WIDTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.model.TrailedRecord;
import io.crums.out_of_box_test.MorselPackTest;
import io.crums.sldg.CompactSkipLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipTable;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.ledgers.HashLedger;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.testing.IoTestCase;
import io.crums.util.Lists;

/**
 * 
 */
public class CopiedBaseTests {

  
  
  static abstract class AbstractHashLedgerTest extends IoTestCase {
    
    
    public final static long MOCK_START_UTC = Crum.INCEPTION_UTC + 90_000_000;
    
    public final static String MOCK_SRC_NAME = "mock_account";
    
    @Test
    public void testEmpty() throws Exception {
      final Object label = new Object() {  };
      
      try (HashLedger hLedger = declareNewInstance(label)) {
        assertEquals(0, hLedger.getSkipLedger().size());
        assertEquals(0, hLedger.getTrailCount());
        assertEquals(0, hLedger.lastWitnessedRowNumber());
      }
    }
    
    
    @Test
    public void testOne() throws Exception {
      final Object label = new Object() {  };
      
      try (HashLedger hLedger = declareNewInstance(label)) {
        Random random = new Random(2);
        byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
        random.nextBytes(mockHash);
        assertEquals(1, hLedger.getSkipLedger().appendRows(ByteBuffer.wrap(mockHash)));
        assertEquals(1, hLedger.getSkipLedger().size());
        Row row = hLedger.getSkipLedger().getRow(1);
        assertEquals(ByteBuffer.wrap(mockHash), row.inputHash());
        CrumTrail trail = MorselPackTest.mockTrail(row.hash(), MOCK_START_UTC, 511, 1002);
        WitnessRecord trailedRecord = new WitnessRecord(row, new TrailedRecord(trail));
        hLedger.addTrail(trailedRecord);
        assertEquals(1, hLedger.getTrailCount());
        TrailedRow trailedRow = hLedger.getTrailByIndex(0);
        assertNotNull(trailedRow);
        assertEquals(trail, trailedRow.trail());
        assertEquals(trail.crum(), trailedRow.trail().crum());
        assertEquals(1, trailedRow.no());
        assertEquals(row.hash(), trailedRow.hash());
        
        trailedRow = hLedger.nearestTrail(1L);
        assertNotNull(trailedRow);
        assertEquals(trail, trailedRow.trail());
        assertEquals(trail.crum(), trailedRow.trail().crum());
        assertEquals(1, trailedRow.no());
        assertEquals(row.hash(), trailedRow.hash());
        
        assertEquals(1L, hLedger.lastWitnessedRowNumber());
        
      }
      
    }
    
    
    @Test
    public void testABunch() throws Exception {
      final Object label = new Object() {  };
      
      long[] trailedRns = { 5L, 29L, 30L, 257L, };
      final long count = 270;
      Random random = new Random(count);
      
      Random trailRand = new Random(count + 1);
      byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
      long utcDelta = 1_800_000;
      
      try (HashLedger hLedger = declareNewInstance(label)) {
        
        // setup
        
        long rn = 1;
        for (int ti = 0; ti < trailedRns.length; ++ti) {
          long nextTrailedRn = trailedRns[ti];
          for (; rn <= nextTrailedRn; ++rn) {
            random.nextBytes(mockHash);
            hLedger.getSkipLedger().appendRows(ByteBuffer.wrap(mockHash));
          }
          assertEquals(nextTrailedRn, rn - 1);
          Row row = hLedger.getSkipLedger().getRow(nextTrailedRn);
          
          int mrklCount = 2 + trailRand.nextInt(2046);
          int mrklIndex = trailRand.nextInt(mrklCount);
          long utc = MOCK_START_UTC + utcDelta * nextTrailedRn;
          
          CrumTrail trail = MorselPackTest.mockTrail(row.hash(), utc, mrklIndex, mrklCount);
          WitnessRecord trailedRecord = new WitnessRecord(row, new TrailedRecord(trail));
          hLedger.addTrail(trailedRecord);
        }
        for (; rn <= count; ++rn) {
          random.nextBytes(mockHash);
          hLedger.getSkipLedger().appendRows(ByteBuffer.wrap(mockHash));
        }
        
        // setup complete; verify the hashes
        
        assertEquals(count, hLedger.size());
        
        random.setSeed(count);
        for (rn = 1; rn <= count; ++rn) {
          Row row = hLedger.getSkipLedger().getRow(rn);
          random.nextBytes(mockHash);
          assertEquals(ByteBuffer.wrap(mockHash), row.inputHash());
        }
        
        // verify timekeeping
        
        assertEquals(trailedRns.length, hLedger.getTrailCount());
        
        for (int index = 0; index < trailedRns.length; ++index) {
          TrailedRow trailedRow = hLedger.getTrailByIndex(index);
          assertEquals(trailedRns[index], trailedRow.no());
          assertEquals(MOCK_START_UTC + utcDelta * trailedRns[index], trailedRow.utc());
          Row row = hLedger.getSkipLedger().getRow(trailedRns[index]);
          assertEquals(row.hash(), trailedRow.hash());
        }
        
        
        rn = 1;
        for (long trailedRn : trailedRns) {
          for (; rn <= trailedRn; ++rn) {
            TrailedRow trailedRow = hLedger.nearestTrail(rn);
            assertEquals(trailedRn, trailedRow.no());
          }
        }
        for (; rn <= count; ++rn) {
          TrailedRow trailedRow = hLedger.nearestTrail(rn);
          assertNull(trailedRow);
        }
      }
      
    }
    
    
    
    
    

    /**
     * Test methods invoke this method.
     */
    protected HashLedger declareNewInstance(Object methodLabel) throws Exception {
      File dir = getMethodOutputFilepath(methodLabel);
      return declareNewInstance(dir);
    }
    
    
    /**
     * Invoked by {@linkplain #declareNewInstance(Object)}.
     * 
     * @param testDir per-run, test directory path (doesn't yet exist)
     */
    protected abstract HashLedger declareNewInstance(File testDir) throws Exception;
  }
  
  
  
  
  
  
  
  static abstract class AbstractSkipLedgerTest extends IoTestCase {

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
        
        // ByteBuffer expectedData = ByteBuffer.allocate(2* HASH_WIDTH);
        // expectedData.put(mockHash).put(SENTINEL_HASH.duplicate()).flip();
        // assertEquals(expectedData, row.data());
        
        // ByteBuffer expectedHash;
        // {
        //   MessageDigest digest = DIGEST.newDigest();
        //   // this taxes the lazy-loading implementation
        //   digest.update(row.data());
        //   expectedHash = ByteBuffer.wrap(digest.digest());
        // }
        // assertEquals(expectedHash, row.hash());
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

      // Row row = ledger.getRow(rowNumber);
      
      // assertEquals(rowNumber, row.rowNumber());
      // assertEquals(entryHash, row.inputHash());
      

      // final int skipPtrCount = SkipLedger.skipCount(rowNumber);
      // assertEquals(skipPtrCount, row.prevLevels());
      
      // ByteBuffer expectedData = ByteBuffer.allocate((1 + skipPtrCount) * HASH_WIDTH);
      // expectedData.put(entryHash.duplicate());
      
      // for (int p = 0; p < skipPtrCount; ++p) {
      //   long referencedRowNum = rowNumber - (1 << p);
      //   ByteBuffer hashPtr = row.prevHash(p);
      //   ByteBuffer referencedRowHash = ledger.rowHash(referencedRowNum);
      //   assertEquals(referencedRowHash, hashPtr);
      //   expectedData.put(referencedRowHash);
      // }
      
      // assertFalse(expectedData.hasRemaining());
      // expectedData.flip();
      
      
      // MessageDigest digest = DIGEST.newDigest();
      // digest.update(expectedData);
      
      // ByteBuffer expectedRowHash = ByteBuffer.wrap(digest.digest());
      // assertEquals(expectedRowHash, row.hash());
    }
    
    
    
    
    
    
    protected SkipLedger newLedger(Object methodLabel) throws Exception {
      SkipTable table = newTable(methodLabel);
      return new CompactSkipLedger(table);
    }
    
    
    protected abstract SkipTable newTable(Object methodLabel) throws Exception;
    
    
  }
  
  



}
