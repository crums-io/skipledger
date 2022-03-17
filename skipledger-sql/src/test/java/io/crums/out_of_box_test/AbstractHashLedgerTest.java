/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import  org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.model.TrailedRecord;
import io.crums.sldg.HashLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;


/**
 * 
 */
public abstract class AbstractHashLedgerTest extends IoTestCase {
  
  
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
      assertEquals(1, trailedRow.rowNumber());
      assertEquals(row.hash(), trailedRow.hash());
      
      trailedRow = hLedger.nearestTrail(1L);
      assertNotNull(trailedRow);
      assertEquals(trail, trailedRow.trail());
      assertEquals(trail.crum(), trailedRow.trail().crum());
      assertEquals(1, trailedRow.rowNumber());
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
        assertEquals(trailedRns[index], trailedRow.rowNumber());
        assertEquals(MOCK_START_UTC + utcDelta * trailedRns[index], trailedRow.utc());
        Row row = hLedger.getSkipLedger().getRow(trailedRns[index]);
        assertEquals(row.hash(), trailedRow.hash());
      }
      
      
      rn = 1;
      for (long trailedRn : trailedRns) {
        for (; rn <= trailedRn; ++rn) {
          TrailedRow trailedRow = hLedger.nearestTrail(rn);
          assertEquals(trailedRn, trailedRow.rowNumber());
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

