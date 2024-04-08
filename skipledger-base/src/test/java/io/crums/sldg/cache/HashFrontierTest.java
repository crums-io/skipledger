/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipLedgers;
import io.crums.sldg.SldgConstants;
import io.crums.testing.SelfAwareTestCase;

/**
 * 
 */
public class HashFrontierTest extends SelfAwareTestCase {
  
  
  @Test
  public void testOne() {
    test(1);
  }
  
  
  @Test
  public void testTwo() {
    test(2);
  }
  
  @Test
  public void testThreeThru33() {
    for (int rows = 3; rows <= 33; ++rows)
      test(rows);
  }
  
  
  @Test
  public void testMillion() {
    test(1_000_000, -1L, new Object() { });
  }
  
  @Test
  public void testMiddleThree() {
    testFromMiddle(3, -1L, 2, 10L);
  }
  
  @Test
  public void testMiddleMillion() {
    testFromMiddle(1_000_000, -1L, 500_000, 10L);
  }
  
  
  private void test(int rows) {
    test(rows, rows, null);
  }
  
  private void test(int rows, long seed, Object label) {
    
    Random random = new Random(seed);
    
    byte[] hash = new byte[SldgConstants.HASH_WIDTH];
    final ByteBuffer inputHash = ByteBuffer.wrap(hash); // fixed view on to *hash*
    
    
    long now = System.nanoTime();
    
    // construct the expected last row hash using a skipledger
    // (assume the skipledger works.. the benchmark we're testing against)
    
    final SkipLedger expected = SkipLedgers.inMemoryInstance();
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(hash); // (indirectly modifies inputHash)
      expected.appendRows(inputHash.clear());
    }
    
    long skipLap = System.nanoTime() - now;
    
    assertEquals(rows, expected.size());
    
    // reset the for the frontier round
    random.setSeed(seed);
    
    var digest = SldgConstants.DIGEST.newDigest();
    
    now = System.nanoTime();
    
    random.nextBytes(hash);
    HashFrontier frontier = HashFrontier.firstRow(inputHash.clear(), digest);
    for (int count = rows - 1; count-- > 0; ) {
      random.nextBytes(hash); // (indirectly modifies inputHash)
      frontier = frontier.nextFrontier(inputHash.clear(), digest);
    }
    
    long frontLap = System.nanoTime() - now;
    
    assertEquals(rows, frontier.rowNumber());
    var hiRow = frontier.frontierRow();
    assertEquals(expected.rowHash(rows), hiRow.hash());
    assertEquals(rows, hiRow.rowNumber());
    
    if (label != null) {
      String method = method(label);
      var numform = new DecimalFormat("#,###.##");
      double skipMillis = skipLap * 1.0e-6;
      double frontierMillis = frontLap * 1.0e-6;
      System.out.println();
      System.out.println("[" + method + "]: ");
      System.out.println("   " + numform.format(rows) + " randoms rows");
      System.out.println("   " + numform.format(skipMillis) + " millis to generate skipledger (in memory)");
      System.out.println("   " + numform.format(frontierMillis) + " millis to generate frontier");
      System.out.println();
    }
  }
  
  
  
  private void testFromMiddle(int rows, long seed, int midRn, long midSeed) {
    
    Random random = new Random(seed);
    
    byte[] hash = new byte[SldgConstants.HASH_WIDTH];
    final ByteBuffer inputHash = ByteBuffer.wrap(hash); // fixed view on to *hash*

    // construct the expected last row hash using a skipledger
    // (assume the skipledger works.. the benchmark we're testing against)
    
    final SkipLedger expected = SkipLedgers.inMemoryInstance();
    
    // populate rows 1 thru (midRn - 1)
    for (int count = midRn - 1; count-- > 0; ) {
      random.nextBytes(hash); // (indirectly modifies inputHash)
      expected.appendRows(inputHash.clear());
    }
    
    // reset the seed for row [midRn]
    random.setSeed(midSeed);
    
    // (respective for-loop initializations sum to *rows*)
    for (int count = rows - midRn + 1; count-- > 0; ) {
      random.nextBytes(hash);
      expected.appendRows(inputHash.clear());
    }
    
    // load the frontier for row [midRn - 1]
    HashFrontier frontier = HashFrontier.loadFrontier(expected, midRn - 1);
    
    // reset the seed for row [midRn] and generate the frontier
    // up to row [rows]..
    random.setSeed(midSeed);
    
    for (int count = rows - midRn + 1; count-- > 0; ) {
      random.nextBytes(hash); // (indirectly modifies inputHash)
      frontier = frontier.nextFrontier(inputHash.clear());
    }

    assertEquals(rows, frontier.rowNumber());
    var hiRow = frontier.frontierRow();
    assertEquals(expected.rowHash(rows), hiRow.hash());
    assertEquals(rows, hiRow.rowNumber());
  }
  

}






