/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.CompactSkipLedger;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.VolatileTable;
import io.crums.sldg.bindle.MultiPath;
import io.crums.sldg.bindle.MultiPathBuilder;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.Lists;

/**
 * 
 */
public class MultiPathTest extends SelfAwareTestCase {
  
  
  @Test
  public void testSmallest() {
    final var label = new Object() { };
    var ledger = newMockLedger(1, label);
    Path path = ledger.statePath();
    var mp = new MultiPath(List.of(path));
    assertFalse(mp.hasRow(0L));
    assertTrue(mp.coversRow(0L));
    assertTrue(mp.hasRow(1L));
    assertTrue(mp.coversRow(1L));
    assertFalse(mp.hasRow(2L));
    assertFalse(mp.coversRow(2L));
    assertEquals(List.of(path), mp.paths());
  }
  

  @Test
  public void testSmallestInvalidDuplicate() {
    final var label = new Object() { };
    var ledger = newMockLedger(1, label);
    Path path = ledger.statePath();
    try {
      new MultiPath(List.of(path, path));
      fail();
    } catch (IllegalArgumentException expected) {    }
  }
  
  

  @Test
  public void testSmallestTwo() {

    final var label = new Object() { };
    var ledger = newMockLedger(2, label);
    Path p1 = ledger.getPath(1L);
    Path p2 = ledger.getPath(1L, 2L);
    
    var mp = new MultiPath(List.of(p1, p2));
    
    assertFalse(mp.hasRow(0L));
    assertTrue(mp.coversRow(0L));
    assertTrue(mp.hasRow(1L));
    assertTrue(mp.coversRow(1L));
    assertTrue(mp.hasRow(2L));
    assertTrue(mp.coversRow(2L));
    assertFalse(mp.hasRow(3L));
    assertFalse(mp.coversRow(3L));
    assertEquals(List.of(p2, p1), mp.paths());
    
  }
  
  
  @Test
  public void testAFew() {

    final var label = new Object() { };
    final int rowCount = 15_000;
    
    var ledger = newMockLedger(rowCount, label);
    
    Integer[] pathOrdinals = {
        1, rowCount,    // state
//        5, 11,          // intersects 8
        57, 61,
        129, rowCount - 11,
        
    };
    Path testPath = getPaths(ledger, 5, 11).get(0);
    
    
    var paths = getPaths(ledger, pathOrdinals);
    var mp = new MultiPath(paths);
    
    assertEquals(8L, mp.highestCommonNo(testPath));
  }
  
  @Test
  public void testAFew2() {

    final var label = new Object() { };
    final int rowCount = 15_000;
    
    var ledger = newMockLedger(rowCount, label);
    
    Integer[] pathOrdinals = {
//        5, 11,          // intersects 8
//        57, 61,
        129, rowCount - 11,
        1, rowCount,    // state
        
    };
    
    var paths = getPaths(ledger, pathOrdinals);
    var mp = new MultiPath(paths);
    
    var testPaths = getPaths(ledger, 5, 11, 57, 61);

    assertEquals(8L, mp.highestCommonNo(testPaths.get(0)));
    assertEquals(60L, mp.highestCommonNo(testPaths.get(1)));
  }
  
  @Test
  public void testIllegalFew() {

    final var label = new Object() { };
    final int rowCount = 15_000;
    
    var ledger = newMockLedger(rowCount, label);
    
    Integer[] pathOrdinals = {
        5, 11,          // intersects 8
        57, 61,
        127, rowCount - 11,
        1, rowCount,    // state
        133, 134        // illegal island
    };
    
    var paths = getPaths(ledger, pathOrdinals);
    
    try {
      new MultiPath(paths);
      fail();
    } catch (IllegalArgumentException expected) {
      System.out.println(method(label) + ":");
      System.out.println("[EXPECTED]: " + expected);
    }

  }
  
  
  @Test
  public void testAFewWithBuilder() {

    final var label = new Object() { };
    final int rowCount = 15_000;
    
    var ledger = newMockLedger(rowCount, label);
    
    Integer[] pathOrdinals = {
        1, rowCount,    // state
//        5, 11,          // intersects 8
        57, 61,
        129, rowCount - 11,
        
    };
    
    var paths = getPaths(ledger, pathOrdinals);
    
    var builder = new MultiPathBuilder(paths.getFirst());
    
    for (var path : paths.subList(1, paths.size()))
      builder.addPath(path);
    
    var mp = builder.toMultiPath();
    
    var testPath = getPaths(ledger, 5, 11).get(0);
    
    assertEquals(8L, mp.highestCommonNo(testPath));
    
    
  }
  
  
  @Test
  public void testIllegalFewWithBuilder() {

    final var label = new Object() { };
    final int rowCount = 15_000;
    
    var ledger = newMockLedger(rowCount, label);
    
    var builder = new MultiPathBuilder(ledger.getPath(5L, 11L));
    
    assertAddFails(builder, ledger.getPath(1L, 3L));
    builder.addPath(ledger.getPath(10L, 14L));
    builder.addPath(ledger.getPath(16L, 32L));
    
    fromChecked(builder);
    
    assertAddFails(builder, ledger.getPath(27L));
    
    builder.addPath(ledger.statePath());

    assertAddFails(builder, ledger.getPath(27L));
    
    fromChecked(builder);
    
  }
  
  
  private void assertAddFails(MultiPathBuilder builder, Path path) {
    try {
      builder.addPath(path);
      fail();
    } catch (IllegalArgumentException expected) {  }
  }
  
  
  static MultiPath fromChecked(MultiPathBuilder builder) {
    return new MultiPath(Lists.asReadOnlyList(builder.paths()));
  }
  
  
  
  private List<Path> getPaths(SkipLedger ledger, Integer... coordinates) {
    final int count = coordinates.length;
    assertEquals(0, count % 2);
    List<Path> paths = new ArrayList<>();
    for (int i = 0; i < count; i += 2)
      paths.add(ledger.getPath(
          coordinates[i].longValue(), coordinates[i + 1].longValue()));
    return paths;
  }
  
  
  
  private SkipLedger newMockLedger(int rows, long seed) {
    Random random = new Random(seed);    

    SkipLedger ledger = new CompactSkipLedger(new VolatileTable());
    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      ledger.appendRows(mockHashBuffer);
    }
    
    return ledger;
  }
  
  private SkipLedger newMockLedger(int rows, Object methodLabel) {
    return newMockLedger(rows, method(methodLabel).hashCode());
  }

}











