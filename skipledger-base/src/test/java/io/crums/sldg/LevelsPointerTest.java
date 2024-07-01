/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.PathTest.newRandomLedger;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;



public class LevelsPointerTest {


  @Test
  public void test8() {
    var ledger = newRandomLedger(52);
    final long rn = 8L;
    Row row = ledger.getRow(rn);
    var ptr = row.levelsPointer();
    assertEquals(rn, ptr.rowNo());
    assertTrue(ptr.isCompressed());
    assertFalse(ptr.isCondensed());
  }




  @Test
  public void test16() {
    var ledger = newRandomLedger(52);
    final long rn = 16L;
    Row row = ledger.getRow(rn);
    var ptr = row.levelsPointer();
    assertEquals(rn, ptr.rowNo());
    assertFalse(ptr.isCompressed());
    assertFalse(ptr.isCondensed());

    var cndPtr = ptr.compressToLevelRowNo(8L);
    assertTrue(cndPtr.isCompressed());
    assertTrue(cndPtr.isCondensed());
    assertEquals(ptr.rowHash(8L), cndPtr.rowHash(8L));
    assertEquals(3, cndPtr.level());
    assertEquals(ptr.rowHash(8L), cndPtr.levelHash());
    assertEquals(ptr.levelHash(3), ptr.rowHash(8L));



    var tree = SkipLedger.levelsMerkleTree(row.levelHashes());
    assertEquals(3, SkipLedger.levelLinked(rn, 8L));
    assertEquals(ByteBuffer.wrap(tree.hash()), ptr.hash());
    assertEquals(Arrays.asList(0L, 8L, 12L, 14L, 15L), ptr.coverage());
    assertEquals(SldgConstants.DIGEST.sentinelHash(), ptr.rowHash(0L));
    assertEquals(ByteBuffer.wrap(tree.data(0, 3)), ptr.levelHash(3));
    assertEquals(ptr.hash(), cndPtr.hash());
  }

}








