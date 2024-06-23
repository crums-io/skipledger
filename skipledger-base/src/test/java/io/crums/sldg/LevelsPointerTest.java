/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.PathTest.newRandomLedger;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.crums.util.mrkl.Proof;


public class LevelsPointerTest {


  @Test
  public void test8() {
    var ledger = newRandomLedger(52);
    final long rn = 8L;
    Row row = ledger.getRow(rn);
    var ptr = row.levelsPointer();
    assertEquals(rn, ptr.rowNo());
    assertFalse(ptr.isCompressed());
    assertFalse(ptr.isCondensed());
    var cndPtr = ptr.compressToLevelRowNo(4L);
    assertTrue(cndPtr.isCompressed());
    assertTrue(cndPtr.isCondensed());
    assertEquals(ptr.rowHash(4L), cndPtr.rowHash(4L));
    assertEquals(2, cndPtr.level());
    assertEquals(ptr.rowHash(4L), cndPtr.levelHash());
    assertEquals(ptr.levelHash(2), ptr.rowHash(4L));



    var tree = SkipLedger.levelsMerkleTree(row.levelHashes());
    assertEquals(2, SkipLedger.levelLinked(rn, 4L));
    assertEquals(ByteBuffer.wrap(tree.hash()), ptr.hash());
    assertEquals(Arrays.asList(0L, 4L, 6L, 7L), ptr.coverage());
    assertEquals(SldgConstants.DIGEST.sentinelHash(), ptr.rowHash(0L));
    assertEquals(ByteBuffer.wrap(tree.data(0, 2)), ptr.levelHash(2));
    assertEquals(ptr.hash(), cndPtr.hash());
  }

}








