/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.PathTest.newRandomLedger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class PathPackTest extends RowBagTest {

  @Override
  protected PathPack newBag(SkipLedger ledger, List<Long> rowNumbers) {
    
    // note: don't use SkipLedger.getPath(..)
    // The SkipLedger class itself might use
    // PathPack instances to return paths. Instead,
    // construct the rows, one at a time.
    
    List<Row> rows = ledger.getRows(rowNumbers);
    Path path = new Path(rows);
    
    return PathPack.forPath(path);
    
  }



  @Test
  public void testSerialSmallest() {
    final int size = 1;
    SkipLedger ledger = newRandomLedger(size);
    Path state = ledger.statePath();
    PathPack pack = PathPack.forPath(state);
    var bytes = pack.serialize();
    PathPack rt = PathPack.load(bytes);
    assertEquals(state, rt.path());
  }




  @Test
  public void testCompressed() {

    var ledger = newRandomLedger(1020);
    Path path = ledger.statePath();
    assertFalse(path.isCompressed());
    assertFalse(path.isCondensed());
    Path cpath = path.compress();
    assertEquals(path, cpath);
    assertTrue(cpath.isCompressed());
    assertTrue(cpath.isCondensed());
    PathPack pack = cpath.pack();
    assertEquals(path, pack.path());
  }


  


}
