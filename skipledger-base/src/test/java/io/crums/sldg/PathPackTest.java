/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import java.util.List;

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

}
