/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.File;

import org.junit.Assert;

import io.crums.sldg.db.CompactFileLedger;

/**
 * 
 */
public class CompactFileLedgerTest extends AbstractLedgerTest {

  @Override
  protected Ledger newLedger(Object methodLabel) throws Exception {
    File file = getMethodOutputFilepath(methodLabel);
    return new CompactFileLedger(file);
  }
  
  @Override
  protected Table newTable(Object methodLabel) throws Exception {
    Assert.fail();
    return null;
  }

}
