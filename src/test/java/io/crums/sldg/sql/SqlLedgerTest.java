/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.sql.Connection;

import org.junit.Assert;

import io.crums.sldg.AbstractSkipLedgerTest;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipTable;

/**
 * 
 */
public class SqlLedgerTest extends AbstractSkipLedgerTest {
  
  

  @Override
  protected SkipLedger newLedger(Object methodLabel) throws Exception {
    File dir = getMethodOutputFilepath(methodLabel);
    Connection con = SqlTestHarness.newDatabase(dir);
    return SqlSkipLedger.declareNewLedger(con, SqlTestHarness.LEDGER_TABLENAME);
  }
  

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    Assert.fail();
    return null;
  }

}
