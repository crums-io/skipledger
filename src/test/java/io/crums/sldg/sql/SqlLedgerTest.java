/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.sql.Connection;

import org.junit.Assert;

import io.crums.sldg.AbstractLedgerTest;
import io.crums.sldg.Ledger;
import io.crums.sldg.Table;

/**
 * 
 */
public class SqlLedgerTest extends AbstractLedgerTest {
  
  

  @Override
  protected Ledger newLedger(Object methodLabel) throws Exception {
    File dir = getMethodOutputFilepath(methodLabel);
    Connection con = SqlTestHarness.newDatabase(dir);
    return SqlLedger.declareNewLedger(con, SqlTestHarness.LEDGER_TABLENAME);
  }
  

  @Override
  protected Table newTable(Object methodLabel) throws Exception {
    Assert.fail();
    return null;
  }

}
