/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.sql.Connection;

import org.junit.jupiter.api.Assertions;

import io.crums.out_of_box_test.AbstractSkipLedgerTest;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipTable;

/**
 * 
 */
public class SqlSkipLedgerTest extends AbstractSkipLedgerTest {
  
  

  @Override
  protected SkipLedger newLedger(Object methodLabel) throws Exception {
    File dir = getMethodOutputFilepath(methodLabel);
    Connection con = SqlTestHarness.newDatabase(dir);
    return SqlSkipLedger.declareNewSkipLedger(con, SqlTestHarness.LEDGER_TABLENAME);
  }
  

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    Assertions.fail();
    return null;
  }

}
