/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.sql.Connection;

import io.crums.sldg.AbstractHashLedgerTest;
import io.crums.sldg.HashLedger;

/**
 * 
 */
public class SqlHashLedgerTest extends AbstractHashLedgerTest {
  
  
  
  
  
  @Override
  protected HashLedger declareNewInstance(File testDir) throws Exception {
    Connection con = SqlTestHarness.newDatabase(testDir);
    return SqlHashLedger.declareNewInstance(con, MOCK_SRC_NAME);
  }
}
