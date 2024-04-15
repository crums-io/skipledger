/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.File;
import java.sql.Connection;

import io.crums.sldg.ledgers.HashLedger;

/**
 * 
 */
public class SqlHashLedgerTest extends CopiedBaseTests.AbstractHashLedgerTest {
  
  
  
  
  
  @Override
  protected HashLedger declareNewInstance(File testDir) throws Exception {
    Connection con = SqlTestHarness.newDatabase(testDir);
    
    var schema = SqlTestHarness.newSchema(MOCK_SRC_NAME);
    return SqlHashLedger.declareNewInstance(con, schema);
  }
}
