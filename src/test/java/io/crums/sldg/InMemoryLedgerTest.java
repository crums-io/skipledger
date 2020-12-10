/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import io.crums.sldg.db.VolatileTable;

/**
 * Ledger with {@linkplain VolatileTable} test.
 */
public class InMemoryLedgerTest extends AbstractLedgerTest {
  
  

  @Override
  protected Table newTable(Object methodLabel) throws Exception {
    return new VolatileTable();
  }

}
