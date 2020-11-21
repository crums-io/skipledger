/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.io.File;

/**
 * 
 */
public class DirectLedgerTest extends LedgerTestTemplate<DirectLedger> {

  @Override
  protected DirectLedger newLedger(int cellsCapacity, Object label) throws Exception {
    File file = getMethodOutputFilepath(label);
    return new DirectLedger(file);
  }

  @Override
  protected void close(DirectLedger ledger) throws Exception {
    ledger.close();
  }

}
