/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import io.crums.sldg.mem.VolatileTable;

/**
 * 
 */
public class InMemorySkipLedgerTest extends AbstractSkipLedgerTest {
  
  

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    return new VolatileTable();
  }

}
