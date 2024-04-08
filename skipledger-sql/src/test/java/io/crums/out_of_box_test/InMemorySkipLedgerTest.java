/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import io.crums.sldg.SkipTable;
import io.crums.sldg.VolatileTable;

/**
 * 
 */
public class InMemorySkipLedgerTest extends AbstractSkipLedgerTest {
  
  

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    return new VolatileTable();
  }

}
