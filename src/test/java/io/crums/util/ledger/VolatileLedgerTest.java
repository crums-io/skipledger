/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;

/**
 * 
 */
public class VolatileLedgerTest extends LedgerTestTemplate<VolatileLedger> {

  @Override
  protected VolatileLedger newLedger(int cellsCapacity, Object lablel) {
    ByteBuffer mem = ByteBuffer.allocate(cellsCapacity * HASH_WIDTH);
    return new VolatileLedger(mem);
  }
  

}




























