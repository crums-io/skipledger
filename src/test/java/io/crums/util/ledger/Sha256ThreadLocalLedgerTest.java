/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;

import java.nio.ByteBuffer;

/**
 * 
 */
public class Sha256ThreadLocalLedgerTest extends LedgerTestTemplate<SkipLedger> {

  @Override
  protected SkipLedger newLedger(int cellsCapacity, Object label) throws Exception {
    SkipLedger s = new VolatileLedger(ByteBuffer.allocate(cellsCapacity * HASH_WIDTH));
    return new Sha256ThreadLocalLedger(s);
  }

}
