/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.util.List;

import io.crums.sldg.bags.RowBag;
import io.crums.sldg.packs.CachingRowPack;
import io.crums.sldg.packs.RowPack;
import io.crums.sldg.packs.RowPackBuilder;

/**
 * Also tests {@linkplain Row#hashCode()} under the covers.
 */
public class CachingRowPackTest extends RowBagTest {
  

  @Override
  protected RowBag newBag(SkipLedger ledger, List<Long> rowNumbers) {
    RowPack bag = RowPackBuilder.createBag(ledger, rowNumbers);
    return new CachingRowPack(bag);
  }

}
