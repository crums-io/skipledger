/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import java.util.List;

import io.crums.sldg.RowBag;
import io.crums.sldg.SkipLedger;
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
