/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import java.util.List;

import io.crums.sldg.RowBag;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.packs.RowPackBuilder;

/**
 *
 */
public class RowPackTest extends RowBagTest {

  @Override
  protected RowBag newBag(SkipLedger ledger, List<Long> rowNumbers) {
    return RowPackBuilder.createBag(ledger, rowNumbers);
  }

}
