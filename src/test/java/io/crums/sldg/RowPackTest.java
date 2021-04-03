/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;

import io.crums.sldg.bags.RowBag;
import io.crums.sldg.packs.RowPackBuilder;

/**
 *
 */
public class RowPackTest extends RowBagTest {

  @Override
  protected RowBag newBag(Ledger ledger, List<Long> rowNumbers) {
    return RowPackBuilder.createBag(ledger, rowNumbers);
  }

}
