/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;

import io.crums.sldg.db.SortedBagBuilder;

/**
 *
 */
public class SortedBagTest extends RowBagTest {

  @Override
  protected RowBag newBag(Ledger ledger, List<Long> rowNumbers) {
    return new SortedBagBuilder().createBag(ledger, rowNumbers);
  }

}
