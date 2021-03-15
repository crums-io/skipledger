/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;


import io.crums.sldg.db.CachingRowBag;
import io.crums.sldg.db.SortedBag;
import io.crums.sldg.db.SortedBagBuilder;

/**
 * Also tests {@linkplain Row#hashCode()} under the covers.
 */
public class CachingRowBagTest extends RowBagTest {
  

  @Override
  protected RowBag newBag(Ledger ledger, List<Long> rowNumbers) {
    SortedBag bag = new SortedBagBuilder().createBag(ledger, rowNumbers);
    return new CachingRowBag(bag);
  }

}
