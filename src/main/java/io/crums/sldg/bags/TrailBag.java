/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;

import java.util.List;

import io.crums.model.CrumTrail;

/**
 * A bag of {@linkplain CrumTrail}s paired with ledger rows.
 */
public interface TrailBag {
  
  
  
  /**
   * Returns the rows with annotated crumtrails.
   * 
   * @return non-null list of positive, strictly ascending row numbers
   */
  List<Long> trailedRows();
  
  
  /**
   * Returns the crumtrail for the given {@code rowNumber}. The given
   * row number is one of the {@linkplain #trailedRows()}.
   */
  CrumTrail crumTrail(long rowNumber);

}
