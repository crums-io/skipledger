/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;

import java.util.Collections;
import java.util.List;

import io.crums.model.CrumTrail;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.Lists;

/**
 * A bag of {@linkplain CrumTrail}s paired with ledger rows.
 */
public interface TrailBag {
  
  
  
  /**
   * Returns the rows with annotated crumtrails.
   * 
   * @return non-null list of positive, strictly ascending row numbers
   */
  List<Long> trailedRowNumbers();
  
  
  /**
   * Returns the crumtrail for the given {@code rowNumber}. The given
   * row number is one of the {@linkplain #trailedRowNumbers()}.
   */
  CrumTrail crumTrail(long rowNumber);
  
  
  
  /**
   * Returns the trailed rows. The default implementation is a composition
   * of {@linkplain #trailedRowNumbers()} and {@linkplain #crumTrail(long)}.
   * 
   * @return list of {@linkplain TrailedRow}s in ascending row numbers. May be
   *         empty
   */
  default List<TrailedRow> getTrailedRows() {
    return Lists.map(trailedRowNumbers(), rn -> new TrailedRow(rn, crumTrail(rn)));
  }
  
  
  default int indexOfNearestTrail(long rowNumber) {
    var trailedRns = trailedRowNumbers();
    if (trailedRns.isEmpty())
      return -1;
    
    int searchIndex = Collections.binarySearch(trailedRns, rowNumber);
    if (searchIndex >= 0)
      return searchIndex;
    
    int insertIndex = -1 - searchIndex;
    return insertIndex == trailedRns.size() ? -1 : insertIndex;
  }

}
