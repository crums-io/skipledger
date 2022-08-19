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
   * @return non-null, possibly empty list of positive, strictly ascending row numbers
   */
  List<Long> trailedRowNumbers();
  
  
  /**
   * Returns the {@linkplain #trailedRowNumbers() trailed row numbers} starting from
   * the given row number.
   * 
   * @return non-null, possibly empty list of positive, strictly ascending row numbers
   */
  default List<Long> trailedRowNumbers(long fromRn) {
    var trailedRns = trailedRowNumbers();
    if (trailedRns.isEmpty())
      return List.of();
    int searchIndex = Collections.binarySearch(trailedRns, fromRn);
    if (searchIndex >= 0)
      return trailedRns.subList(searchIndex, trailedRns.size());
    int insertIndex = -1 - searchIndex;
    return insertIndex == trailedRns.size() ?
        List.of() :
          trailedRns.subList(insertIndex, trailedRns.size());
  }
  
  
  /**
   * Returns the {@linkplain #trailedRowNumbers() trailed row numbers} evidencing the
   * minimum ages of the rows in the given range.
   * 
   * @param fromRn 
   * @param toRn
   * @return
   */
  default List<Long> trailedRowNumbers(long fromRn, long toRn) {
    if (fromRn < 1 || toRn < fromRn)
      throw new IllegalArgumentException("fromRn: %d, toRn: %d".formatted(fromRn, toRn));
    var trailedRns = trailedRowNumbers();
    if (trailedRns.isEmpty())
      return List.of();
    int headIndex = Collections.binarySearch(trailedRns, fromRn);
    if (headIndex < 0) {
      headIndex = -1 - headIndex;
      if (headIndex == trailedRns.size())
        return List.of();
    }

    // (exclusive)..
    int tailIndex = Collections.binarySearch(trailedRns, toRn);
    if (tailIndex < 0) {
      tailIndex = -1 - tailIndex;
    } else
      tailIndex++;
    
    return trailedRns.subList(headIndex, tailIndex);
  }
  
  
  /**
   * Returns the crumtrail for the given {@code rowNumber}. The given
   * row number is one of the {@linkplain #trailedRowNumbers()}.
   */
  CrumTrail crumTrail(long rowNumber) throws IllegalArgumentException;
  
  
  /**
   * Returns the trailed row at the given {@code rowNumber}.
   * 
   * @param rowNumber one of {@linkplain #trailedRowNumbers()}
   * 
   * @return not null
   */
  default TrailedRow getTrailedRow(long rowNumber) throws IllegalArgumentException {
    return new TrailedRow(rowNumber, crumTrail(rowNumber));
  }
  
  /**
   * Returns the trailed rows. The default implementation is a composition
   * of {@linkplain #trailedRowNumbers()} and {@linkplain #crumTrail(long)}.
   * 
   * @return list of {@linkplain TrailedRow}s in ascending row numbers. May be
   *         empty
   */
  default List<TrailedRow> getTrailedRows() {
    return Lists.map(trailedRowNumbers(), rn -> getTrailedRow(rn));
  }
  
  
  /**
   * Returns the index of the first crumtrail (in the list returned by
   * {@linkplain #getTrailedRows()} that establishes minimum age of the row with
   * the given number, or -1 if no such crumtrail exists.
   */
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
  
  
  /**
   * Returns a list of trailed rows starting from the given row number.
   * 
   * @return possibly empty list of ordered trailed rows
   */
  default List<TrailedRow> getTrailRows(long fromRn) {
    
    return Lists.map(
        trailedRowNumbers(fromRn),
        rn -> getTrailedRow(rn));
  }

}

















