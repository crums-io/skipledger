/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;


import java.util.Collections;
import java.util.List;

import io.crums.sldg.SkipLedger;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Lists;

/**
 * A bag or {@linkplain SourceRow}s.
 */
public interface SourceBag {
  
  
  /**
   * Returns the available row sources ordered by row number.
   * 
   * @return non-empty list
   */
  List<SourceRow> sources();
  
  
  /**
   * Returns the row source for the given row number.
   * 
   * @throws IllegalArgumentException if {@code rowNumber} is not found in {@linkplain #sourceRowNumbers()}
   */
  default SourceRow getSourceByRowNumber(long rowNumber) throws IllegalArgumentException {
    SkipLedger.checkRealRowNumber(rowNumber);
    int index = Collections.binarySearch(sourceRowNumbers(), rowNumber);
    if (index < 0)
      throw new IllegalArgumentException(rowNumber + " not found");
    return sources().get(index);
  }
  
  
  /**
   * Returns the row numbers of the {@linkplain #sources() sources}.
   * 
   * @return ordered list of source row numbers
   */
  default List<Long> sourceRowNumbers() {
    return Lists.map(sources(), s -> s.rowNumber());
  }
  

}
