/*
 * Copyright 2021-2025 Babak Farhang
 */
package io.crums.sldg.src;


import static java.util.Collections.binarySearch;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.crums.util.Lists;

/**
 * A bag or {@linkplain SourceRow}s.
 */
public interface SourceBag {

  

  /**
   * Returns the available source rows, ordered by row number.
   */
  List<? extends SourceRow> sources();
  
  /**
   * Returns the ordered list of row numbers.
   */
  default List<Long> sourceNos() {
    return Lists.map(sources(), SourceRow::no);
  }
  
  
  default SourceRow sourceByNo(long rowNo) throws NoSuchElementException {
    int index = binarySearch(sourceNos(), rowNo);
    if (index < 0)
      throw new NoSuchElementException("rowNo " + rowNo);
    return sources().get(index);
  }
  
  
  default Optional<SourceRow> findSourceByNo(long rowNo) {
    int index = binarySearch(sourceNos(), rowNo);
    return index < 0 ? Optional.empty() : Optional.of(sources().get(index));
  }
  
  
  default boolean containsSource(long rowNo) {
    return binarySearch(sourceNos(), rowNo) >= 0;
  }
  
  
  
  
  
}













