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
 * A bag of {@linkplain SourceRow}s.
 */
public interface SourceBag {

  

  /**
   * Returns the available source rows, ordered by row number.
   */
  List<? extends SourceRow> sources();
  
  
  /**
   * Returns the salt scheme {@linkplain SourceRow}s used to
   * compute each source-row's hash.
   */
  SaltScheme saltScheme();
  
  /**
   * Returns the ordered list of row numbers.
   */
  default List<Long> sourceNos() {
    return Lists.map(sources(), SourceRow::no);
  }
  
  
  /**
   * Retruns the source row with the given no.
   * 
   * @see #findSourceByNo(long)
   */
  default SourceRow sourceByNo(long rowNo) throws NoSuchElementException {
    int index = binarySearch(sourceNos(), rowNo);
    if (index < 0)
      throw new NoSuchElementException("rowNo " + rowNo);
    return sources().get(index);
  }
  
  
  /**
   * Searches and returns the source row with the given number, if any.
   */
  default Optional<SourceRow> findSourceByNo(long rowNo) {
    int index = binarySearch(sourceNos(), rowNo);
    return index < 0 ? Optional.empty() : Optional.of(sources().get(index));
  }
  
  
  /**
   * Returns {@code true} if this bag contains the source row
   * with the given no.
   */
  default boolean containsSource(long rowNo) {
    return binarySearch(sourceNos(), rowNo) >= 0;
  }
  
  
  
  
  
}













