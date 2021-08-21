/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import io.crums.sldg.src.SourceRow;

/**
 * A read-only view of the source-rows, each hash of which defines its input-hash in the skip
 * ledger. In many, if not most, use cases this is an append-only table
 * (or view) in a relational database.
 * 
 * @see #size()
 * @see #getSourceRow(long)
 */
public interface SourceLedger extends AutoCloseable {
  
  
  /**
   * <p>Does not throw checked exceptions.</p>
   * 
   * {@inheritDoc}
   */
  void close();
  
  
  /**
   * Returns the number rows in the source table. The rows themselves are
   * numbered 1 thru the return value (inclusive). I.e. row numbers are both ascending
   * and <em>contain no gaps (!)</em>.
   * 
   * @return &ge; 0
   */
  long size();
  
  
  /**
   * Returns the source-row at the given 1-based row-number.
   * 
   * @param rn &ge; 1, &le; {@linkplain #size()}
   * 
   * @return non-null
   */
  SourceRow getSourceRow(long rn);
  
  
  

}
