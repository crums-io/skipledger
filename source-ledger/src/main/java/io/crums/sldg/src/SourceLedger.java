/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


/**
 * A read-only view of the ledger source-rows indexed by row no.
 * In many, if not most, use cases this is an append-only table
 * (or view) from a relational database.
 * 
 * @see #size()
 * @see #getSourceRow(long)
 * @see SourceRow
 */
public interface SourceLedger {
  
  
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
   *
   * @see #isSalted()
   */
  SourceRow getSourceRow(long rn) throws IndexOutOfBoundsException;


  /**
   * Determines whether source rows are salted. Either every cell in a row
   * is salted, or none are.
   */
  boolean isSalted();


}
