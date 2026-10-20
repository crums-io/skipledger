/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import io.crums.util.collections.Lists;

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
   * Returns a list of source-rows. The default implementation just calls {@linkplain #getSourceRow(long)}
   * in sequence. An implementation may override to expose a faster way to retrieve. For e.g., if it's
   * backed in a database, a single query may be more efficient.
   * 
   * @param rowNos  not empty, strictly ascending row numbers &ge; 1, and &le; {@linkplain #size()}. In
   *                practice, an ordered list, or set
   * 
   * @return the argument list mapped (not lazily) to source-rows
   */
  default List<SourceRow> getSourceRows(Collection<Long> rowNos) {
    long last = 0L;
    SourceRow[] rows = new SourceRow[rowNos.size()];
    int index = 0;

    for (Iterator<Long> i = rowNos.iterator(); index < rows.length; index++) {
      long rn = i.next();
      if (rn <= last) {
        String msg = rn <= 0 ?
            "illegal row number (%d) in args: %s".formatted(rn, rowNos) :
            (
              rn == last ?
                "duplicate row numbers (%d) in args: %".formatted(rn, rowNos) :
                "out-of-sequence row numbers (%d, %d) in args".formatted(last, rn, rowNos)
            );
        throw new IllegalArgumentException(msg);
      }

      rows[index] = getSourceRow(rn);
      last = rn;
    }

    if (index == 0)
      throw new IllegalArgumentException("empty argument");

    assert index == rowNos.size();
    return Lists.asReadOnlyList(rows);
  }


  /**
   * Determines whether source rows are salted. Either every cell in a row
   * is salted, or none are.
   */
  boolean isSalted();


}
