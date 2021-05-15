/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;

/**
 * Storage interface for skip ledger. Note the row indexing here, unlike
 * {@linkplain Ledger}, is 0-based.
 * 
 * @see SldgConstants#DIGEST
 */
public interface SkipTable extends AutoCloseable {
  
  
  /**
   * The row width of the table is twice the digest hash width. A row
   * contains 2 hashes: one, the hash of the input entry; two, the (precomputed)
   * hash of the entire (abstract) row (the input entry and all the row's
   * hash pointers).
   */
  public final static int ROW_WIDTH = SldgConstants.DIGEST.hashWidth() * 2;
  
  /**
   * Appends one or more rows.
   * 
   * @param rows with non-zero multiple of {@linkplain #ROW_WIDTH} remaining bytes
   * @param index the <em>expected</em> first row index (or the current size).
   * 
   * @return the new {@linkplain #size()} of the base table
   */
  long addRows(ByteBuffer rows, long index);
  
  /**
   * Returns a previously appended row at the given index. Indexes, unlike
   * row numbers are 0-based. The implementation checks the argument is
   * in the valid range.
   * 
   * @param index &ge; 0 and &lt; {@linkplain #size()}
   * 
   * @return non-null buffer with {@linkplain #ROW_WIDTH} remaining bytes
   */
  ByteBuffer readRow(long index);
  
  
  /**
   * Returns the number of rows thus far appended.
   * 
   * @return &ge; 0
   * 
   * @see #trimSize(long)
   */
  long size();
  
  
  /**
   * Determines if the instance is empty.
   * 
   * @return <tt>true</tt> iff {@linkplain #size()} == 0
   */
  default boolean isEmpty() {
    return size() == 0;
  }
  
  
  /**
   * Closes the instance.
   */
  default void close() {  }
  
  
  /**
   * Trims the table to the given {@code newSize}. Optional operation.
   * 
   * @param newSize &ge; 1 and &le; {@linkplain #size() size}
   * 
   * @throws UnsupportedOperationException if the implementation does not support it.
   */
  default void trimSize(long newSize) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

}
