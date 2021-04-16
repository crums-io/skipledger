/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import io.crums.sldg.BaggedRow;
import io.crums.sldg.Row;
import io.crums.sldg.SldgConstants;
import io.crums.util.hash.Digest;

/**
 * <p>A bag of rows. The general contract is that if a row's
 * {@linkplain #inputHash(long) input hash} is known, then
 * the {@linkplain #rowHash(long) row hash}es the row links
 * to are also known.
 * </p><p>
 * <em>Return values are read-only.</em> Excepting buffers (which carry positional state),
 * this means return values are immutable.
 * </p>
 */
public interface RowBag extends Digest {
  
  
  /**
   * Returns the {@linkplain Row#hash() row-hash} for the row at the given number.
   * The eligible input arguments are both from the {@linkplain #getFullRowNumbers() full
   * row numbers} and also the row numbers of rows they reference.
   * 
   * @param rowNumber a member of {@code Ledger.coverage(getFullRowNumbers())}
   *                  (note this includes row 0, where the sentinel hash must be returned)
   * 
   * @return 32-byte hash
   */
  ByteBuffer rowHash(long rowNumber);
  
  
  /**
   * Returns the {@linkplain Row#inputHash() input hash} for the row at the given
   * number. The eligible input arguments are those by {@linkplain #getFullRowNumbers()}.
   * 
   * @param rowNumber one of the {@linkplain #getFullRowNumbers() full-row-numbers}
   * 
   * @return 32-byte hash
   */
  ByteBuffer inputHash(long rowNumber);
  
  /**
   * Returns the row numbers for which full rows in this bag
   * can be constructed.
   * 
   * @return non-null, not empty, strictly ascending list of positive row numbers
   * 
   * @see #getRow(long)
   */
  List<Long> getFullRowNumbers();

  

  /**
   * The lowest (full) row number in the bag, or 0 if empty.
   * 
   * @return &ge; 0
   */
  default long lo() {
    List<Long> rns = getFullRowNumbers();
    return rns.isEmpty() ? 0L : rns.get(0);
  }
  

  /**
   * The highest (full) row number in the bag, or 0 if empty.
   *
   * @return &ge; {@linkplain #lo()}
   */
  default long hi() {
    List<Long> rns = getFullRowNumbers();
    return rns.isEmpty() ? 0L : rns.get(rns.size() - 1);
  }
  
  
  /**
   * Determines if full (hash) information is available for the row with the given number.
   * 
   * @return {@code Collections.binarySearch(getFullRowNumbers(), rowNumber) >= 0}
   */
  default boolean hasFullRow(long rowNumber) {
    return Collections.binarySearch(getFullRowNumbers(), rowNumber) >= 0;
  }
  
  
  
  
  
  /**
   * Returns the row in this bag with the given number.
   * 
   * @param rowNumber one of the {@linkplain #getFullRowNumbers() full-row-numbers}
   * 
   * @return
   */
  default Row getRow(long rowNumber) {
    return new BaggedRow(rowNumber, this);
  }


  @Override
  default int hashWidth() {
    return SldgConstants.HASH_WIDTH;
  }


  @Override
  default String hashAlgo() {
    return SldgConstants.DIGEST.hashAlgo();
  }


  @Override
  default ByteBuffer sentinelHash() {
    return SldgConstants.DIGEST.sentinelHash();
  }

}
