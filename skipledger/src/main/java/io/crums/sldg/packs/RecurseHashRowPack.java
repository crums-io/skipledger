/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.bags.RowBag;


/**
 * Recursive implementation of {@linkplain RowBag#rowHash(long)} for a
 * bag without redundant data.
 * 
 * <h2>Tautological Self-consistency</h2>
 * <p>
 * An important consequence of laying out row data this way (i.e. without redundancies)
 * is that the data is collectively self-consistent. What this means is that given a
 * {@linkplain RowPack} with the correct number of hash entries, the individual hash entries
 * don't need to be validated against each other: they collectively represent <em>some</em>
 * ledger.
 * </p><p>
 * In retrospect, this observation is a bit obvious: aside from its trailed (witnessed) rows a skip
 * ledger's rows can always be regenerated from scratch. A {@linkplain RowBag} implemented this
 * way, is much like regenerating a subset of a skip ledger from scratch.
 * </p>
 */
public abstract class RecurseHashRowPack implements RowBag {

  @Override
  public ByteBuffer rowHash(long rowNumber) {

    if (rowNumber <= 0) {
      if (rowNumber == 0)
        return SldgConstants.DIGEST.sentinelHash();
      throw new IllegalArgumentException("negative rowNumber: " + rowNumber);
    }
    
    ByteBuffer hash = refOnlyHash(rowNumber);
    if (hash != null)
      return hash;

    // not here? ok, assume we can construct it
    try {
      
      return getRow(rowNumber).hash();  // the recursion
      
    } catch (IllegalArgumentException internal) {
      throw new IllegalArgumentException(
          "no data for rowNumber " + rowNumber +
          " - cascaded internal error msg: " + internal.getMessage(), internal);
    }
  }
  
  
  /**
   * Returns the ref-only row-hash from the minimal store, if found. A minimal bag does not
   * redundantly contain row-hashes for rows for which their input-hashes are known: the row-hash
   * of such rows can computed from the hash of the concatenation of the input hash with the
   * row-hashes from lower numbered rows.
   * 
   * @param rowNumber
   * @return
   */
  protected abstract ByteBuffer refOnlyHash(long rowNumber);

}
