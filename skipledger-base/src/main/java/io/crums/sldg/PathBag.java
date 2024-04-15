/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;

/**
 * Implementation interface of {@linkplain RowBag#rowHash(long)}. This models
 * a bag without redundant data. The general contract for such a bag is that
 * the highest row [number] in the bag threads thru all the row numbers in it
 * to lowest numbered row (usually 1). We can guarantee, that way, that the
 * rows in the bag indeed belong to the same ledger.
 * 
 * <h2>Other Design Options</h2>
 * <p>
 * A perhaps more flexible model would also allow including row no.s that are
 * linked thru a path to the highest row no. but for which a path to the first
 * row (usually [1]) isn't necessarily stored. (The skip path from highest
 * row no. to lowest row no. would be still required in the bag.) This would
 * complicate the logic of merging bags, but it's certainly doable. (Would
 * model it as a special kind of directed graph, maybe.) Punting for now.
 * </p>
 * 
 * <h2>Tautological Self-consistency</h2>
 * <p>
 * An important consequence of laying out row data this way (i.e. without redundancies)
 * is that the data is collectively self-consistent. What this means is that given a
 * {@linkplain PathBag} with the correct number of hash entries, its individual hash entries
 * don't need to be validated against each other: they collectively represent <em>some</em>
 * ledger.
 * </p><p>
 * In retrospect, this observation is a bit obvious: aside from its trailed (witnessed) rows a skip
 * ledger's rows can always be regenerated from scratch. A {@linkplain RowBag} implemented this
 * way, is much like regenerating a subset of a skip ledger from scratch.
 * </p>
 * <h2>Refactoring Note</h2>
 * <p>
 * This code was lifted from the {@code RecurseHashRowPack}.
 * </p>
 */
public interface PathBag extends RowBag {
  
  Path path();
  
  
  @Override
  default ByteBuffer rowHash(long rowNumber) {

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
   * Returns the ref-only row-hash from the minimal bag, if found;
   * {@code null}, otherwise.
   */
  ByteBuffer refOnlyHash(long rowNumber);

}
