/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import static io.crums.sldg.SkipLedger.rowHash;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import io.crums.util.Lists;

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
 * In retrospect, this observation is a bit obvious: a skip
 * ledger's rows can always be regenerated from scratch. A {@linkplain RowBag} implemented this
 * way, is much like regenerating a subset of a skip ledger from scratch.
 * </p>
 */
public interface PathBag extends RowBag {
  
  Path path();
  
  
  /**
   * {@inheritDoc } By default a {@code PathBag} first looks up a
   * row's hash using {@link #refOnlyHash(long)}, and if not found
   * invokes {@link RowBag#getRow(long) getRow(rowNo)}.{@link BaggedRow#hash() hash()}.
   * 
   */
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



  @Override
  default LevelsPointer levelsPointer(long rowNo) {
    
    final var pathRns = getFullRowNumbers();
    int index = Collections.binarySearch(pathRns, rowNo);
    if (index < 0)
        throw new IllegalArgumentException(
          "rowNo " + rowNo + " not contained in bag; " + this);

    if (isCondensed() && SkipLedger.isCondensable(rowNo)) {
      int level;
      long refedRowNo;
      if (index == 0) {
        level = 0;
        refedRowNo = rowNo - 1;
      } else {
        refedRowNo = pathRns.get(index - 1);
        long diff = rowNo - refedRowNo;
        level = Long.numberOfTrailingZeros(diff);
        if (diff <= 0 || diff != Long.highestOneBit(diff) ||
            level >= SkipLedger.skipCount(rowNo))
          throw new IllegalArgumentException(
              "assertion failure: row [" + rowNo + "]; not linked: " + pathRns +
               "; implemenation class: " + getClass());
      }

      var funnel = getFunnel(rowNo, level).orElseThrow(
        () -> new IllegalArgumentException(
            "expected funnel [" + rowNo + ":" + level + "] not found"));

      return
          new LevelsPointer(rowNo, level, rowHash(refedRowNo), funnel);
    }


    final int levels = SkipLedger.skipCount(rowNo);
    List<ByteBuffer> levelHashes =
        Lists.functorList(levels, li -> rowHash(rowNo - (1L << li)));
    return new LevelsPointer(rowNo, levelHashes);
    
  }
  

  /**
   * Returns the ref-only row-hash from the minimal bag, if found;
   * {@code null}, otherwise.
   */
  ByteBuffer refOnlyHash(long rowNo);


  /**
   * Tests whether this instance contains condensed rows.
   */
  boolean isCondensed();

}
