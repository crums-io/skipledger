/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.util.Lists;
import io.crums.util.Sets;

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
public interface RowBag {
  
  
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
   * The lowest (full) row number in the bag, or 0 if empty. In the context of morsels,
   * (i.e. non-empty instances) this is set to 1L.
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
   */
  default Row getRow(long rowNumber) {
    return new BaggedRow(rowNumber, this);
  }
  
  
  /**
   * Returns a path connecting the given target row numbers. Note the target row numbers
   * must be contained in this bag.
   * 
   * @param targets 2 or more monotonically increasing row numbers that are to be
   *                included in the returned path
   * 
   * @return a path connecting the above targets.
   * 
   * @see #getFullRowNumbers()
   * @see #hasFullRow(long)
   * @see #getPath(List)
   */
  default Path getPath(long... targets) {
    return getPath(Lists.longList(targets));
  }
  

  /**
   * Returns a path connecting the given target row numbers. Note the target row numbers
   * must be contained in this bag.
   * 
   * @param targets 2 or more ascending row numbers that are to be
   *                included in the returned path
   * 
   * @return a path connecting the {@code targets}.
   * 
   * @see #getFullRowNumbers()
   * @see #hasFullRow(long)
   * @see #getPath(long...)
   */
  default Path getPath(List<Long> targets) {
    if (targets.size() < 2)
      throw new IllegalArgumentException("at least 2 targets required: " + targets);

    var rns = SkipLedger.stitch(targets);
    
    if (!Sets.sortedSetView(getFullRowNumbers()).containsAll(rns)) {
      var notFound = new ArrayList<>(targets);
      notFound.removeAll(getFullRowNumbers());
      throw new IllegalArgumentException(
          "not found: " + notFound + " in targets " + targets);
    }
    
    return new Path(Lists.map(rns, rn -> getRow(rn)));
  }
  
  
  
  /**
   * Returns a path connecting the first row (numbered 1), the given {@code targets},
   * and the last row (numbered {@linkplain #hi()}. 
   * 
   * @param targets <em>zero</em> or more ascending row numbers that are to be
   *                included in the returned path
   * @return a path connecting the first row, the given {@code targets}, and the last row
   */
  default Path getFullPath(List<Long> targets) {
    if (targets.isEmpty())
      getPath(lo(), hi());
    
    // make a mutable copy of the argument
    var original = targets;
    {
      var copy = new ArrayList<Long>(targets.size() + 2);
      copy.addAll(targets);
      targets = copy;
    }
    
    long firstRn = targets.get(0);
    final long lo = lo(); // (it's always 1: should prolly hardcode it)
    if (firstRn < lo)
      throw new IllegalArgumentException("targets < " + lo + ": " + original);
    if (firstRn > lo)
      targets.add(0, lo);
    
    long lastRn = targets.get(targets.size() - 1);
    final long hi = hi();
    if (lastRn > hi)
      throw new IllegalArgumentException("targets > " + hi + ": " + original);
    if (lastRn < hi)
      targets.add(hi);
    
    return getPath(targets);
  }
  

}
