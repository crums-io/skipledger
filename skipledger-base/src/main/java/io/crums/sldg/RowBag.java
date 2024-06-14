/*
 * Copyright 2021-2024 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.crums.util.Lists;
import io.crums.util.Sets;

/**
 * <p>A bag of rows from a ledger. The general contract is that if a row's
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
   * Returns the level pointer.
   * @param rowNo
   * @return
   */
  default LevelsPointer levelsPointer(long rowNo) {
    final int levels = SkipLedger.skipCount(rowNo);
    List<ByteBuffer> levelHashes =
        Lists.functorList(levels, li -> rowHash(rowNo - (1L << li)));
    return new LevelsPointer(rowNo, levelHashes);
  }


  /**
   * Returns the hash-funnel for the given row no. and level index,
   * if it exists.
   * 
   * @param rn      row no. the funnel outputs level-merkel hash for
   * @param level   level index
   * 
   * @return        by default empty
   */
  default Optional<List<ByteBuffer>> getFunnel(long rn, int level) {
    return Optional.empty();
  }
  
  
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
    int size = rns.size();
    return size == 0 ? 0L : rns.get(size - 1);
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
   * <p>
   * The default implementation return's a {@linkplain BaggedRow}.
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
