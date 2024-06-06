/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;
import io.crums.sldg.SkipLedger;


/**
 * Implements a reasonable caching strategy for {@linkplain SkipLedger}
 * {@linkplain Row row}s.
 * 
 * <h2>General Rationale</h2>
 * <p>
 * Proofs of membership of a row in a ledger involve constructing a linked {@linkplain Path path}
 * from the latest (highest numbered) row in the skip ledger to a specified earlier
 * row (lower numbered). Since such paths are constructed using a minimum number of linked rows,
 * the more skip pointers a row has, the more likely 
 * </p>
 * 
 * <h2>Static Cache Structure</h2>
 * <p>
 * The main data structure here is a fixed-depth binary tree of rows. (For now, consider
 * each row as just a single node in this tree.)
 * Rows (nodes) at each level in this tree share the same number of skip pointers.
 * At the root of this tree sits the row (again, node) in the skip ledger with the
 * greatest number of skip pointers. It's never a tie. Following the root level, the rows
 * at each subsequent level have one fewer [common number of] skip pointer[s].
 * </p><p>
 * The binary tree itself is implemented as an array. Searches take constant time because
 * nodes (rows) in the tree are never traversed; for a given row number, its index into
 * this array is calculated and if the node (row) there has matching row number, it's returned.
 * </p>
 * 
 * <h3>Notes</h3>
 * <h4>Concurrency Tradeoffs</h4>
 * <p>
 * Under concurrent load, pushes to the cache may not always succeed. This is a side-effect
 * of trying to minimize concurrent touch-points. The touchpoints here are 2 volatile references
 * and an exclusive lock held when the maximum height of the tree shouldn't change. Searching the tree is
 * constant in time, however a cache hit will miss if the tree root is being concurrently raised. Since
 * such events are finite and few (&lt; 62), this optimistic strategy is expected to pay off most of
 * the time.
 * </p>
 * <h4>State Path Caching Prop</h4>
 * <p>
 * We're specially storing row 1 here because it always lies on the state path. But the same can be
 * said of rows 2, 4, 8, and so on. (And also on the way down)
 * </p>
 */
public class RowCache {
  
  /**
   * Maximum number of levels cached (22). This translates to
   * a maximum of 4194303 ({@code (1 << 22) - 1}) rows stored
   * in the tree cache.
   */
  public final static int MAX_LEVELS_CACHED = 22;
  
  public final static int DEFAULT_LEVELS_CACHED = 10;
  
  private final Object lastRowLock = new Object();

  private final int levelsCached;
  
  private final int minLevelCached;
  
  private final SerialRow[] treeCache;
  
  /**
   * The maximum level known to the cache.
   */
  private int maxLevel;
  
  //
  // special bookkeeping for first and last rows
  //
  
  private volatile SerialRow rowOne;
  
  // the last row seen
  private volatile SerialRow lastRow;
  private long lastRowNumber;
  
  
  
  
  /**
   * 
   * @param levelsCached the number of top levels cached, &ge; 1 and &le;
   *                     {@linkplain #MAX_LEVELS_CACHED}
   * @param minLevelCached the minimum (skip-pointer) level held in the cache
   *                      (&ge; 1)
   */
  public RowCache(int levelsCached, int minLevelCached) {
    if (levelsCached < 1 || levelsCached >  MAX_LEVELS_CACHED)
      throw new IllegalArgumentException("levelsCached " + levelsCached);
    if (minLevelCached < 1 || minLevelCached > 63)
      throw new IllegalArgumentException(
          "minLevelCached " + minLevelCached);
    
    this.levelsCached = levelsCached;
    this.minLevelCached = minLevelCached;
    
    int nodeCount = (1 << levelsCached) - 1;
    this.treeCache = new SerialRow[nodeCount];
    
    
  }
  
  
  public SerialRow getRow(long rowNumber) {

    if (rowNumber < 2) {
      if (rowNumber == 1)
        return rowOne;
      
      throw new IllegalArgumentException("rowNumber " + rowNumber);
    }
    
    if (rowNumber == lastRowNumber)
      return lastRow(rowNumber);
      
    if ((rowNumber & 1L) != 0)
      return null;
    
    // below, same as SkipLedger.skipCount(rowNumber) - 1
    int rowLevel = Long.numberOfTrailingZeros(rowNumber);
    if (rowLevel < minLevelCached)
      return null;
    
    return getRowFromTree(rowNumber, rowLevel);
  }
  
  
  /**
   * Returns the cached last row
   * @param rowNumber
   * @return
   */
  private SerialRow lastRow(long rowNumber) {
    SerialRow last = lastRow;
    return last != null && last.no() == rowNumber ? last : null;
  }
  
  
  /**
   * 
   * @param rowNumber &gt; 0
   * @param rowLevel {@code = Long.numberOfTrailingZeros(rowNumber)}
   */
  private SerialRow getRowFromTree(long rowNumber, int rowLevel) {
    
    SerialRow cachedRow;
    
    synchronized (treeCache) {

      int serialIndex = indexInTree(rowNumber, rowLevel, this.maxLevel);
      if (serialIndex == -1)
        return null;
      
      cachedRow = treeCache[serialIndex];
    }
    
    return cachedRow != null && cachedRow.no() == rowNumber ? cachedRow : null;
  }
  
  
  private int indexInTree(long rowNumber, int rowLevel, int maxLevel) {

    if (rowLevel > maxLevel)
      return -1;
    
    // Not using the code there (cuz short n simple) but I doc'ed
    // it in io.crums.util.tree.Trees to see how this breadth-first
    // serial indexing works.
    
    final int depth = maxLevel - rowLevel;
    
    if (depth == 0) {
      
      return 0;
    
    } else if (depth >= this.levelsCached) {
      
      return -1;
    
    } else {
      
      // depthStartIndex is the same as the number of nodes in a tree of (depth - 1)
      int depthStartIndex = (1 << (depth - 1)) - 1;
      int nodesAtDepth = 1 << depth;
      
      
      long indexAtLevel = (rowNumber >>> rowLevel) - 1;
      // for eg, rowNumber=8  (base 2:  1000), rowLevel=3, => indexAtLevel=0
      //         rowNumber=12 (base 2:  1100), rowLevel=2, => indexAtLevel=2
      //         rowNumber=22 (base 2: 10110), rowLevel=1, => indexAtLevel=10
      
      
      
      if (indexAtLevel >= nodesAtDepth)
        return -1;
      
      return depthStartIndex + (int) indexAtLevel;
    }
  }
  
  
  
  public void pushRow(Row row) {
    
    final long rn = row.no();
    
    if (rn == 1) {
      this.rowOne = SerialRow.toInstance(row);
      return;
    }
    
    if (this.lastRowNumber <= rn)
      setLastRowNumber(row);
    
    final int rowLevel = Long.numberOfTrailingZeros(rn);
    if (rowLevel < this.minLevelCached)
      return;
    
    int maxLevelSnap = this.maxLevel;
    int reqMaxLevel = 64 - Long.numberOfLeadingZeros(rn);
    final int assumedMaxLevel = Math.max(reqMaxLevel, maxLevelSnap);
    
    int index = indexInTree(rn, rowLevel, assumedMaxLevel);
    if (index == -1)
      return;
    
    synchronized (treeCache) {
      if (maxLevel < assumedMaxLevel)
        this.maxLevel = assumedMaxLevel;
      else if (maxLevel > assumedMaxLevel) {
        index = indexInTree(rn, rowLevel, maxLevel);
        if (index == -1)
          return;
      }
      treeCache[index] = SerialRow.toInstance(row);
    }
  }
  
  
  
  
  
  /**
   * Conditionally sets the last row number and pushes it to the cache tree.
   * 
   * @see #pushToTree(Row)
   */
  private void setLastRowNumber(Row row) {
    long rn = row.no();
    synchronized (lastRowLock) {
      if (lastRow == null || lastRow.no() < rn) {
        this.lastRow = SerialRow.toInstance(row);
        this.lastRowNumber = rn;
      }
    }
  }
  

}
















