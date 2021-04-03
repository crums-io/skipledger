/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;

import io.crums.util.Lists;

/**
 * Structural representation of a path in a ledger.
 */
public class PathInfo {
  
  private final List<Long> declaration;
  
  
  /**
   * Constructs an instance using the given declaring row numbers. These are typically
   * a subset of the final {@linkplain #rowNumbers() row numbers} which are composed
   * by stitching in linking row numbers as necessary.
   * 
   * @param declaration positive, not empty, sorted list of positive row numbers
   */
  public PathInfo(List<Long> declaration) {
    this.declaration = Lists.readOnlyOrderedCopy(declaration);
  }
  
  
  /**
   * Constructs a skip path instance.
   * 
   * @param lo &ge; 1
   * @param hi &gt; {@code lo}
   */
  public PathInfo(long lo, long hi) {
    if (lo < 1 || hi <= lo)
      throw new IllegalArgumentException("lo, hi: " + lo + ", " + hi);
    Long[] rows = { lo, hi };
    this.declaration = Lists.asReadOnlyList(rows);
  }
  
  
  /**
   * Constructs a camel path instance. A camel path instance is the concatentation
   * of 2 adjacent skip paths meeting at the {@code target} row number.
   * 
   * @param lo &ge; 1
   * @param target &ge; {@code lo}
   * @param hi &gt; {@code target}
   */
  public PathInfo(long lo, long target, long hi) {
    if (lo <= 0 || target < lo || hi <= target)
      throw new IllegalArgumentException(
          "lo " + lo + "; target " + target + "; hi " + hi);
    
    Long[] rows = { lo, target, hi };
    this.declaration = Lists.asReadOnlyList(rows);
  }
  
  
  /**
   * Returns the row numbers that structurally define the path.
   * If necessary (usually), this is a stitched version of the {@linkplain
   * #declaration() declaration}.
   * 
   * @return lazily loaded, read-only list of linked row numbers
   */
  public final List<Long> rowNumbers() {
    return Ledger.stitch(declaration);
  }
  
  
  /**
   * Returns the declared row numbers. These are the exactly the
   * numbers specified at construction.
   * 
   * @return ordered list of positive longs with no duplicates
   * 
   * @see #rowNumbers()
   */
  public final List<Long> declaration() {
    return declaration;
  }
  
  /**
   * Returns the lowest row number.
   */
  public final long lo() {
    return declaration.get(0);
  }
  
  
  /**
   * Determines if this is a state path. By convention, a path that starts at row number 1
   * is considered a <em>state path</em> from the ledger.
   * 
   * @return {@code lo() == 1L}
   */
  public final boolean isState() {
    return lo() == 1L;
  }
  
  
  /**
   * Returns the highest row number.
   */
  public final long hi() {
    return declaration.get(declaration.size() - 1);
  }
  
  
  /**
   * <p>Instances are equal if their data are equal.</p>
   * {@inheritDoc}
   */
  public final boolean equals(Object o) {
    return o == this || (o instanceof PathInfo) && ((PathInfo) o).declaration.equals(declaration);
  }
  
  
  /**
   * <p>Consistent with {@linkplain #equals(Object)}.</p>
   * {@inheritDoc}
   */
  public final int hashCode() {
    return declaration.hashCode();
  }

}
