/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


/**
 * Cell salting scheme. For the most part, this just documents how the
 * scheme works.
 */
public interface SaltScheme {
  
  
  
  public final static SaltScheme SALT_ALL = new EmptyScheme() {
    @Override public boolean isPositive() { return false; }
  };
  
  public final static SaltScheme NO_SALT = new EmptyScheme() {
    @Override public boolean isPositive() { return true; }
  };
  
  /**
   * Returns the list of salted (or unsalted) cell indices.
   * 
   * @return a possibly empty array of cell indices
   * @see #isPositive()
   */
  int[] cellIndices();
  
  /**
   * Qualifies whether {@linkplain #cellIndices()} are the salted
   * or <em>un</em>salted cell indices. If the return value is
   * {@code false}, then <em>every</em> cell is salted excepting
   * those indexed in return value of {@code cellIndices()}.
   * <p>
   * Salting <em>all</em> cells under the scheme, then, requires
   * this method to return {@code false} and {@code cellIndices()}
   * return an empty array.
   * </p>
   */
  boolean isPositive();
  

  /** Returns {@code !noSalt()}. */
  default boolean hasSalt() {
    return !noSalt();
  }
  
  /** Returns {@code isPositive() && cellIndices().length == 0}. */
  default boolean noSalt() {
    return isPositive() && cellIndices().length == 0;
  }

  /** Returns {@code !isPositive() && cellIndices().length == 0}. */
  default boolean saltAll() {
    return !isPositive() && cellIndices().length == 0;
  }
  
  /** Determines whether the cell at the given index is salted. */
  default boolean isSalted(int cell) {
    int[] indices = cellIndices();
    int index = indices.length;
    while (index-- > 0 && indices[index] != cell);
    boolean found = index != -1;
    return isPositive() == found;
  }

}


abstract class EmptyScheme implements SaltScheme {
  final static int[] EMPTY = new int[0];
  
  @Override
  public final int[] cellIndices() {
    return EMPTY;
  }
  

  @Override
  public final boolean hasSalt() {
    return !isPositive();
  }
  

  @Override
  public final boolean noSalt() {
    return isPositive();
  }
  
  @Override
  public final boolean saltAll() {
    return !isPositive();
  }

  @Override
  public final boolean isSalted(int cell) {
    return !isPositive();
  }
}




