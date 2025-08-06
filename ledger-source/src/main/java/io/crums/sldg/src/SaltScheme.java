/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;

import java.util.Arrays;
import java.util.Objects;

/**
 * Cell salting scheme. For the most part, this just documents how the
 * scheme works. It's primary purpose in code is to obviate the need to
 * preface a row-cell va(lue with a flag (or something similar) indicating
 * whether it is salted or not.
 * <p>
 * In the original salting scheme, ledger cells were either all salted, or
 * none were. Under the new scheme, certain column numbers (cell indices)
 * may be either uniformly salted or uniformly <em>unsalted</em>. So this is
 * slightly more flexible. One could conceive a salting scheme where the
 * scheme's rules are contingent on what data is in the row, but for now
 * no such thing is implemented.
 * </p>
 * 
 * <h2>Design Issue</h2>
 * <p>
 * The choice of salt scheme is decided by the ledger owner. A receipt from
 * the ledger, received by a user (recipient), belies the scheme chosen
 * -- at least for the specific column no.s (cell indices) in the source
 * row divulged. But why should a user care about such details? Since
 * {@linkplain SourcePackBuilder} already verifies that every source row
 * added indeed conforms to the salt scheme, why not discover the salt
 * scheme (as source rows are added)? That class already "discovers" other
 * attributes in the data set (e.g. the maximum byte-size of row cells).. we
 * can add the salting scheme to the discover process as well.
 * </p><p>
 * Why is this an issue? Because peeps are supposed to be able to manipulate
 * bindles without access to the ledger.
 * </p>
 */
public interface SaltScheme {
  
  /**
   * Returns an immutable instance.
   * 
   * @param cellIndices  copied. See {@linkplain #cellIndices()}
   * @param positive     see {@linkplain #isPositive()}
   */
  public static SaltScheme of(int[] cellIndices, boolean positive) {
    return new WrappedScheme(cellIndices, positive);
  }
  
  /** Wraps and returns the given scheme, ensuring its wellformed. */
  public static SaltScheme wrap(SaltScheme s) {
    return s instanceof WrappedScheme ? s : new WrappedScheme(s);
  }
  
  /**
   * Implementation / safe-wrapper class.
   */
  final static class WrappedScheme implements SaltScheme {
    final int[] cellIndices;
    final boolean positive;
    
    WrappedScheme(int[] cellIndices, boolean positive) {
      this.cellIndices = cellIndices.clone();
      this.positive = positive;
      if (cellIndices.length == 0)
        return;
      
      Arrays.sort(this.cellIndices);
      if (this.cellIndices[0] < 0)
        throw new IllegalArgumentException(
            "cellIndices[0] (sorted) < 0 (%d)".formatted(this.cellIndices[0]));
      
      for (int index = 1; index < cellIndices.length; ++index )
        if (this.cellIndices[index] == this.cellIndices[index -1])
          throw new IllegalArgumentException(
              "duplicate values (%d) in cellIndices[%d:%d] (sorted)"
              .formatted(this.cellIndices[index], index -1, index));
    }
    
    WrappedScheme(SaltScheme base) {
      this(base.cellIndices(), base.isPositive());
    }
    
    @Override
    public boolean equals(Object o) {
      return o == this || o instanceof WrappedScheme w && equals(w);
    }
    
    @Override
    public int hashCode() {
      int code = Arrays.hashCode(cellIndices());
      return isPositive() ? code : ~code;
    }

    @Override
    public int[] cellIndices() {
      return cellIndices.clone();
    }

    @Override
    public boolean isPositive() {
      return positive;
    }
  }
  
  
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
  
  
  /** Instances are equivalent if they have the same indices and sign. */
  default boolean equals(SaltScheme other) {
    return
        isPositive() == other.isPositive() &&
        Arrays.equals(cellIndices(), other.cellIndices());
  }

  /** Returns {@code true} iff some cell-indexes are salted, others not. */
  default boolean isMixed() {
    return cellIndices().length > 0;
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




