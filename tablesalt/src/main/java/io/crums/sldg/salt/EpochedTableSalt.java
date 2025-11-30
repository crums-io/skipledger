/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.salt;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;

/**
 * A composition of table salts, each spanning a range of row numbers.
 * 
 * <h2>Purpose</h2>
 * <p>
 * While {@linkplain TableSalt} is designed to <em>minimize</em> the
 * number of secrets (to one seed salt), this subclass allows one to
 * specify a <em>new</em> secret seed, after a specified row number.
 * Ideally, it would never be necessary to introduce new secret seeds. Still,
 * there will likely be cases where introducing a new secret seed will be
 * warranted: it may even be policy.
 * </p>
 * <h2>Limits</h2>
 * <p>
 * This is designed to support a moderate number of seeds, no more than
 * say tens of thousands, since it's all held in memory.
 * </p>
 */
public class EpochedTableSalt extends TableSalt {
  
  public static TableSalt createTableSalt(Collection<? extends EpochSeed> seeds) {
    final int count = seeds.size();
    switch (count) {
    case 0:     throw new IllegalArgumentException("empty seeds");
    case 1:
      EpochSeed epochSeed = seeds.iterator().next();
      if (epochSeed.startRow() != 1L)
        throw new IllegalArgumentException("startNo (1) not found: " + seeds);
      return new TableSalt(epochSeed.seed());
    default:
      return new EpochedTableSalt(seeds, count > 100);
    }
  }
  
  /**
   * Salt seed and starting row number.
   */
  public static class EpochSeed implements Comparable<EpochSeed> {
    
    private final long startRow;
    private final byte[] seed;
    
    /**
     * The only constructor.
     * 
     * @param startNo   first row no. seed applies to
     * @param seed      salt seed (a secret)
     */
    public EpochSeed(long startNo, byte[] seed) {
      this.startRow = startNo;
      this.seed = seed;
      if (startNo < 1L)
        throw new IllegalArgumentException("startNo not positive: " + startNo);
      if (seed.length < MIN_SEED_BYTES)
        throw new IllegalArgumentException(
            "seed length (%d) < MIN_SEED_BYTES (%d)"
            .formatted(seed.length, MIN_SEED_BYTES));
    }
    
    /**
     * Returns the first row number this "epoch" handles.
     * 
     * @return &ge; 1L
     */
    public final long startRow() {
      return startRow;
    }
    
    /**
     * Returns the table salt for this epoch. This returns the
     * <em>same</em> instance across invocations (so that it can be cleared).
     */
    public final byte[] seed() {
      return seed;
    }

    /**
     * Instances are ordered by {@linkplain #startRow()}.
     */
    @Override
    public final int compareTo(EpochSeed o) {
      return Long.compare(startRow, o.startRow);
    }

    /**
     * Instances are equal if they have the same {@linkplain #startRow()}
     * and same {@linkplain #seed()}.
     * 
     * @see TableSalt#sameSeed(TableSalt)
     */
    @Override
    public final boolean equals(Object o) {
      return o == this ||
          o instanceof EpochSeed s &&
          s.startRow == startRow &&
          Arrays.equals(s.seed, seed);
    }

    /** @return {@linkplain #startRow()} */
    @Override
    public final int hashCode() {
      return Long.hashCode(startRow);
    }
    
    /** @return parenthesized {@linkplain #startRow()} */
    @Override
    public String toString() {
      return "(" + startRow + ")";
    }
  }

  
  private long[] startRows;
  private TableSalt[] salters;
  private boolean binarySearch;
  
  /**
   * Full constructor.
   * 
   * @param epochSeeds   not empty (size 1 supported pedagogically)
   * @param binarySearch if {@code true} then start-rows (see
   *                     {@linkplain EpochSeed#startRow()} are binary-searched;
   *                     otherwise, searched linearly, last-to-first.
   * @see #createTableSalt(Collection)
   */
  public EpochedTableSalt(
      Collection<? extends EpochSeed> epochSeeds, boolean binarySearch) {
    super(new byte[MIN_SEED_BYTES]);
    EpochSeed[] copy = epochSeeds.toArray(new EpochSeed[epochSeeds.size()]);
    if (copy.length == 0)
      throw new IllegalArgumentException("empty saltEpochs");
    
    this.startRows = new long[copy.length];
    this.salters = new TableSalt[copy.length];
    this.binarySearch = binarySearch;
    
    Arrays.sort(copy);
    long lastNo = copy[0].startRow();
    if (lastNo != 1L)
      throw new IllegalArgumentException("startNo (1) not found: " + epochSeeds);
    
    startRows[0] = lastNo;
    salters[0] = new TableSalt(copy[0].seed());
    final int seedSize = copy[0].seed().length;
    
    for (int index = 1; index < copy.length; ++index) {
      var es = copy[index];
      if (es.startRow() == lastNo)
        throw new IllegalArgumentException(
            "duplicate startNo %d: %s".formatted(lastNo, epochSeeds));
      if (es.seed().length != seedSize)
        throw new IllegalArgumentException(
            "mismatched seed lengths: %d (startNo %d) and %d (previous)"
            .formatted(es.seed().length, es.startRow, seedSize));
      lastNo = es.startRow();
      startRows[index] = lastNo;
      salters[index] = new TableSalt(es.seed());
    }
  }

  @Override
  public void close() {
    super.close();
    for (var salter : salters)
      salter.close();
  }

  @Override
  public byte[] rowSalt(long row, MessageDigest digest)
      throws IllegalStateException {
    return salters[indexOf(row)].rowSalt(row, digest);
  }

  @Override
  public byte[] cellSalt(long row, long cell, MessageDigest digest) {
    return salters[indexOf(row)].cellSalt(row, cell, digest);
  }
  
  private int indexOf(long row) {
    if (row <= 0L)
      throw new IllegalArgumentException("row (no.) must be positive: " + row);
    if (binarySearch) {
      int index = Arrays.binarySearch(startRows, row);
      assert index != -1;
      return index < 0 ? -2 - index : index;
    }
    int index = startRows.length;
    while (index-- > 1 && startRows[index] > row);
    return index;
  }
  
  
  
  
  

}

































