/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.EasyList;
import io.crums.util.Lists;
import io.crums.util.hash.Digest;

/**
 * 
 */
public abstract class Ledger implements Digest, Closeable {
  
  /**
   * <p>Returns the number of skip pointers at the given row number. The
   * returned number is one plus the <em>exponent</em> in the highest power of 2 that is
   * a factor of the row number. For odd row numbers, this is always 1 (since the highest
   * factor here is 2<sup><small>0</small></sup>).</p>
   * 
   * <h3>Strict Law of Averages</h3>
   * 
   * <p>The <em>average</em> number of skip pointers is <em>always</em> less than 2.
   * Equivalently, the total number of skip pointers up to (and including) a given row
   * number, is always less than twice the row number.
   * </p>
   * 
   * @param rowNumber &gt; 0
   * @return &ge; 1 (with average value of 2)
   * 
   * @see #maxRows(long)
   */
  public static int skipCount(long rowNumber) {
    checkRealRowNumber(rowNumber);
    return 1 + Long.numberOfTrailingZeros(rowNumber);
  }
  

  /**
   * Throws an <tt>IllegalArgumentException</tt> if the given row number is not &ge; 1.
   * If the row number is zero, the thrown exception details why row <tt>0</tt> is a bad
   * argument.
   * 
   * @param rowNumber &ge; 1
   * @throws IllegalArgumentException if <tt>rowNumber &le; 0</tt>
   */
  public static void checkRealRowNumber(long rowNumber) throws IllegalArgumentException {
    
    if (rowNumber < 1) {
      
      String msg;
      if (rowNumber == 0)
        msg =
            "row 0 is an *abstract row that hashes to zeroes; " +
            "its has infinite pointer levels (";
      else
        msg = "negative rowNumber " + rowNumber;
      
      throw new IllegalArgumentException(msg);
    }
  }
  

  
  

  /**
   * Returns the rows <em>covered</em> by the specified 
   * @param lo
   * @param hi
   * @return
   */
  public static SortedSet<Long> skipPathCoverage(long lo, long hi) {
    
    return coverage(skipPathNumbers(lo, hi));
  }
  
  
  
  /**
   * Returns the rows <em>covered</em> by the given row numbers. The returned
   * ordered set contains both the given row numbers and the row numbers referenced
   * in the rows at those row numbers. Altho its size is sensitive to its inputs,
   * the returned list never blows up: it's size may grow at most by a factor that
   * is no greater than the base 2 log of the highest row number in its input.
   * <p>
   * <em>Note the returned set may (and likely does) contain the sentinel row number 0 (zero).
   * </em> The reason why is that the returned set, is the set of row numbers whose
   * hashes are needed to compute the hashes of the given row numbers. And while the contents
   * of the sentinel row are undefined, it's hash <em>is</em>.
   * </p>
   * 
   * @param rowNumbers non-empty bag of positive (&ge; 1) numbers,
   *        in whatever order, dups OK
   * 
   * @return non-empty set of row
   */
  public static SortedSet<Long> coverage(Collection<Long> rowNumbers) {

    SortedSet<Long> covered = new TreeSet<>();
    for (Long rowNumber : rowNumbers) {
      covered.add(rowNumber);
      int pointers = skipCount(rowNumber);
      for (int e = 0; e < pointers; ++e) {
        long delta = 1L << e;
        long referencedRowNumber = rowNumber - delta;
        assert referencedRowNumber >= 0;
        covered.add(referencedRowNumber);
      }
    }
    
    return Collections.unmodifiableSortedSet(covered);
  }
  
  
  /**
   * Returns the structural path from a lower
   * (older) row number to a higher (more recent) row number in a ledger.
   * This is just the shortest structural path following the hash pointers in each
   * row from the <tt>hi</tt> row number to the <tt>lo</tt> one. The returned list
   * however is returned in reverse order, in keeping with the temporal order of
   * ledgers.
   * 
   * @param lo row number &gt; 0
   * @param hi row number &ge; <tt>lo</tt>
   * 
   * @return a monotonically ascending list of numbers from <tt>lo</tt> to </tt>hi</tt>,
   *         inclusive
   */
  public static List<Long> skipPathNumbers(long lo, long hi) {
    if (lo < 1)
      throw new IllegalArgumentException("lo " + lo + " < 1");
    if (hi <= lo) {
      if (hi == lo)
        return Collections.singletonList(lo);
      else
        throw new IllegalArgumentException("hi " + hi + " < lo " + lo);
    }
    
    // create a descending list of row numbers (which we'll reverse)
    EasyList<Long> path = new EasyList<>(16);
    path.add(hi);
    
    for (long last = path.last(); last > lo; last = path.last()) {
      
      for (int base2Exponent = skipCount(last); base2Exponent-- > 0; ) {
        long delta = 1L << base2Exponent;
        long next = last - delta;
        if (next >= lo) {
          path.add(next);
          break;
        }
      }
    }
    
    return Lists.reverse(path);
  }
  
  
  
  
  
  
  /**
   * Appends one or more hash entries to the end of the ledger.
   * 
   * 
   * @param entryHashes the input hash of the next {@linkplain Row row}
   * 
   * @return the new size of the ledger, or equivalently, the row number of the last
   * entry just added
   * 
   * @see #hashWidth()
   */
  public abstract long appendRows(ByteBuffer entryHashes);
  
  
  
  
  
  /**
   * Returns the number of rows in this ledger. Recall the row numbers are one-based,
   * so if the ledger is not empty, then the return value also represents that last existing
   * row number.
   * 
   * @return &ge; 0
   */
  public abstract long size();
  
  
  
  public boolean isEmpty() {
    return size() == 0;
  }
  
  

  /**
   * Returns the row with the given number.
   * 
   * @param rowNumber positive (&gt; 0), since the sentinel row is <em>abstract</em> (a row
   *                  whose hash is identically zero)
   * 
   * @return non-null
   */
  public abstract Row getRow(long rowNumber);
  
  
  /**
   * Returns the hash of the row at the given number.
   * 
   * @param rowNumber non-negative (&ge; 0), but note that row zero is a sentinel;
   *        the effective row numbers are 1-based
   * 
   * @return a possibly read-only buffer
   */
  public abstract ByteBuffer rowHash(long rowNumber);
  

  /**
   * Returns a hash representing the current state of the ledger.
   * 
   * @return the hash of the last row, if not empty; the sentinel hash (all zeroes), if empty.
   */
  public ByteBuffer stateHash() {
    long size = size();
    return size == 0 ? sentinelHash() : rowHash(size);
  }
  

  
  /**
   * Returns the skip-path from row 1 to the row numbered {@linkplain #size() size},
   * or <tt>null</tt> if this ledger is empty.
   */
  public SkipPath statePath() {
    long size = size();
    return size == 0 ? null : skipPath(1, size);
  }
  
  
  /**
   * Returns the skip-path (the shortest string of rows) connecting the row with the
   * given <tt>lo</tt> number from the row with the given <tt>hi</tt> number.
   */
  public SkipPath skipPath(long lo, long hi) {
    if (hi > size())
      throw new IllegalArgumentException("hi " + hi + " > size " + size());
    if (lo < 1)
      throw new IllegalArgumentException("lo " + lo + " < 1");
    
    List<Long> rowNumPath = skipPathNumbers(lo, hi);
    int length = rowNumPath.size();
    Row[] rows = new Row[length];
    for (int index = length; index-- > 0; )
      rows[index] = getRow(rowNumPath.get(index));
    
    return new SkipPath(Lists.asReadOnlyList(rows), Collections.emptyList(), false);
  }
  
  
  
  @Override
  public void close() {  }
  

}
