/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db.flat;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.EasyList;
import io.crums.util.Lists;

/**
 * <p>Ideas left over from prototyping (Take 1). The reason why I want to
 * keep this around (yes, it could be accessed from history) is so I
 * don't forget that for double the disk space, I can really speed up
 * serving ledger parts this way. Plus, I/O is cheaper than compute
 * (hash computation is hot), so this might be something to consider.</p>
 * 
 * <p>Note for maximum efficiency, the code below needs to be modified:
 * it needs one additional cell that would store the row's hash.</p>
 */
public class ProtoFlatLedger {
  
  

  
  /**
   * Returns the starting cell number of the given row number. This is just the
   * number of existing cells before the specified row.
   * 
   * @param rowNumber postive number (&gt; 0)
   * 
   * @return a non-negative number (&ge; 0) and &le; <tt>2 * rowNumber - 1</tt>
   */
  public static long cellNumber(long rowNumber) {
    checkRealRowNumber(rowNumber);
    
    // count the number of cells *before this row number
    --rowNumber;
    
    long index = rowNumber;
    for (long n = rowNumber; n > 0; n >>= 1)
      index += n;
    
    return index;
  }
  
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
   * Returns the number of cells for the given row number. This is just
   * one more than {@linkplain #skipCount(long)}
   * @param rowNumber
   * @return
   */
  public static int rowCells(long rowNumber) {
    return 1 + skipCount(rowNumber);
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
        if (referencedRowNumber != 0)
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
   * @param hi row number &ge; <tt>lo</tt>
   * @param lo row number &gt; 0
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
   * Throws an <tt>IllegalArgumentException</tt> if the given row number is not &ge; 1.
   * If the row number is zero, the thrown exception details why row <tt>0</tt> is a bad
   * argument.
   * 
   * @param rowNumber &ge; 1
   * @throws IllegalArgumentException
   */
  public static void checkRealRowNumber(long rowNumber) throws IllegalArgumentException {
    
    if (rowNumber < 1) {
      
      String msg;
      if (rowNumber == 0)
        msg =
            "row 0 is an *abstract row that hashes to zeroes; " +
            "it doesn't have well-defined contents";
      else
        msg = "negative rowNumber " + rowNumber;
      
      throw new IllegalArgumentException(msg);
    }
  }
  
  
  
  
  /**
   * Returns the maximum number of rows that can be fitted in a ledger with the given number
   * of cells.
   *  
   * @param cells &ge; 0
   * 
   * @return non-negative
   */
  public static long maxRows(long cells) {
    
    if (cells < 0)
      throw new IllegalArgumentException("negative cells: " + cells);

    // total number of cells never exceeds 3 times the number of rows
    // so the following estimate is never too many
    long rowsEstimate = cells / 3;
    
    // while the required number of cells for the *next higher estimate is <= cells
    while (cellNumber(rowsEstimate + 2) <= cells)
      ++rowsEstimate;
    
    return rowsEstimate;
  }
  
  
  
  
  
  

  /**
   * 
   */
  private ProtoFlatLedger() {
  }

}
