/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.EasyList;
import io.crums.util.Lists;
import io.crums.util.hash.Digest;
import io.crums.util.hash.Digests;

/**
 * <p>
 * Skeletal skip ledger implementation. While the storage layer here is abstracted
 * away, it models a fixed width table of "cells". Each of these cells contains a
 * hash, which SHA-256 being the default, is 32 bytes wide. This class pins down the
 * arithmetic for grouping and rendering these cells as rows in the skip ledger.</p>
 * 
 * <h3>Numbering</h3>
 * 
 * <p>
 * Row numbers are 1-based (start at 1). This is not strictly true: every ledger
 * contains an <em>abstract</em>, predefined sentinel row numbered zero whose hash
 * is also zero.
 * </p><p>
 * The numbering of cells, however, follows the usual zero-based convention.
 * </p>
 * 
 * <h3>Implementation Note</h3>
 * 
 * <p>This base class is stateless (it defines no instance fields, only methods).
 * A concrete implementation, of course does have state. This "statelessness", in turn,
 * is exploited in the {@linkplain FilterLedger} pattern.</p>
 * 
 * <h3>Changing the Hash Function</h3>
 * 
 * <p>The default hash function (SHA-256) can be changed by overriding an implementation's
 * following methods:
 * <ol>
 * <li>{@linkplain #hashWidth()}</li>
 * <li>{@linkplain #hashAlgo()}</li>
 * <li>{@linkplain #sentinelHash()}</li>
 * <li><b>Or equivalently:</b> by updating {@linkplain #DEF_DIGEST} and recompiling 🙃</li>
 * </ol>
 * This information <em>could</em> be encoded in a single object, but it's still a reference
 * I don't care to carry around per instance and its anscilliary objects.
 * 
 * </p>
 * <h4>I/O Methods</h4>
 * 
 * <p>These are {@linkplain #getCells(long, int)} and {@linkplain #putCells(long, ByteBuffer)}
 * which a concrete class (e.g. file-backed or RDBMS-backed) must implement.</p>
 * 
 * @see #skipCount(long)
 * @see #cellNumber(long)
 */
public abstract class SkipLedger implements Digest {
  
  
  /**
   * The default hashing algorithm is set here. Currently SHA-256.
   * 
   * @see Digests#SHA_256
   */
  public final static Digest DEF_DIGEST = Digests.SHA_256;
  
  
  
  
  
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
   * Returns the number of rows in this ledger. Recall the row numbers are one-based,
   * so if the ledger is not empty, then the return value also represents that last existing
   * row number.
   * 
   * @return &ge; 0
   */
  public long size() {
    return maxRows(cellCount());
  }
  

  /**
   * Returns the total number of backing cells.
   */
  public abstract long cellCount();
  
  
  /**
   * Internally, whenever a hash needs to be calculated, this is the digest that is used.
   * This is a hook to provide reuse.
   */
  protected MessageDigest digest() {
    return newDigest();
  }
  
  
  /**
   * @return 32
   */
  public int hashWidth() {
    return DEF_DIGEST.hashWidth();
  }
  
  
  /**
   * @return SHA-256
   */
  public String hashAlgo() {
    return DEF_DIGEST.hashAlgo();
  }
  
  
  
  @Override
  public ByteBuffer sentinelHash() {
    return DEF_DIGEST.sentinelHash();
  }
  
  
  /**
   * Returns the specified cells from persistent storage.
   * 
   * @param index the starting cell index (&ge; 0)
   * @param count the number of cells to return (&ge; 2)
   * 
   * @return a buffer with <tt>count * </tt> {@linkplain #hashWidth()} <em>remaining</em> bytes.
   *    the returned buffer may be read-only
   */
  protected abstract ByteBuffer getCells(long index, int count);
  
  
  /**
   * Writes the given cells to persistent storage. On return, the state instance is updated.
   * (This is critical to the proper working of the {@linkplain FilterLedger} pattern used
   * here.)
   * 
   * @param index the starting cell index (&ge; 0)
   * @param cells contiguous block of cells
   */
  protected abstract void putCells(long index, ByteBuffer cells);
  
  
  
  /**
   * Returns the given row. Each row consists of multiple [hash] cells (each
   * {@linkplain #hashWidth()} bytes wide). The first cell contains the hash of
   * the row's content (the hash of a row in a backing table, for example). The next cell
   * contains a hash pointer to the previous row. There are as many hash pointers to previous
   * rows as defined by {@linkplain #skipCount(long)}.
   * 
   * @param rowNumber positive (&gt; 0), since the sentinel row is <em>abstract</em> (a row
   *                  whose hash is identically zero)
   * 
   * @return a possibly read-only buffer, not necessarily positioned at zero,
   *  but with {@linkplain #hashWidth()} remaining bytes
   */
  public ByteBuffer getRow(long rowNumber) {
    long index = cellNumber(rowNumber);
    int count = 1 + skipCount(rowNumber);
    return getCells(index, count);
  }
  
  
  /**
   * Returns the hash of the given row number.
   * 
   * @param rowNumber non-negative (&ge; 0), but note that row zero is a sentinel;
   *        the effective row numbers are 1-based
   * 
   * @return a possibly read-only buffer
   */
  public ByteBuffer rowHash(long rowNumber) {
    if (rowNumber < 1) {
      if (rowNumber == 0)
        return sentinelHash();
      
      throw new IllegalArgumentException("negative rowNumber: " + rowNumber);
    }
    MessageDigest digest = digest();
    ByteBuffer row = getRow(rowNumber);
    return rowHash(row, digest);
  }
  
  
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
   * Returns the rows whose pointers connect the given <tt>lo</tt> and <tt>hi</tt>
   * row numbers. The returned rows are returned in ascending order.
   * 
   * @param lo row number &ge; 1
   * @param hi row number &ge; <tt>lo</tt>
   * 
   * @return the skip rows as one contiguous block (you need to keep track of the 
   *    input arguments in order to interpret the returned block)
   * 
   * @see #skipPathNumbers(long, long)
   */
  public ByteBuffer skipPathRows(long lo, long hi) {
    long size = size();
    if (hi > size)
      throw new IllegalArgumentException("hi " + hi + " > size " + size );
    if (lo < 1)
      throw new IllegalArgumentException("lo " + lo + " < 1");
    
    List<Long> skipRowNumbers = skipPathNumbers(lo, hi);
    
    int totalCells = skipRowNumbers.stream().mapToInt(r -> 1 + skipCount(r)).sum();
    int cellWidth = hashWidth();
    
    ByteBuffer skipRows = ByteBuffer.allocate(totalCells * cellWidth);
    
    for (long rowNum : skipRowNumbers) {
      ByteBuffer row = getRow(rowNum);
      skipRows.put(row);
    }
    
    assert !skipRows.hasRemaining();
    
    return skipRows.flip();
  }
  
  

  /**
   * Appends the given row, represented as a hash, to the end of the ledger.
   * 
   * @param entryHash the hash of the next <em>entry</em> in the ledger (sans skip pointers)
   * 
   * @return the new size of the ledger, or equivalently, the row number of the last
   * entry just added
   * 
   * @see #hashWidth()
   * 
   * @see #appendNextRows(ByteBuffer)
   */
  public long appendRow(ByteBuffer entryHash) {
    int cellWidth = hashWidth();
    if (entryHash.remaining() != cellWidth)
      throw new IllegalArgumentException(
          "expected " + cellWidth + " remaining bytes: " + entryHash);
    
    long nextRowNum = size() + 1;
    
    int skipCount = skipCount(nextRowNum);
    
    ByteBuffer nextRow = ByteBuffer.allocate((1 + skipCount) * cellWidth);
    nextRow.put(entryHash);
    for (int p = 0; p < skipCount; ++p) {
      long referencedRowNum = nextRowNum - (1L << p);
      ByteBuffer hashPtr = rowHash(referencedRowNum);
      nextRow.put(hashPtr);
    }
    nextRow.flip();
    
    long cellNumber = cellNumber(nextRowNum);
    
    putCells(cellNumber, nextRow);
    return nextRowNum;
  }
  
  
  /**
   * Appends the given entries <em>en bloc</em>.
   * 
   * @param entryHashes the hashes of multiple next <em>entries</em> in the ledger
   *            (sans skip pointers)
   *            
   * @return the new size of the ledger, or equivalently, the row number of the last
   *         entry added
   *         
   * @see #appendRow(ByteBuffer)
   */
  public long appendRowsEnBloc(ByteBuffer entryHashes) {
    
    SingleTxnLedger txn = new SingleTxnLedger(this);
    txn.appendRowsEnBloc(entryHashes);
    putCells(txn.newCellIndex(), txn.newCells());
    return txn.size();
  }
  
  
  protected final int checkedEntryCount(ByteBuffer entryHashes, int cellWidth) {
    int entries = entryHashes.remaining() / cellWidth;
    if (entryHashes.remaining() % cellWidth != 0 || entries == 0)
      throw new IllegalArgumentException(
          "entryHashes remaining bytes (" + entryHashes.remaining() +
          ") not a postive multiple of hashWidth " + cellWidth);
    return entries;
  }
  
  
  
  
  private ByteBuffer rowHash(ByteBuffer row, MessageDigest digest) {
    digest.reset();
    digest.update(row);
    return ByteBuffer.wrap(digest.digest());
  }
  

}









