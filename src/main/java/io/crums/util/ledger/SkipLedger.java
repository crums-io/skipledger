/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

import io.crums.util.EasyList;
import io.crums.util.hash.Digest;

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
 * is exploited in the {@linkplain FilterLedger} in order to layer-in message digest
 * re-use.
 * 
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
   * The default hashing algorithm is SHA-256.
   */
  public final static String SHA256_ALGO = "SHA-256";
  
  /**
   * The hash width of the default hashing algorithm (SHA-256) is 32 bytes wide.
   */
  public final static int SHA256_WIDTH = 32;
  
  
  private final static ByteBuffer SHA256_SENTINEL_HASH =
      ByteBuffer.allocate(SHA256_WIDTH).asReadOnlyBuffer();
  
  
  
  /**
   * Returns the starting cell number of the given row number. This is just the
   * number of existing cells before the specified row.
   * 
   * @param rowNumber postive number (&gt; 0)
   * 
   * @return a non-negative number (&ge; 0)
   */
  public static long cellNumber(long rowNumber) {
    checkRowNumber(rowNumber);
    
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
   * factor here is 2<sup><small>0</small></sup>); for even numbers, it's always 2 or
   * greater.</p>
   * 
   * <h3>Strict Law of Averages</h3>
   * 
   * <p>It can be shown that the <em>average</em> number of skip pointers per row is
   * <em>never</em> more than twice the number of rows (proof not provided here).
   * Equivalently, the total number of skip pointers up to (and including) a given row
   * number, is never more than twice the row number.
   * </p>
   * 
   * @param rowNumber &gt; 0
   * @return &ge; 1 (with average value of 2)
   * 
   * @see #maxRows(long)
   */
  public static int skipCount(long rowNumber) {
    checkRowNumber(rowNumber);
    return 1 + Long.numberOfTrailingZeros(rowNumber);
  }
  
  
  
  public static List<Long> hiToLoNumberPath(long hi, long lo) {
    if (lo < 0)
      throw new IllegalArgumentException();
    if (hi <= lo) {
      if (hi == lo)
        return Collections.singletonList(lo);
      else
        throw new IllegalArgumentException("hi " + hi + " < lo " + lo);
    }
    
    // create the descending list of row numbers
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
    
    return Collections.unmodifiableList(path);
  }
  
  
  
  private static void checkRowNumber(long rowNumber) {
    if (rowNumber < 1)
      throw new IllegalArgumentException("row number (" + rowNumber + ") not positive");
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
  public abstract long size();
  
  
  
  
//  /**
//   * Returns a new digest per the hashing algo.
//   * 
//   * @see #hashAlgo()
//   */
//  public MessageDigest newDigest() {
//    String algo = hashAlgo();
//    try {
//      return MessageDigest.getInstance(algo);
//    } catch (NoSuchAlgorithmException nsax) {
//      throw new RuntimeException("failed to create '" + algo + "' digest: " + nsax);
//    }
//  }
  
  
  /**
   * Internally, whenever a hash needs to be calculated, this is the digest that is used.
   * This is a hook to provide reuse.
   */
  protected MessageDigest digest() {
    return newDigest();
  }
  
  
  /**
   * Returns the hash width in bytes.
   * 
   * @return 32
   */
  public int hashWidth() {
    return SHA256_WIDTH;
  }
  
  
  /**
   * Returns the hashing algorithm used in the ledger.
   * 
   * @return SHA-256
   */
  public String hashAlgo() {
    return SHA256_ALGO;
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
   * Appends the given row, represented as a hash, to the end of the ledger.
   * 
   * @param rowHashes the hash of the <em>content</em> of the row (sans skip pointers).
   * 
   * @return the new size of the ledger, or equivalently, the row number of the last
   * entry just added
   * 
   * @see #hashWidth()
   */
  public long appendNextRow(ByteBuffer contentHash) {
    int cellWidth = hashWidth();
    if (contentHash.remaining() != cellWidth)
      throw new IllegalArgumentException(
          "expected " + cellWidth + " remaining bytes: " + contentHash);
    
    long nextRowNum = size() + 1;
    
    int skipCount = skipCount(nextRowNum);
    
    ByteBuffer nextRow = ByteBuffer.allocate((1 + skipCount) * cellWidth);
    nextRow.put(contentHash);
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
  
  
  protected ByteBuffer sentinelHash() {
    return SHA256_SENTINEL_HASH.duplicate();
  }
  
  
  
  
  private ByteBuffer rowHash(ByteBuffer row, MessageDigest digest) {
    digest.reset();
    digest.update(row);
    return ByteBuffer.wrap(digest.digest());
  }
  

}









