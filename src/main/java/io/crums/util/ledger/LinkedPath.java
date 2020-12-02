/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.util.ledger;


import static io.crums.util.ledger.Constants.DEF_DIGEST;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.hash.Digest;

/**
 * A list of connected rows in a {@linkplain SkipLedger} with strictly ascending row numbers.
 * This describes a hash proof that specified rows are indeed from the same ledger with strong
 * constraints on the possible row numbers.
 * 
 * <h3>Validation Guarantee</h3>
 * 
 * Every instance's hash pointers are checked for validity at construction. Furthermore,
 * every public constructor makes a defensive copy of its row inputs. The upshot of this
 * guarantee is that a reference to instance at runtime, is a reference to an immutable proof.
 */
public class LinkedPath implements Digest {
  
  
  
  private final List<Row> path;
  
  
  /**
   * Constructs a shortest-path (skip path) instance.
   * 
   * @param lo the low row number (inclusive)
   * @param hi the hi row number (inclusive)
   * 
   * @param skipRows concatentation of rows
   */
  public LinkedPath(long lo, long hi, ByteBuffer skipRows) {
    this(lo, hi, skipRows, true);
  }
  
  
  /**
   * Package-private constructor for creating a shortest path instance. This allows
   * skipping a defensive copy when it is provably unnecessary.
   * 
   * @param lo
   * @param hi
   * @param skipRows
   * @param deepCopy
   */
  LinkedPath(long lo, long hi, ByteBuffer skipRows, boolean deepCopy) {
    Objects.requireNonNull(skipRows, "null skipRows");
  
    List<Long> skipPathNumbers = SkipLedger.skipPathNumbers(lo, hi);
    Row[] rows = new Row[skipPathNumbers.size()];
    final int hashWidth = hashWidth();
    for (int index = 0; index < rows.length; ++index) {
      long rowNumber = skipPathNumbers.get(index);
      int cells = 1 + SkipLedger.skipCount(rowNumber);
      int bytes = cells * hashWidth;
      int limit = skipRows.position() + bytes;
      skipRows.limit(limit);
      rows[index] = deepCopy ? new Row(rowNumber, skipRows) : new Row(rowNumber, skipRows, false);
      skipRows.position(limit);
    }
    
    this.path = Lists.asReadOnlyList(rows);

    MessageDigest digest = newDigest();
    verify(this.path, digest);
  }
  
  
  /**
   * Constructs a new instance. The input <tt>path</tt> is defensively copied.
   * (An instance must guarantee immutable state.)
   * 
   * @param path non-empty list of rows with ascending row numbers, each row linked to
   *  from the next via one of its hash pointers
   */
  public LinkedPath(List<Row> path) {
    Objects.requireNonNull(path, "null path");
    if (path.isEmpty())
      throw new IllegalArgumentException("empth path");
    
    Row[] rows = new Row[path.size()];
    rows = path.toArray(rows);
    
    this.path = Lists.asReadOnlyList(rows);
    
    MessageDigest digest = newDigest();
    verify(this.path, digest);
  }
  
  
  
  /**
   * Package-private constructor. Does not make a copy of the
   * inputs. (public constructors do.)
   * 
   * @param path read-only, immutable list
   */
  LinkedPath(List<Row> path, MessageDigest digest) {
    this.path = Objects.requireNonNull(path, "null path");
    
    if (path.isEmpty())
      throw new IllegalArgumentException("empty path");
    
    verify(this.path, digest);
  }
  
  
  /**
   * Returns the list of rows specifying the unbroken path.
   * 
   * @return non-empty list of rows with ascending row numbers, each successive row linked to
   *  the previous via one of the next row's hash pointers
   */
  public List<Row> path() {
    return path;
  }
  
  
  /**
   * Returns the lowest (first) row number in the list of {@linkplain #path()}.
   */
  public final long loRowNumber() {
    return path.get(0).rowNumber();
  }
  

  /**
   * Returns the highest (last) row number in the list of {@linkplain #path()}.
   */
  public final long hiRowNumber() {
    return path.get(path.size() - 1).rowNumber();
  }


  /**
   * Verifies the skip pointer along the path. Note this does <em>not</em>
   * assume any particular structural path (e.g. V-form). The only requirement
   * for passing verification is that the path be connected.
   * 
   * @param path
   * @param digest
   */
  protected final void verify(List<Row> path, MessageDigest digest) {
    Objects.requireNonNull(digest, "null digest");
    
    // artifact of deciding to reverse the order of the displayed rows
    // (ascending). TODO: rewrite working code :)
    path = Lists.reverse(path);
    
    for (int index = 0, nextToLast = path.size() - 2; index < nextToLast; ++index) {
      Row row = path.get(index);
      Row prev = path.get(index + 1);
      
      if (!Digest.equal(row, prev))
        throw new IllegalArgumentException(
            "digest conflict at index " + index + ": " +
                path.subList(index, index + 2));
      
      long rowNumber = row.rowNumber();
      long prevNumber = prev.rowNumber();
      long deltaNum = rowNumber - prevNumber;
      
      if (deltaNum < 1 || rowNumber % deltaNum != 0)
        throw new IllegalArgumentException(
            "row numbers at index " + index + ": " + path.subList(index, index + 2));
      if (Long.highestOneBit(deltaNum) != deltaNum)
        throw new IllegalArgumentException(
            "non-power-of-2 delta " + deltaNum + " at index " + index + ": " +
                path.subList(index, index + 2));
      
      digest.reset();
      digest.update(prev.data());
      ByteBuffer prevRowHash = ByteBuffer.wrap(digest.digest());
      
      int pointerIndex = Long.numberOfTrailingZeros(deltaNum);
      ByteBuffer hashPointer = row.skipPointer(pointerIndex);
      if (!prevRowHash.equals(hashPointer))
        throw new IllegalArgumentException(
            "hash conflict at index " + index + ": " + path.subList(index, index + 2));
    }
  }


  @Override
  public int hashWidth() {
    return DEF_DIGEST.hashWidth();
  }


  @Override
  public String hashAlgo() {
    return DEF_DIGEST.hashAlgo();
  }
  
  
  /**
   * Determines whether this linked path is a skip path. A skip path describes
   * the shortest possible path connecting the {@linkplain #loRowNumber()} from
   * the {@linkplain #hiRowNumber()}.
   * 
   * @see SkipPath
   */
  public boolean isSkipPath() {
    List<Long> vRowNumbers = SkipLedger.skipPathNumbers(
        path.get(0).rowNumber(),
        path.get(path.size() - 1).rowNumber());
    
    return Lists.map(path, r -> r.rowNumber()).equals(vRowNumbers);
  }
  
  
  
  public boolean intersects(LinkedPath other) {
    if (!Digest.equal(this, other))
      return false;
    
    
    SortedSet<Long> coverage = SkipLedger.coverage(rowNumbers());
    SortedSet<Long> otherCoverage = SkipLedger.coverage(other.rowNumbers());
    
    return Sets.intersect(coverage, otherCoverage);
  }
  
  
  /**
   * Returns the row numbers in the path.
   * 
   * @return non-empty, ascending list of row numbers
   */
  public final List<Long> rowNumbers() {
    return Lists.map(path, r -> r.rowNumber());
  }

}














