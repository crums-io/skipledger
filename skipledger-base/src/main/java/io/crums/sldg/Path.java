/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.Lists;
import io.crums.util.hash.Digest;

/**
 * A list of connected rows in a {@linkplain SkipLedger} with strictly ascending row numbers.
 * This describes a hash proof that the specified rows are indeed from the same ledger with strong
 * constraints on the possible row numbers.
 * 
 * <h2>Validation Guarantee</h2>
 * <p>
 * Every instance's hash pointers are checked for validity at construction. Furthermore,
 * every public constructor makes a defensive copy of its row inputs. The upshot of this
 * guarantee is that a reference to an instance at runtime, is a reference to an immutable proof.
 * </p><p>
 * An instance's hashing algorithm ({@linkplain Digest} implementation) is delegated to
 * its {@linkplain Row}s.
 * </p>
 * <h2>Skip Paths</h2>
 * <p>
 * The list of rows linking the highest-numbered row to the lowest-numbered row in a path
 * is not unique. It may be a small or large list. However any path with a large list of rows
 * can be reduced to an equivalent minimal subset of its rows that connect the high and low numbered
 * rows in the path. This minimal length path is called a <em>skip path</em>.
 * </p><p>
 * (Constructing an abstract skip path from a high row number to low row number is straight
 * forward: at each step (row) you link to the lowest numbered row your row links to, while minding
 * not to skip beyond the target low-numbered row.)
 * </p>
 * <h2>Performance Note</h2>
 * <p>
 * Instances are <em>small</em> objects. Self reminder: there's little point in optimization.
 * Serialization footprint, maybe; clock-cycles NO(!).
 * </p>
 */
public class Path {
  
  
  /**
   * The maximum number of rows in a path.
   */
  public final static int MAX_ROWS = 256 * 256;
  
  
  
  private final List<Row> rows;
  
  
  public Path(List<Row> path) throws IllegalArgumentException, HashConflictException {
    if (path.isEmpty())
      throw new IllegalArgumentException("empth path");
    else if (path.size() > MAX_ROWS)
      throw new IllegalArgumentException("too many rows: " + path.size());

    Row[] rows = new Row[path.size()];
    rows = path.toArray(rows);
    this.rows = Lists.asReadOnlyList(rows);
    
    verify();
  }
  
  
  
  
  Path(List<Row> rows, Object trustMe) {
    this.rows = Objects.requireNonNull(rows);
  }

  
  
  /**
   * Copy constructor.
   */
  protected Path(Path copy) {
    this.rows = Objects.requireNonNull(copy, "null copy").rows;
  }

  

  
  
  
  
  /**
   * Returns the list of rows specifying the unbroken path.
   * 
   * @return non-empty list of rows with ascending row numbers, each successive row linked to
   *  the previous via one of the next row's hash pointers
   */
  public final List<Row> rows() {
    return rows;
  }
  
  

  /**
   * Returns this instance as a {@code PathPack}. The returned
   * instance is not necessarily memo-ised.
   * 
   * @see MemoPathPack
   */
  public PathPack pack() {
    return PathPack.forPath(this);
  }
  
  
  
  
  /**
   * Returns the lowest (first) row number in the list of {@linkplain #rows()}.
   */
  public final long loRowNumber() {
    return first().rowNumber();
  }
  
  
  /**
   * Returns the first row.
   */
  public final Row first() {
    return rows.get(0);
  }
  
  
  /**
   * Returns the last row.
   */
  public final Row last() {
    return rows.get(rows.size() - 1);
  }

  /**
   * Returns the highest (last) row number in the list of {@linkplain #rows()}.
   */
  public final long hiRowNumber() {
    return last().rowNumber();
  }
  
  
  /**
   * Determines whether this is a skip path. A skip path describes
   * the shortest possible list of rows connecting the {@linkplain #hiRowNumber()} to
   * the {@linkplain #loRowNumber()}.
   */
  public final boolean isSkipPath() {
    List<Long> vRowNumbers = SkipLedger.skipPathNumbers(loRowNumber(), hiRowNumber());
    
    // skip paths are *minimum length paths that are unique
    // therefore there is no need to check the row numbers individually
    // as in say Lists.map(rows, r -> r.rowNumber()).equals(vRowNumbers);
    // just their sizes
    //
    // TODO: Document proof of above statement: structural uniqueness of minimum path
    //       ..and not in code (which is how I know this)
    
    return vRowNumbers.size() == rows.size();
  }
  
  


  /**
   * Returns a subsequence of this path. This is equivalent to
   * returning {@code new Path(rows().subList(fromIndex, toIndex))}
   * --sans the overhead for checking row hashes.
   * 
   * @param fromIndex   the starting index (inclusive)
   * @param toIndex     the ending index (exclusive)
   * 
   * @throws IndexOutOfBoundsException
   *         beyond the usual requirements, the subpath cannot be empty
   */
  public final Path subPath(int fromIndex, int toIndex) {
    final int rc = rows.size();
    if (fromIndex < 0 || fromIndex >= toIndex || toIndex > rc)
      throw new IndexOutOfBoundsException(
          "fromIndex (" + fromIndex + "), toIndex (" + toIndex + "); size = " +
          rc);

    return new Path(rows.subList(fromIndex, toIndex), null);
  }


  /** Shorthand for {@link #subPath(int, int) subpath(fromIndex, rows().size())}. */
  public final Path subPath(int fromIndex) {
    return fromIndex == 0 ? this : subPath(fromIndex, rows.size());
  }



  /**
   * Returns the longest subsequence of this path such that the first row
   * is numbered no less than {@code rn}.
   * 
   * @param rn    row number (inclusive)
   * 
   * @throws IllegalArgumentException  if {@code rn > hiRowNumber()}
   */
  public final Path tailPath(long rn) {
    int index = Collections.binarySearch(rowNumbers(), rn);
    if (index < 0) {
      index = -1 - index;
      if (index == rows.size())
        throw new IllegalArgumentException(
          "[" + rn + "] out of range [" + loRowNumber() + "-" + hiRowNumber() +
          "]");
    }
    return subPath(index);
  }



  /**
   * Returns the longest subsequence of this path such that the last row
   * is numbered less than {@code rn}.
   * 
   * @param rn    row number (exclusive)
   * 
   * @throws IllegalArgumentException  if {@code rn <= loRowNumber()}
   */
  public final Path headPath(long rn) {
    int index = Collections.binarySearch(rowNumbers(), rn);
    if (index < 0) {
      index = -1 - index;
      if (index == rows.size())
        return this;
    }
    if (index == 0)
      throw new IllegalArgumentException(
        "[" + rn + "] out of range [" + loRowNumber() + "-" + hiRowNumber() +
        "]");
    
    return subPath(0, index);
  }


  


  /**
   * Returns the highest row number whose hash is known by both this and the
   * {@code other} instance (from the same ledger). If the 2 paths do not
   * intersect, zero is returned.
   * 
   * @throws HashConflictException if the row hashes conflict at that row number
   */
  public final long highestCommonNo(Path other) throws HashConflictException {
    
    final long hiNo;
    {
      var intersect = new TreeSet<Long>();
      intersect.addAll(rowNumbersCovered());
      intersect.retainAll(other.rowNumbers());
      if (intersect.isEmpty())
        return 0L;
      hiNo = intersect.last();
    }

    if (hiNo != 0L && !getRowHash(hiNo).equals(other.getRowHash(hiNo)))
      throw new HashConflictException("at [" + hiNo + "]");

    return hiNo;
  }



  
  
  /**
   * Returns the row numbers in the rows, in the order they occur.
   * 
   * @return non-empty, ascending list of row numbers &ge; 1
   */
  public final List<Long> rowNumbers() {
    return Lists.map(rows, Row::rowNumber);
  }
  
  

  
  /**
   * Determines if this path has a (full) row with this row-number (not just a reference to it).
   */
  public boolean hasRow(long rowNumber) {
    return Collections.binarySearch(rowNumbers(), rowNumber) >= 0;
  }
  

  /**
   * Determines whether this path references (knows the hash) of the given row number.
   */
  public boolean hasRowCovered(long rowNumber) {
    if (rowNumber == 0)
      return true;
      
    var rns = rowNumbers();
    final int rnCount = rns.size();
    
    int searchIndex = Collections.binarySearch(rns, rowNumber);
    if (searchIndex >= 0)
      return true;
    if (-1 - searchIndex == rnCount)
      return false;
    
    final int rnLevels = SkipLedger.skipCount(rowNumber);
    for (int level = 0; level < rnLevels; ++level) {
      long rn = rowNumber + (1L << level);
      searchIndex = Collections.binarySearch(rns, rn);
      if (searchIndex >= 0)
        return true;
      if (-1 - searchIndex == rnCount)
        return false;
    }
    return false;
  }
  
  
  /**
   * Returns all the row numbers referenced in this path. This collection includes
   * the path's {@linkplain #rowNumbers() row numbers} as well other rows referenced thru the rows'
   * skip pointers.
   * <p>The upshot of this accounting is that the hash of every row number returned
   * is known by the instance. 
   * </p>
   * 
   * @see #getRowHash(long)
   */
  public final SortedSet<Long> rowNumbersCovered() {
    return SkipLedger.coverage(rowNumbers());
  }
  
  
  /**
   * Returns the hash of the row with the given <code>rowNumber</code>. This is not
   * just a convenience for getting the {@linkplain Row#hash() hash} of one of the
   * instance's {@linkplain #rows() rows}; it also provides a more sophisticated
   * algorithm so that it can return the hash of <em>any</em> row returned by
   * {@linkplain #rowNumbersCovered()}.
   * 
   * @throws IllegalArgumentException if <code>rowNumber</code> is not a member of the
   * set returned by {@linkplain #rowNumbersCovered()}
   */
  public ByteBuffer getRowHash(long rowNumber) throws IllegalArgumentException {
    return
        rowNumber == 0 ?
            SldgConstants.DIGEST.sentinelHash() :
              getRowOrReferringRow(rowNumber).hash(rowNumber);
  }
  
  
  
  public Row getRowByNumber(long rowNumber) throws IllegalArgumentException {
    
    final List<Long> rowNumbers = rowNumbers();
    
    int index = Collections.binarySearch(rowNumbers, rowNumber);
    
    if (index < 0)
      throw new IllegalArgumentException("no such row [" + rowNumber + "]");
    
    return rows.get(index);
  }
  
  
  
  /**
   * Returns the row with lowest row-number that knows about the given <code>rowNo</code>.
   * I.e. if this path instance has a row with the given <code>rowNo</code> then that row
   * is returned. Otherwise, the <em>first</em> row that references the row with given
   * row-number is returned.
   * 
   * @return a row whose {@linkplain Row#rowNumber() row number} is &ge; <code>rowNo</code>
   * 
   * @throws IllegalArgumentException if <code>rowNumber</code> is not a member of the
   * set returned by {@linkplain #rowNumbersCovered()}
   * 
   * @see #getRowHash(long)
   */
  public Row getRowOrReferringRow(final long rowNo) throws IllegalArgumentException {
    
    final List<Long> rowNos = rowNumbers();
    final int insertIndex;
    {
      int index = Collections.binarySearch(rowNos, rowNo);
      
      if (index >= 0)
        return rows().get(index);
      
      insertIndex = -1 - index;
    }
    
    final long maxRefRn = rowNo + (1L << (SkipLedger.skipCount(rowNo) - 1));
    
    for (int index = insertIndex; index < rowNos.size(); ++index) {
      long rn = rows.get(index).rowNumber();
      if (SkipLedger.rowsLinked(rowNo, rn))
        return rows.get(index);
      
      if (rn > maxRefRn)
        break;
    }
    
    throw new IllegalArgumentException(
        "row [" + rowNo + "] not covered: " + this);
  }
  
  
  /** Returns the number of rows. */
  public int length() {
    return rows.size();
  }
  
  
  
  
  /**
   * Instances are equal if they have the same rows. This only needs
   * to verify 2 paths have the same row numbers and that the
   * hash of their <em>last</em> rows are the same.
   */
  @Override
  public final boolean equals(Object o) {
    return
        o == this ||
        o instanceof Path other
        && rows.size() == other.rows.size()
        && loRowNumber() == other.loRowNumber()
        && last().equals(other.last())
        // this last check is also necessary
        && rowNumbers().equals(other.rowNumbers());
  }
  
  
  
  /**
   * Consistent with {@link #equals(Object)}.
   */
  @Override
  public final int hashCode() {
    long lhash = hiRowNumber();
    lhash = lhash * 255 + rows.size();
    int cryptFuzz = last().hash().getInt();
    return Long.hashCode(lhash) ^ cryptFuzz;
  }
  
  
  
  
  public String toString() {
    return "Path" + rowNumbers();
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private void verify() {

    var digest = SldgConstants.DIGEST.newDigest();

    // artifact of deciding to reverse the order of the displayed rows
    List<Row> rpath = Lists.reverse(this.rows);
    
    for (int index = 0, nextToLast = rpath.size() - 2; index < nextToLast; ++index) {
      Row row = rpath.get(index);
      Row prev = rpath.get(index + 1);
      
//      if (!Digest.equal(row, prev))
//        throw new IllegalArgumentException(
//            "digest conflict at index " + index + ": " +
//                rpath.subList(index, index + 2));
      
      long rowNumber = row.rowNumber();
      long prevNumber = prev.rowNumber();
      long deltaNum = rowNumber - prevNumber;
      
      if (deltaNum < 1 || rowNumber % deltaNum != 0)
        throw new IllegalArgumentException(
            "row numbers at index " + index + ": " + rpath.subList(index, index + 2));
      if (Long.highestOneBit(deltaNum) != deltaNum)
        throw new IllegalArgumentException(
            "non-power-of-2 delta " + deltaNum + " at index " + index + ": " +
                rpath.subList(index, index + 2));
      
      digest.reset();
      digest.update(prev.data());
      ByteBuffer prevRowHash = ByteBuffer.wrap(digest.digest());
      
      int pointerIndex = Long.numberOfTrailingZeros(deltaNum);
      ByteBuffer hashPointer = row.prevHash(pointerIndex);
      if (!prevRowHash.equals(hashPointer))
        throw new HashConflictException(
            "hash conflict at index " + index + ": " + rpath.subList(index, index + 2));
    }
  }
  
}
























