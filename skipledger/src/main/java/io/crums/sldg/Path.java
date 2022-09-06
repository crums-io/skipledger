/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

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
public class Path implements Digest {
  
  
  /**
   * The maximum number of rows in a path.
   */
  public final static int MAX_ROWS = 256 * 256;
  
  
  
  private final List<Row> rows;
  
  
  public Path(List<Row> path) throws IllegalArgumentException, HashConflictException {
    this(path, true);
  }
  
  
  
  Path(List<Row> path, boolean copy) {
    
    if (Objects.requireNonNull(path, "null path").isEmpty())
      throw new IllegalArgumentException("empth path");
    else if (path.size() > MAX_ROWS)
      throw new IllegalArgumentException("too many rows: " + path.size());
    
    if (copy) {
      Row[] rows = new Row[path.size()];
      rows = path.toArray(rows);
      this.rows = Lists.asReadOnlyList(rows);
    } else
      this.rows = path;
    
    
    verify();
  }
  
  

  
  
  /**
   * Copy constructor.
   */
  protected Path(Path copy) {
    this.rows = Objects.requireNonNull(copy, "null copy").rows;
  }


  // D I G E S T     M E T H O D S
  
  @Override
  public final int hashWidth() {
    return rows.get(0).hashWidth();
  }

  @Override
  public final String hashAlgo() {
    return rows.get(0).hashAlgo();
  }
  

  
  
  
  
  /**
   * Returns the list of rows specifying the unbroken path.
   * 
   * @return non-empty list of rows with ascending row numbers, each successive row linked to
   *  the previous via one of the next row's hash pointers
   */
  public List<Row> rows() {
    return rows;
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
   * Returns the row numbers in the rows, in the order they occur.
   * 
   * @return non-empty, ascending list of row numbers &ge; 1
   */
  public final List<Long> rowNumbers() {
    return Lists.map(rows, r -> r.rowNumber());
  }
  
  

  
  /**
   * Determines if this path has a (full) row with this row-number (not just a reference to it).
   */
  public final boolean hasRow(long rowNumber) {
    return Collections.binarySearch(rowNumbers(), rowNumber) >= 0;
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
  public final ByteBuffer getRowHash(long rowNumber) throws IllegalArgumentException {
    return
        rowNumber == 0 ?
            sentinelHash() :
              getRowOrReferringRow(rowNumber).hash(rowNumber);
  }
  
  
  
  public Row getRowByNumber(long rowNumber) throws IllegalArgumentException {
    
    final List<Long> rowNumbers = rowNumbers();
    
    int index = Collections.binarySearch(rowNumbers, rowNumber);
    
    if (index < 0)
      throw new IllegalArgumentException("no such row [" + rowNumber + "]");
    
    return rows().get(index);
  }
  
  
  
  /**
   * Returns the row with lowest row-number that knows about the given <code>rowNumber</code>.
   * I.e. if this path instance has a row with the given <code>rowNumber</code> then that row
   * is returned. Otherwise, the <em>first</em> row that references the row with given
   * row-number is returned.
   * 
   * @return a row whose {@linkplain Row#rowNumber() row number} is &ge; <code>rowNumber</code>
   * 
   * @throws IllegalArgumentException if <code>rowNumber</code> is not a member of the
   * set returned by {@linkplain #rowNumbersCovered()}
   * 
   * @see #getRowHash(long)
   */
  public final Row getRowOrReferringRow(long rowNumber) throws IllegalArgumentException {
    
    final List<Long> rowNumbers = rowNumbers();
    {
      int index = Collections.binarySearch(rowNumbers, rowNumber);
      
      if (index >= 0)
        return rows().get(index);
      
      else if (-1 - index == rowNumbers.size())
        throw new IllegalArgumentException("rowNumber " + rowNumber + " not covered");
    }

    final int pointerLevels = SkipLedger.skipCount(rowNumber);
    
    for (int exp = 0; exp < pointerLevels; ++exp) {
      
      long referrerNum = rowNumber + (1L << exp);
      int index = Collections.binarySearch(rowNumbers, referrerNum);
      
      if (index >= 0) {
        Row row = rows.get(index);
        assert row.prevLevels() > exp;
        return row;
        
      } else if (-1 - index == rowNumbers.size())
        break;
    }
    
    
    throw new IllegalArgumentException("rowNumber " + rowNumber + " not covered");
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private void verify() {

    MessageDigest digest = rows.get(0).newDigest();

    // artifact of deciding to reverse the order of the displayed rows
    List<Row> rpath = Lists.reverse(this.rows);
    
    for (int index = 0, nextToLast = rpath.size() - 2; index < nextToLast; ++index) {
      Row row = rpath.get(index);
      Row prev = rpath.get(index + 1);
      
      if (!Digest.equal(row, prev))
        throw new IllegalArgumentException(
            "digest conflict at index " + index + ": " +
                rpath.subList(index, index + 2));
      
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
























