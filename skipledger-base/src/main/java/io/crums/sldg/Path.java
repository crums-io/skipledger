/*
 * Copyright 2020-2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.DIGEST;
import static java.util.Collections.binarySearch;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import io.crums.util.Lists;
import io.crums.util.Sets;

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
 * <h2>FIXME</h2>
 * <p>
 * The protocol for what hash pointer information is requried for the <em>first</em>
 * row in a <em>condensed</em> path must change: presently this requires a hash pointer to
 * the adjacent (previous) row, and a funnel (from which the levels pointer is
 * calculated). Instead, we should allow modeling a condensed row using its
 * levels-pointer and input hashes alone. Presently, methods like
 * </p>
 * <ul>
 * <li>{@linkplain #subPath(int, int)}</li>
 * <li>{@linkplain #headPath(long)}</li>
 * <li>{@linkplain #tailPath(long)}</li>
 * <li>{@linkplain #nosCovered()}</li>
 * </ul>
 * <p>are broken for condensed instances. The following methods are
 * also broken with condensed instances:</p>
 * <ul>
 * <li>{@linkplain #getRowOrReferringRow(long)}</li>
 * <li>{@linkplain #getRowHash(long)}</li>
 * </ul>
 * 
 */
public class Path {
  
  
  /**
   * The maximum number of rows in a path.
   * <p>
   * TODO: remove this is a silly and arbitrary restriction.
   * (Yes, instances are typically small, but I'm already finding application
   * for large instances: {@linkplain #skipPath(boolean, Long...)} was created
   * cuz a Path can serve as a client-side repo for ledger state.)
   * </p>
   */
  public final static int MAX_ROWS = 256 * 256;
  
  
  
  private final List<Row> rows;
  
  
  public Path(List<Row> path) throws IllegalArgumentException, HashConflictException {
    if (path.isEmpty())
      throw new IllegalArgumentException("empth path");
    else if (path.size() > MAX_ROWS)
      throw new IllegalArgumentException("too many rows: " + path.size());

    this.rows = List.copyOf(path);
    
    verify();
  }
  
  
  
  
  Path(List<Row> rows, Object trustMe) {
    this.rows = Objects.requireNonNull(rows);
  }

  
  
  /**
   * Copy constructor.
   */
  protected Path(Path copy) {
    this.rows = copy.rows;
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
    return first().no();
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
    return last().no();
  }
  
  
  /**
   * Determines whether this is a skip path. A skip path describes
   * the shortest possible list of rows connecting the {@linkplain #hi()} to
   * the {@linkplain #lo()} numbered rows.
   */
  public final boolean isSkipPath() {
    List<Long> vRowNumbers = SkipLedger.skipPathNumbers(lo(), hi());
    
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
   * Tests whether the path is condensed. A path is considered condensed
   * if <em>any</em> of its rows are condensed. Usually, to be
   * {@linkplain #isCompressed() compressed}, a path must also be be
   * condensed.
   */
  public final boolean isCondensed() {
    int index = rows.size();
    while (index-- > 0 && !rows.get(index).isCondensed());
    return index != -1;
  }



  /**
   * Tests whether the path is compressed. A path is compressed
   * <b>iff</b> <em>all</em> its rows are compressed.
   * 
   * @see Row#isCompressed()
   * @see #isCondensed()
   * @see #compress()
   */
  public final boolean isCompressed() {
    int index = rows.size();
    while (index-- > 0 && rows.get(index).isCompressed());
    return index == -1;
  }




  /**
   * Returns a {@linkplain #isCompressed() compressed} version of this
   * instance; or {@code this} instance, if already compressed.
   */
  public final Path compress() {
    if (isCompressed())
      return this;
    
    Row[] crows = new Row[rows.size()];
    crows[0] = CondensedRow.compressToLevelRowNo(first(), first().no() - 1);
    for (int index = 1; index < crows.length; ++index)
      crows[index] =
          CondensedRow.compressToLevelRowNo(
              rows.get(index),
              rows.get(index - 1).no());
    
    // FIXME: uncomment after adequately tested..
    // return new Path(Lists.asReadOnlyList(crows), null);
    return new Path(Lists.asReadOnlyList(crows));
  }

  


  /**
   * Returns a subsequence of this path. This is equivalent to
   * returning {@code new Path(rows().subList(fromIndex, toIndex))}
   * --sans the overhead for checking row hashes.
   * <p>
   * Note the first row in the returned subpath must reference the
   * previous row (thru its hash). If that first row is <em>not</em>
   * condensed (or if this entire path is not {@linkplain #isCondensed()
   * condensed}), then this method always succeeds; otherwise, if
   * condensed, the first row in the returned subpath must be condensed
   * at the zero-th level (i.e. it must explicitly reference the hash of its
   * immediate predecessor row.)
   * </p>
   * 
   * @param fromIndex   the starting index (inclusive)
   * @param toIndex     the ending index (exclusive)
   * 
   * @throws IndexOutOfBoundsException
   *         beyond the usual requirements, the subpath cannot be empty
   * @throws IllegalArgumentException
   *         if the first row in the to-be-returned subpath, does not
   *         reference the row immediately before it
   */
  public final Path subPath(int fromIndex, int toIndex) {
    final int rc = rows.size();
    if (fromIndex < 0 || fromIndex >= toIndex || toIndex > rc)
      throw new IndexOutOfBoundsException(
          "fromIndex (" + fromIndex + "), toIndex (" + toIndex + "); size = " +
          rc);

    // verify the first row in the subpath references the previous
    // row no.
    Row subFirst = rows.get(fromIndex);
    long sfRn = subFirst.no();
    if (!subFirst.levelsPointer().coversRow(sfRn - 1))
      throw new IllegalArgumentException(
          "first subpath row [" + sfRn+ "] is condensed: " +
          subFirst.levelsPointer() + "; previous row [" + (sfRn - 1) +
          " must be covered");

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
    int index = indexOf(rn);
    if (index < 0) {
      index = -1 - index;
      if (index == rows.size())
        throw new IllegalArgumentException(
          "[" + rn + "] out of range [" + loRowNumber() + "-" + hiRowNumber() +
          "]");
    }
    return subPath(index);
  }


  private int indexOf(long rn) {
    return binarySearch(rowNumbers(), rn);
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
    int index = indexOf(rn);
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
   * intersect, zero is returned. The given path must be from the same ledger.
   * <p>
   * The relationship is <em>symmetric</em>: i.e. <br/>
   * {@code this.highestCommonNo(other) == other.highestCommonNo(this)}
   * </p>
   * 
   * @throws HashConflictException
   *         if the row hashes conflict at this highest common row no.
   */
  public final long highestCommonNo(Path other) throws HashConflictException {
    return highestCommonImpl(other, Path::nosCovered);
  }


  /**
   * Returns the highest common <em>full</em> row number in this and the
   * other instance, or zero if there is no such row. The 2 paths are
   * assumed to be from the same ledger, and if their row hashes conflict at
   * this highest common row no., then a {@code HashConflictException} is
   * thrown.
   * <p>
   * The relationship is <em>symmetric</em>: i.e. <br/>
   * {@code this.highestCommonFullNo(other) == other.highestCommonFullNo(this)}
   * </p>
   * 
   * @throws HashConflictException
   *         if the row hashes conflict at this highest common full row no.
   */
  public final long highestCommonFullNo(Path other) throws HashConflictException {
    return highestCommonImpl(other, Path::nos);
  }


  /** Returns the highest row no. in the intersection of the no.s generated. */
  private long highestCommonImpl(
    Path other, Function<Path, Collection<Long>> rowNoFunc) {

    final long hiNo;

    // FIXME
    // there are way faster (but more complicated) ways to compute hiNo
    // (since the collection is always sorted). Punting for now..
    {
      var nosA = rowNoFunc.apply(this);
      var nosB = rowNoFunc.apply(other);

      // pick the smallest collection, first
      if (nosA.size() > nosB.size()) {
        var temp = nosA;
        nosA = nosB;
        nosB = temp;
      }

      Set<Long> intersect = HashSet.newHashSet(nosA.size());
      intersect.addAll(nosA);
      intersect.retainAll(nosB);
      hiNo = intersect.stream().max(Long::compare).orElse(0L);
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
    return Lists.map(rows, Row::no);
  }
  
  

  
  /**
   * Determines if this path has a (full) row with this row-number (not just a reference to it).
   */
  public final boolean hasRow(long rowNumber) {
    return indexOf(rowNumber) >= 0;
  }
  

  /**
   * Determines whether this path references (knows the hash of) the given row no.
   */
  public final boolean hasRowCovered(long rowNo) {
    if (rowNo == 0L)
      return true;
    
    int index = indexOf(rowNo);
    if (index >= 0)
      return true;
    index = -1 - index;
    if (index == rows.size())
      return false;

    final long maxRefRn = rowNo + (1L << (SkipLedger.skipCount(rowNo) - 1));
    for (; index < rows.size(); ++index) {
      Row row = rows.get(index);
      if (row.no() > maxRefRn)
        break;
      if (row.levelsPointer().coversRow(rowNo))
        return true;
    }
    return false;
    
  }



  
  
  /**
   * Returns all the row numbers referenced in this path. This collection includes
   * the path's {@linkplain #rowNumbers() row numbers} as well other rows referenced thru the rows'
   * skip pointers.
   * <p>The upshot of this accounting is that the hash of every row number returned
   * is known by the instance. <em>Note, this may contain zero, the sentinel row.</em>
   * </p>
   * 
   * @see #getRowHash(long)
   */
  public final SortedSet<Long> nosCovered() {
    if (!isCondensed())
      return SkipLedger.coverage(rowNumbers());

    var coveredRns = new TreeSet<Long>();
    for (var row : rows) {
      coveredRns.addAll(row.levelsPointer().coverage());
      coveredRns.add(row.no());
    }
    return Collections.unmodifiableSortedSet(coveredRns);
  }

  /**
   * @deprecated too wordy..
   * @see #nosCovered()
   */
  public final SortedSet<Long> rowNumbersCovered() {
    return nosCovered();
  }

  
  
  /**
   * Returns the hash of the row with the given {@code rowNo}. This is not
   * just a convenience for getting the {@linkplain Row#hash() hash} of one of the
   * instance's {@linkplain #rows() rows}; it also provides a more sophisticated
   * algorithm so that it can return the hash of <em>any</em> row returned by
   * {@linkplain #nosCovered()}.
   * 
   * @throws IllegalArgumentException if {@code rowNo} is not a member of the
   * set returned by {@linkplain #nosCovered()}
   */
  public ByteBuffer getRowHash(long rowNo) throws IllegalArgumentException {
    
    return
        rowNo == 0L ?
            SldgConstants.DIGEST.sentinelHash() :
              getRowOrReferringRow(rowNo).hash(rowNo);
  }
  
  
  
  public Row getRowByNumber(long rowNumber) throws IllegalArgumentException {
    int index = indexOf(rowNumber);
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
   * @return a row whose {@linkplain Row#no() row no.} is &ge; <code>rowNo</code>
   * 
   * @throws IllegalArgumentException if <code>rowNumber</code> is not a member of the
   * set returned by {@linkplain #rowNumbersCovered()}
   * 
   * @see #getRowHash(long)
   */
  public Row getRowOrReferringRow(final long rowNo) throws IllegalArgumentException {
    
    final int insertIndex;
    {
      int index = indexOf(rowNo);
      
      if (index >= 0)
        return rows().get(index);
      
      insertIndex = -1 - index;
    }
    
    final long maxRefRn = rowNo + (1L << (SkipLedger.skipCount(rowNo) - 1));
    
    for (int index = insertIndex; index < rows.size(); ++index) {
      Row row = rows.get(index);
      if (row.no() > maxRefRn)
        break;
      
      if (row.levelsPointer().coversRow(rowNo))
        return row;
    }
    
    throw new IllegalArgumentException(
        "row [" + rowNo + "] not covered: " + this);
  }
  
  
  /** Returns the number of rows. */
  public final int length() {
    return rows.size();
  }


  /**
   * Returns the row numbers in ascending order.
   * 
   * @return immutable, <em>value</em>-based list
   */
  public final List<Long> nos() {
    return Lists.map(rows, Row::no);
  }


  /** Returns the lowest (first / minimum) full row no. */
  public final long lo() {
    return first().no();
  }


  /** Returns the highest (last / maximum) full row no. */
  public final long hi() {
    return last().no();
  }



  /**
   * Returns a <em>skip path</em> version of this instance, or this
   * instance if already a {@linkplain #isSkipPath() skip path}.
   */
  public final Path skipPath() {
    final int len = length();
    if (len <= 2)
      return this;
    var rowNos = SkipLedger.skipPathNumbers(lo(), hi());
    return
        rowNos.size() == length() ? this :
            new Path(Lists.map(rowNos, this::getRowByNumber), null);
    
  }


  /**
   * Returns a minumum {@linkplain #length() length} instance connecting the
   * first row, the given target rows, and the last row.
   * 
   * @param targets   target row no.s
   * 
   * @return  empty, if this instance does not have sufficient info about the
   *          given target row no.s
   */
  public final Optional<Path> skipPath(Long... targets) {
    return skipPath(false, targets);
  }


  /**
   * Returns a minumum {@linkplain #length() length} connecting the
   * the given target rows.
   * 
   * @param trim      if {@code false}, then the {@linkplain #lo() lo} and
   *                  {@linkplain #hi() hi} row no.s are automatically
   *                  included as targets
   * 
   * @param targets   target row no.s
   * 
   * @return  empty, if this instance does not have sufficient info about the
   *          given target row no.s
   */
  public final Optional<Path> skipPath(boolean trim, Long... targets) {
    var sorted = new TreeSet<>(Arrays.asList(targets));
    if (!Sets.sortedSetView(nos()).containsAll(sorted))
      return Optional.empty();

    if (!trim) {
      sorted.add(lo());
      sorted.add(hi());
    }
    List<Long> rowNos = SkipLedger.stitchSet(sorted);
    if (rowNos.size() == length())
      return Optional.of(this);
    
    List<Row> rows = Lists.map(rowNos, this::getRowByNumber);
    return Optional.of(new Path(rows, null));
  }



  /**
   * Appends the given path to the tail end of this path and returns it.
   * The given {@code tail} path, must <em>cover</em> (know the hash of)
   * the highest numbered row in this path.
   * 
   * @param tail  from same ledger, and covers the highest row in <em>this</em>
   *              path
   * @return  an appended version of this instance , or {@code this} if
   *          {@code tail.hi() == hi()})
   * 
   * @throws HashConflictException
   *          if the hash of the last row in this instance conflicts with
   *          the hash of the same numbered row in {@code tail}
   * 
   * @see #hasRowCovered(long)
   */
  public Path appendTail(Path tail) {
    final long hiRn = hi();
    if (!tail.hasRowCovered(hiRn))
      throw new IllegalArgumentException(
        "hi row [" + hiRn + "] is not covered: " + tail);

    if (!tail.getRowHash(hiRn).equals(getRowHash(hiRn)))
      throw new HashConflictException("at hi row [" + hiRn + "]");

    if (tail.hi() == hiRn)
      return this;

    return new Path(
        List.copyOf(Lists.concat(rows, tail.tailPath(hiRn + 1).rows)),
        null);

    // we've already verified the hash at hiRn, so the following
    // validating constructor is unnecessary..
    //
    // return new Path(Lists.concat(rows, tail.tailPath(hiRn + 1).rows));
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
  
  
  
  @Override
  public String toString() {
    var rns = rowNumbers();
    final int size = rns.size();
    if (size > 12) {
      var str = "Path" + rns.subList(0, 6);
      str = str.substring(0, str.length() - 1) +
          ", .. (" + (size - 12) + " more), " +
          rns.subList(size - 6, size).toString().substring(1);

      return str;
    }
    return "Path" + rowNumbers();
  }
  
  
  
  
  
  
  
  
  private void addRefs(Row row, HashMap<Long,ByteBuffer> rowHashes) {
    final long rn = row.no();
    addRef(rn, row.hash(), rowHashes, true);
    if (rn == 1L)
      return;

    final boolean validImpl;
    
    if (!row.isCondensed()) {
      final int levels = row.prevLevels();
      for (int lv = levels; lv-- > 0; )
        addRef(rn - (1L << lv), row.prevHash(lv), rowHashes);

      

      validImpl =
          SkipLedger.rowHash(
              rn,
              row.inputHash(),
              row.levelHashes()).equals(row.hash());
    } else {
  
      var levelsPtr = row.levelsPointer();

      // check for hash conflicts (we expect *not to add anything)
      boolean added =
          addRef(
              rn - (1L << levelsPtr.level()),
              levelsPtr.levelHash(),
              rowHashes);

      assert !added;  // this shoulda been filtered earlier, where we could
                      // report the prev row no.

      validImpl = 
          SkipLedger.rowHash(
              row.inputHash(),
              levelsPtr.hash()).equals(row.hash());
    }


    if (!validImpl)
      throw new HashConflictException(
          "invalid hash at [" + rn + "]. Implementation class: " +
          row.getClass().getName());
  }
  



  /** @see #addRef(long, ByteBuffer, HashMap, boolean) */
  private boolean addRef(
      long no, ByteBuffer hash, HashMap<Long,ByteBuffer> rowHashes) {
    return addRef(no, hash, rowHashes, false);
  }
  
  /**
   * Puts the given {@code (no, hash)} to the {@code rowHashes} map.
   * 
   * @param assertUnknown if {@code true}, then assert that there
   *                      is no entry for that row {@code no}
   * @return {@code true} if a new entry was added
   * @throw HashConflictException if {@code hash} conflicts with an
   *                      entry in the {@code rowHashes} map
   */
  private boolean addRef(
      long no, ByteBuffer hash, HashMap<Long,ByteBuffer> rowHashes,
      boolean assertUnknown) {

    var known = rowHashes.put(no, hash);
    if (known == null)
      return true;
    
    if (!known.equals(hash))
      throw new HashConflictException(
          "at row [" + no + "]: expected hash <" + known +
          ">; actual <" + hash + ">; (assertUnknown=" + assertUnknown + ")");

    // if we were expecting known == null, then the following assertion fails
    // .. even tho the hashes agreed
    assert !assertUnknown;
    return false;
  }
  
  
  
  private void verify() {

    if (rows.get(0).no() <= 0L)
      throw new IllegalArgumentException(
          "illegal row no. at index [0]: " + rows.get(0).no());

    HashMap<Long,ByteBuffer> rowHashes = new HashMap<>();
    rowHashes.put(0L, DIGEST.sentinelHash());

    long prevNo = rows.get(0).no() - 1L;
    for (int index = 0; index < rows.size(); ++index) {
      final Row row = rows.get(index);
      final long rn = row.no();
      if (rn <= prevNo)
        throw new IllegalArgumentException(
            "out-of-sequence rows after index [" + (index - 1) + "]: " +
            prevNo + ", " + rn);

      if (!SkipLedger.rowsLinked(prevNo, rn))
        throw new IllegalArgumentException(
            "unlinked rows after index (" + (index - 1) +
            "): " + prevNo + ", " + rn);
      
      if (row.isCondensed()) {
        var levelsPtr = row.levelsPointer();
        if (prevNo + (1L << levelsPtr.level()) != rn)
          throw new IllegalArgumentException(
            "condensed row [" + rn + "] with level index <" + levelsPtr.level() +
            "> does not link with row [" + prevNo + "]");
      }

      addRefs(row, rowHashes);
      
      prevNo = rn;

    }
  }
  
}
























