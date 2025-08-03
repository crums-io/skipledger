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
   * {@code this.highestCommonNo(other) == other.highestCommonNo(this)} <br/>
   * always evaluates to {@code true}.
   * </p>
   * 
   * @throws HashConflictException
   *         if the row hashes conflict at this highest common row no.
   * 
   * @see #forkedNo(Path)
   * @see #highestCommonOrForkNo(Path)
   * @see #highestCommonFullNo(Path)
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
   *         
   * @see #forkedNo(Path)
   */
  public final long highestCommonFullNo(Path other) throws HashConflictException {
    return highestCommonImpl(other, Path::nos);
  }


  /**
   * Returns the highest row no. in the intersection of the no.s generated.
   * 
   * @param rowNoFunc either {@code Path::nos} or {@code Path::nosCovered}
   */
  private long highestCommonImpl(
    Path other, Function<Path, Collection<Long>> rowNoFunc) {
    
    long hiNo;
    {
      long hi = hi();
      if (hi > other.hi())
        return other.highestCommonImpl(this, rowNoFunc);
      
      if (other.hasRowCovered(hi))
        hiNo = hi;
      else 
        hiNo =
            commonNos(other, rowNoFunc).stream().max(Long::compare).orElse(0L);
    }
    
    if (hiNo > 0L && !other.getRowHash(hiNo).equals(getRowHash(hiNo)))
      throw new HashConflictException("at row [" + hiNo + "]");

    return hiNo;
  }
  
  
  
  private Set<Long> commonNos(
      Path other, Function<Path, Collection<Long>> rowNoFunc) {
    
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
    return intersect;
  }
  
  
  
  
  /**
   * Compares this path with the {@code other} and returns the <em>first</em>
   * (lowest) row number whose hashes differ (conflict), if any. This propery is
   * <em>symmetric</em>. I.e. for any 2 instances {@code a} and {@code b} the
   * expression <br/>
   * {@code a.forkNo(b).equals(b.forkNo(a))} <br/>
   * evaluates to {@code true}.
   * 
   * @param other a path, possibly from another ledger
   * 
   * @return the first row no. the 2 paths conflict; empty, if both paths are
   *         in agreement (whether because they don't intersect, or because
   *         the row-hashes at the highest common covered no. are equal)
   *         
   * @see #highestCommonOrForkNo(Path)
   */
  public final Optional<Long> forkedNo(Path other) {
    long cof = highestCommonOrForkNo(other);
    return cof < 0L ? Optional.of(-cof) : Optional.empty();
  }


  /**
   * Compares this path with the {@code other} and returns the <em>highest</em>
   * intersecting row number iff the row-hashes are equal at that no.;
   * otherwise the <em>lowest</em> intersecting row number at which the
   * row-hashes disagree (not equal) is <em>returned in the negative</em>.
   * <p>
   * This property is <em>symmetric</em>.
   * </p>
   * 
   * @see #forkedNo(Path)
   * @see #highestCommonNo(Path)
   */
  public final long highestCommonOrForkNo(Path other) {
    final long hi = hi();
    if (hi > other.hi())
      return other.highestCommonOrForkNo(this);
    
    // assert hi <= other.hi();
    
    if (other.hasRowCovered(hi) &&
        last().hash().equals(other.getRowHash(hi)))
      return hi;
    
    
    
    var coveredNos = commonNos(other, Path::nosCovered);
    
    long hiNo = coveredNos.stream().max(Long::compare).orElse(0L);
    if (hiNo == 0L || other.getRowHash(hiNo).equals(getRowHash(hiNo)))
      return hiNo;
    
    Long[] nos = coveredNos.toArray(new Long[coveredNos.size()]);
    Arrays.sort(nos);
    
    int index = nos.length - 1;
    while (
        index-- > 0 &&
        !other.getRowHash(nos[index]).equals(getRowHash(nos[index])));
    
    return -nos[index + 1];
  }
  
  
  
  /**
   * Returns a comparison of this and the {@code other} path. This method
   * <em>does not throw {@code HashConflictException}</em>s. It's purpose
   * is to measure to what degree 2 ledgers (as represented by {@code Path}s)
   * are related, if at all.
   * <p>
   * This operation is <em>symmetric</em>. That is, {@code a.comp(b)} equals
   * {@code b.comp(a)}.
   * </p>
   * 
   * @param other a path from this or another ledger
   * 
   * @see Comp
   */
  public final Comp comp(Path other) {
    
    final long hi = hi();
    
    if (other == this)
      return Comp.ofOk(hi);
    
    if (hi > other.hi())
      return other.comp(this);
    
    // assert other.hi() >= hi;

    long commonNo = 0L;
    long conflictNo = 0L;
    
    // optimistically check if this path's last row hash
    // against that recorded in other; if they agree, we're
    // done
    if (other.hasRowCovered(hi)) {
      if (last().hash().equals(other.getRowHash(hi)))
        return Comp.ofOk(hi);
      conflictNo = hi;
    }
    
    // collect the common covered row no.s in both paths into a set
    Set<Long> coveredNos = commonNos(other, Path::nosCovered);
    if (coveredNos.isEmpty())
      return Comp.of();
    
    // optimistically check the row-hash of the highest common no
    // (the optimism, is in avoiding sorting the set)
    if (conflictNo == 0L) {
      long hiNo = coveredNos.stream().max(Long::compare).get();
      if (hiNo == 0L)
        return Comp.of();
      
      if (other.getRowHash(hiNo).equals(getRowHash(hiNo)))
        return Comp.ofOk(hiNo);
      
      conflictNo = hiNo;
    }    
    
    assert conflictNo != 0L;
    
    // sort the covered no.s
    Long[] nos = coveredNos.toArray(new Long[coveredNos.size()]);
    Arrays.sort(nos);
    
    // iterate over the row no.s in reverse; skip the last index
    // since we've already checked it..
    // the first row no. at which the row-hashes match breaks the loop
    //
    for (int index = nos.length -1; index-- > 0; ) {
      long rowNo = nos[index];
      if (rowNo == 0L)
        break;
      if (other.getRowHash(rowNo).equals(getRowHash(rowNo))) {
        commonNo = rowNo;
        break;
      }
      
      assert conflictNo > rowNo;
      conflictNo = rowNo;
    }
    
    
    return new Comp(commonNo, conflictNo);
  }

  

  /**
   * A measure of difference and similarity between 2 paths, composed of 2
   * non-negative numbers {@code commonNo}, and {@code conflictNo}.
   * 
   * @see Path#comp(Path)
   * @see #commonNo()
   * @see #conflictNo()
   * @see #ok()
   * @see #conflict()
   * @see #forked()
   * @see #forkNo()
   * @see #isEmpty()
   */
  public record Comp(long commonNo, long conflictNo) implements Comparable<Comp> {
    
    public final static Comp EMPTY = new Comp(0L, 0L);
    
    
    /**
     * Returns an "OK" instance.
     * 
     * @param commonNo   the <em>highest</em> row no. the 2 paths' row-hashes
     *                   match (&ge; 0)
     */
    public static Comp ofOk(long commonNo) {
      return new Comp(commonNo, 0L);
    }
    
    
    /**
     * Returns a "Conflict" instance. The 2 ledgers' row-hashes conflict
     * at the given no.
     * 
     * @param conflictNo the <em>lowest</em> row no. the 2 paths' row-hashes
     *                   conflict; zero, if they don't conflict.
     */
    public static Comp ofConflict(long conflictNo) {
      return new Comp(0L, conflictNo);
    }
    
    
    /**
     * Returns the {@linkplain Comp#EMPTY} instance, indicating 2 paths that
     * do not intersect.
     */
    public static Comp of() {
      return EMPTY;
    }
    
    /**
     * Full Constructor.
     * 
     * @param commonNo   the <em>highest</em> row no. the 2 paths' row-hashes
     *                   match (&ge; 0)
     * @param conflictNo the <em>lowest</em> row no. the 2 paths' row-hashes
     *                   conflict; zero, if they don't conflict. If not zero,
     *                   then {@code conflictNo > commonNo}.
     * 
     * @throws IllegalArgumentException
     *         if either argument is negative, or if {@code conflictNo} is
     *         non-zero <em>and</em> {@code commonNo >= conflictNo}
     */
    public Comp {
      if (commonNo < 0L)
        throw new IllegalArgumentException("commonNo " + commonNo);
      if (conflictNo < 0L)
        throw new IllegalArgumentException("conflictNo " + conflictNo);
      if (conflictNo != 0L && commonNo >= conflictNo)
        throw new IllegalArgumentException(
            "commonNo (%d) must be less than (non-zero) conflictNo (%d)"
            .formatted(commonNo, conflictNo));
    }
    
    
    /**
     * The <em>highest</em> row no. the 2 paths' row-hashes are equal.
     * 
     * @return &ge; 0
     */
    public long commonNo() {
      return commonNo;
    }
    
    
    /**
     * The <em>lowest</em> row number the 2 paths' row-hashes conflict; zero, if
     * they don't conflict. If not zero, then {@code > commonNo()}.
     * 
     * @return &ge; 0
     */
    public long conflictNo() {
      return conflictNo;
    }
    
    
    /** Returns the greater of {@linkplain #conflictNo()} and {@linkplain #commonNo()} */
    public long hiNo() {
      return Math.max(conflictNo, commonNo);
    }
    
    
    /**
     * Returns {@code true} if the 2 paths' row-hashes <em>do not conflict</em>
     * at any row no. Note, empty instances (see {@linkplain #isEmpty()}) are
     * thus OK.
     * 
     * @return {@code conflictNo() == 0L}
     * 
     * @see #conflict()
     */
    public boolean ok() {
      return conflictNo == 0L;
    }

    
    /**
     * Returns {@code true} if the 2 paths' row-hashes conflict at some
     * row no.
     * 
     * @return {@code conflictNo() != 0L}
     * @see #ok()
     */
    public boolean conflict() {
      return conflictNo != 0L;
    }
    
    /**
     * Returns {@code true} if the 2 paths are forked from a common
     * ancestor.
     * 
     * @return {@code commonNo() != 0L && conflictNo() != 0L}
     */
    public boolean forked() {
      return conflictNo != 0L && commonNo != 0L;
    }
    
    
    /**
     * Returns the row number the 2 paths are forked at, if known.
     * This returns {@linkplain #conflictNo()} iff
     * {@code conflictNo() - commonNo() == 1L}. That is, not all
     * {@linkplain #forked()} instances return a fork no.
     */
    public Optional<Long> forkNo() {
      return
          conflictNo - commonNo == 1L ?
              Optional.of(conflictNo) :
                Optional.empty();
        
    }
    
    /**
     * Returns {@code true}, iff the 2 paths do not intersect.
     * 
     * @return {@code commonNo() == 0L && conflictNo() == 0L}
     * @see Comp#of()
     */
    public boolean isEmpty() {
      return commonNo == 0L && conflictNo == 0L;
    }
    
    


    /**
     * Instances are ordered in <em>descending</em> {@linkplain #conflictNo()}
     * (excepting zero, which is treated as first, not last); instances are
     * secondly ordered in <em>ascending</em> {@linkplain #commonNo()}.
     * 
     * @return
     */
    @Override
    public int compareTo(Comp o) {
      
      int comp = Long.compare(o.compConflictNo(), compConflictNo());
      return comp == 0 ? Long.compare(commonNo, o.commonNo) : comp;
    }
    
    
    private long compConflictNo() {
      return conflictNo == 0L ? Long.MAX_VALUE : conflictNo;
    }
    
    
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
    var rns = SkipLedger.stitchCompress( rowNumbers());
    final int size = rns.size();
    StringBuilder s = new StringBuilder(18 + size * 8);
    s.append("Path[stitch:<");
    if (size > 12) {
      s.append(rns.subList(0, 6).toString().substring(1));
      s.setLength(s.length() - 1);
      s.append(", ..(").append(size - 12).append(" more), ");
      s.append(rns.subList(size - 6, size).toString().substring(1));
      s.setLength(s.length() - 1);
    } else {
      s.append(rns.toString().substring(1));
      s.setLength(s.length() - 1);
    }
    return s.append(">]").toString();
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
























