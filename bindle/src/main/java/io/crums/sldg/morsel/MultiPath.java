/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.SerialFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.Path.Comp;
import io.crums.sldg.PathPack;
import io.crums.sldg.Row;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * A collection of <em>intersecting</em> paths from the same
 * ledger. The row-hashes at any row no. the paths know about
 * are in agreement. Further, each path intersects with at least
 * one other path in the collection. There are no other
 * constraints. Duplicate paths are not allowed.
 * 
 * <h2>Constraint Verification</h2>
 * <p>
 * Since the {@linkplain Path}s are verified at instantiation, it
 * suffices to verify the hash of the highest numbered row
 * common in any pair of paths is the same: the hashes at any
 * common, lower-numbered rows in the pair of paths are guaranteed
 * to be equal. This pair-wise logic is captured in
 * {@linkplain Path#highestCommonNo(Path)}. Excepting the trivial
 * singleton case, the constraint is verified by ensuring that for
 * each path {@code p} in the collection, there exists another path
 * {@code p2} where {@code p.highestCommonNo(p2)} returns a non-zero value.
 * </p>
 * <h2>Authority Row[s]</h2>
 * <p>
 * If every path in the structure links to (the hash of) the
 * highest numbered row ({@linkplain #hi()}), whether directly or
 * thru another path that does, then {@linkplain #singleAuthority()}
 * returns {@code true}. Otherwise, there are multiple authority
 * rows (see {@linkplain #authorityNos()}) and not all row hashes
 * can be guaranteed to belong to the same skipledger.
 * </p>
 * 
 * 
 * @see #singleAuthority()
 * @see #paths() 
 * 
 * @see MultiPathBuilder
 */
public class MultiPath implements Serial {
  
  /** Desceding-hi comparator; ascending-lo tie-breaker. */
  public final static Comparator<Path> DESCENDING_HI =
      (a, b) -> {
        int comp = Long.compare(b.hi(), a.hi());
        return comp == 0 ? Long.compare(a.lo(), b.lo()) : comp;
      };
  
  private final Path[] paths;
  
  private final boolean stitched;
  
  
  
  public MultiPath(MultiPathBuilder builder) {
    this.paths = builder.paths();
    Arrays.sort(paths, DESCENDING_HI);
    this.stitched = calculateStitched();
  }


  /**
   * Constructs an instance from the given collection.
   * 
   * @param paths       not empty, with non-null elements, no-duplicates
   * @throws HashConflictException 
   *            if the hash at any row no. in the paths conflict
   * @throws IllegalArgumentException
   *            if the paths do not intersect.
   */
  public MultiPath(Collection<Path> paths)
      throws HashConflictException, IllegalArgumentException {
    this(paths.toArray(new Path[paths.size()]));
  }
  
  
  private MultiPath(Path[] paths) {
    this.paths = paths;
    Arrays.sort(paths, DESCENDING_HI);
    this.stitched = verifyPaths();
  }
  
  
  /**
   * Returns the paths in descending order of {@linkplain Path#hi()} row no.
   * 
   * @return non-empty, read-only
   * @see #DESCENDING_HI
   */
  public final List<Path> paths() {
    return Lists.asReadOnlyList(paths);
  }
  
  
  /**
   * Compares the given {@code path} with those in this collection and returns
   * the highest row number in the path whose row hash is already
   * known by this instance.
   * 
   * @param path        from the same ledger
   * @return    &ge; 0
   * @throws HashConflictException
   *                     if {@code path} is not from a ledger represented
   *                     by this instance
   */
  public final long highestCommonNo(Path path)
      throws HashConflictException, MalformedNuggetException {
    Comp comp = comp(path);
    if (comp.ok())
      return comp.commonNo();
    throw new HashConflictException(
        "%s %s MultiPath at row [%d]".formatted(
            path, comp.forked() ? "forks" : "conflicts", comp.conflictNo()));
//    return highestCommonNo(paths(), path);
  }
  
  
  
  public final Path.Comp comp(Path path) throws MalformedNuggetException {
    
    final long pathHi = path.hi();
    
    Comp bestComp = Comp.of();
    for (Path p : paths) {
      Comp comp = p.comp(path);
      bestComp = upgrade(bestComp, comp, path);
      if (bestComp.ok()) {
        if (bestComp.commonNo() == pathHi)
          break;
      } else if (bestComp.forkNo().isPresent())
        break;
      
    }
    return bestComp;
  }
  
  
  private Comp upgrade(Comp a, Comp b, Path path) throws MalformedNuggetException {
    int progressComp = b.compareTo(a);
    if (progressComp == 0)
      return a;
    if (progressComp < 0) {
      var swap = a;
      a = b;
      b = swap;
    }
    return upgradeSorted(a, b, path);
  }
  
  
  /**
   * Invariant: {@code b} is greater than {@code b}, i.e. {@code b.compareTo(a)} is positive
   */
  private Comp upgradeSorted(Comp a, Comp b, Path path) throws MalformedNuggetException {
    
    if (b.ok() || b.commonNo() >= a.commonNo()) {
      return b;
    }
    
    // assert b.conflictNo() > 0; // too obv.
    
    if (a.commonNo() >= b.conflictNo()) {
      var authorityNos = authorityNos();
      assert authorityNos.size() > 1;
      throw new MalformedNuggetException(
          """
          multi (%d) authority instance proven inconsistent by %s:
          row-hashes agree at row [%d];
          disagree at row [%d]
          (impossible if all paths in this instance were from
          the same ledger); authority nos. %s
          """.formatted(
              authorityNos.size(), path, a.commonNo(), b.conflictNo(),
              authorityNos));
    }
    
    return new Comp(a.commonNo(), b.conflictNo());
  }
  
  
  
  
  
//  
//  
//  /**
//   * Returns the extremum of {@linkplain Path#highestCommonOrForkNo(Path)}
//   * across all this instance's {@linkplain #paths()}. That is, if the returned
//   * value is negative, then it is the <em>smallest</em> return value; if
//   * positive, then it is the <em>largest</em> value returned by
//   * {@linkplain Path#highestCommonOrForkNo(Path)}.
//   * 
//   * <p>
//   * There are no comparison caveats (a path will be compared to itself).
//   * <em>Does not throw</em> {@code HashConflictException}.
//   * </p>
//   * 
//   * @param path
//   * @return
//   */
//  public final long highestCommonOrForkNo(Path path) {
//    return highestCommonOrForkNo(paths(), path);
//  }
//  
//  
//  
//  public final long highestCommonOrForkNo(MultiPath other) {
//    if (this == other)
//      return hi();
//    
//    long cof = 0L;
//    
//    for (Path p : other.paths())
//      cof = cofemum(cof, highestCommonOrForkNo(p));
//    
//    return cof;
//  }
//  
//  
//  
//  public final Optional<Long> forkNo(Path path) {
//    long cof = highestCommonOrForkNo(paths(), path);
//    return cof < 0L ? Optional.of(-cof) : Optional.empty();
//  }

  
  
  /**
   * Returns the highest ledger row number known to the collection.
   * 
   * @return &ge; {@code lo()}
   * @see #lo()
   */
  public final long hi() {
    return paths[0].hi();
  }
  
  
  /**
   * Returns {@code true} iff <em>all</em> ledger assertions from this
   * instance can be linked to the hash of the last row (numbered
   * {@linkplain hi()}).
   * 
   * @return {@code authorityNos().size() == 1}
   * 
   */
  public final boolean singleAuthority() {
    return stitched;
  }
  
  
  /**
   * Returns the "authority" row numbers. These identify the rows whose
   * row-hashes are the source of proofs from this instance. The returned
   * list of row no.s is in descending order, with {@linkplain #hi()} as its
   * first element. Ideally, the returned list is a singleton.
   * 
   * @return  non-empty, read-only list of row no.s in descending order with no
   *          duplicates
   */
  public final List<Long> authorityNos() {
    if (stitched)
      return List.of(paths[0].hi());
    
    
    ArrayList<Long> hiNos = new ArrayList<>(paths.length);
    hiNos.add(paths[0].hi());
    
    // for each path p, if p.hi() is not covered by the previous
    // paths, add p.hi() to hiNos..
    
    for (int index = 1; index < paths.length; ++index) {
      Path p = paths[index];
      if (p.hi() == hiNos.getLast())
        continue;
      // assert p.hi() < hiNos.getLast();
      int searchIndex = index;
      while (searchIndex -- > 0 && !paths[searchIndex].hasRowCovered(p.hi()));
      if (searchIndex == -1)
        hiNos.add(p.hi());
    }
    
    return Collections.unmodifiableList(hiNos);
  }
  

  /**
   * Returns the lowest (full) row number known to the collection.
   * 
   * @return positive no.
   * @see #hi()
   */
  public final long lo() {
    return lo(paths());
  }
  
  
  /**
   * Returns {@code true} iff the <em>input</em>-hash of the row at the
   * given number is known by this collection. Note, if this method
   * returns {@code true}, then {@linkplain #coversRow(long)} also return
   * {@code true} with the same argument.
   * 
   * @see #coversRow(long)
   */
  public final boolean hasRow(long rowNo) {
    int index = paths.length;
    while (index-- > 0 && !paths[index].hasRow(rowNo));
    return index != -1;
  }
  
  
  public final Optional<Row> findRow(long rowNo) {
    return findRow(paths(), rowNo);
  }
  
  
  /**
   * Returns {@code true} iff the [commitment] hash of the row at the
   * given number is known by this collection.
   * 
   * @see #hasRow(long)
   */
  public final boolean coversRow(long rowNo) {
    int index = paths.length;
    while (index-- > 0 && !paths[index].hasRowCovered(rowNo));
    return index != -1;
  }
  
  
  
  public final ByteBuffer rowHash(long rowNo) {
    return rowHash(Lists.asReadOnlyList(paths), rowNo);
  }
  
  
  
  private boolean verifyPaths() {
    
    // TODO: speed this up (if it matters)
    
    final int count = paths.length;
    if (count == 0)
      throw new IllegalArgumentException("path count zero");
    
    if (count == 1)
      return true;
    
    boolean stitched = true;
    var pathList = paths();
    for (int index = 1; index < paths.length; ++index) {
      Path p = paths[index];
      if (paths[index-1].equals(p))
        throw new IllegalArgumentException(
            "duplicate path at index [%d]: %s".formatted(index, pathList));
      long hcn = highestCommonNo(pathList.subList(0, index), p);
      stitched &= p.hi() == hcn;
      if (hcn == 0L)
        throw new IllegalArgumentException(
            p + " must intersect with others in collection: " + paths());
    }
    return stitched;
  }
  
  
  
  private boolean calculateStitched() {
    for (int index = 1; index < paths.length; ++index) {
      Path p = paths[index];
      int searchIndex = index;
      for (; searchIndex-- > 0 && !paths[searchIndex].hasRowCovered(p.hi()); );
      if (searchIndex == -1)
        return false;
    }
    return true;
  }
  
  
  
  

  
  
  @Override
  public int serialSize() {
    int bytes = 4;
    for (var path : paths)
      bytes += path.pack().serialSize();
    return bytes;
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putInt(paths.length);
    for (var path: paths)
      path.pack().writeTo(out);
    return out;
  }
  
  
  
  
  public static MultiPath load(ByteBuffer in)
      throws HashConflictException, SerialFormatException {
    
    int count = in.getInt();
    if (count <= 0)
      throw new SerialFormatException(
          "read illegal path count %d in %s".formatted(count, in));
    if (count * SldgConstants.HASH_WIDTH > in.remaining())
      throw new SerialFormatException(
          "read count (%d) will cause underflow: %s".formatted(count, in));
    Path[] paths = new Path[count];
    for (int index = 0; index < count; ++index)
      paths[index] = PathPack.load(in).path();
    
    return new MultiPath(paths);
  }
  
  


  static long lo(Collection<Path> paths) throws NoSuchElementException {
    return paths.stream().map(Path::lo).min(Long::compareTo).get();
  }
  
  
  static long hi(Collection<Path> paths) throws NoSuchElementException {
    return paths.stream().map(Path::hi).max(Long::compareTo).get();
  }
  
  
  static long highestCommonNo(List<Path> paths, Path path)
      throws HashConflictException {

    Objects.requireNonNull(path, "path");
    long hiInterNo = 0L;
    final long pathHi = path.hi();
    for (int index = paths.size(); index-- > 0; ) {
      long commonHi = paths.get(index).highestCommonNo(path);
      if (commonHi > hiInterNo) {
        hiInterNo = commonHi;
        if (commonHi == pathHi)
          return pathHi;
      }
        
    }
    return hiInterNo;
  }
  
  
//  static long highestCommonOrForkNo(Collection<Path> paths, Path path) {
//
//    Objects.requireNonNull(path, "path");
//    long no = 0L;
//    for (Path p : paths)
//      no = cofemum(no, path.highestCommonOrForkNo(p));
//    
//    return no;
//  }
//  
//  /** Common-or-fork extremum. */
//  private static long cofemum(long cofA, long cofB) {
//    return
//        // Slightly more efficient version of ..
//        //
//        // (cofA < 0L || cofB < 0L) ?
//        //     Math.min(cofA, cofB) : Math.max(cofA, cofB);
//        // 
//        cofA < 0L ?
//            Math.min(cofA, cofB) :
//              cofB < 0L ? cofB :
//                Math.max(cofA, cofB);
//
//  }
  
  
  static ByteBuffer rowHash(Collection<Path> paths, long rowNo) {
    for (Path path : paths) {
      if (path.hasRowCovered(rowNo))
        return path.getRowHash(rowNo);
    }
    throw new IllegalArgumentException("row [" + rowNo + "] not covered");
  }
  
  /** Find any. */
  static Optional<Row> findRow(List<Path> paths, long rowNo) {
    return
        paths.stream().filter(p -> p.hasRow(rowNo)).findAny()
        .map(p -> p.getRowByNumber(rowNo));
  }
  

}















