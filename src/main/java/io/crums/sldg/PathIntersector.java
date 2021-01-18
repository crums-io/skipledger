/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.crums.util.Sets;

/**
 * An iteration over the intersections of a pair of paths.
 * 
 * <h3>Thread-safety</h3>
 * <p>With the notable exception of the thread-safe {@linkplain #iterator()} method
 * (which returns a reset clone of the instance) <em>
 * the behavior of instances is undefined under concurrent access.</em></p>
 * 
 * @see #firstConflict()
 */
public class PathIntersector implements Iterable<RowIntersection>, Iterator<RowIntersection> {
  
  private final Path a;
  private final Path b;
  
  private final Iterator<Long> coveredIntersectIter;
  
  private RowIntersection next;
  private RowIntersection conflict;

  /**
   * 
   */
  public PathIntersector(Path a, Path b) {
    this(
        Objects.requireNonNull(a, "null a"),
        Objects.requireNonNull(b, "null b"),
        false);
  }
  
  
  public PathIntersector(PathIntersector copy) {
    this(copy.a, copy.b, false);
  }
  
  
  private PathIntersector(Path a, Path b, boolean ignored) {
    this.a = a;
    this.b = b;
    
    this.coveredIntersectIter =
        Sets.intersectionIterator(
            a.rowNumbersCovered(),
            b.rowNumbersCovered().tailSet(1L));
    
    updateNext();
  }

  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public RowIntersection next() {
    if (next == null)
      throw new NoSuchElementException();
    
    RowIntersection out = next;
    updateNext();
    return out;
  }
  
  
  private void updateNext() {
    next = nextIntersection();
    if (next != null && next.isConflict()) {
      conflict = next;
      next = null;
    }
  }
  
  
  /**
   * Collects and returns the remaining <em>unique-pair</em> intersections.
   * 
   * @return non-null, read-only list of intersections in ascending
   * {@linkplain RowIntersection #rowNumber() row number}s.
   * 
   * @see #streamUniquePairs()
   */
  public List<RowIntersection> collect() {
    if (!hasNext())
      return Collections.emptyList();
    
    return stream().collect(Collectors.toUnmodifiableList());
  }
  
  
  /**
   * Consumes the remaining intersections in this iteration until the first conflict is
   * encountered and returns it, if any. Conflicts are arguably as important as
   * valid intersections: a conflict proves that 2 paths do not belong to the same ledger.
   */
  public Optional<RowIntersection> firstConflict() {
    while (hasNext())
      next();
    return Optional.ofNullable(conflict);
  }
  
  
  
  
  
  private RowIntersection nextIntersection() {
    if (!coveredIntersectIter.hasNext())
      return null;
    
    long rowNumber = coveredIntersectIter.next();
    Row rowA = a.getRowOrReferringRow(rowNumber);
    Row rowB = b.getRowOrReferringRow(rowNumber);
    
    boolean conflict = !rowA.hash(rowNumber).equals(rowB.hash(rowNumber));
    return new RowIntersection(rowNumber, rowA, rowB, conflict);
  }
  

  /**
   * Returns a new copy of this instance that starts the iteration from the beginning. Its
   * iteration-state is independent of this instance.
   */
  @Override
  public PathIntersector iterator() {
    return new PathIntersector(this);
  }
  
  
  
  public Stream<RowIntersection> stream() {
    return StreamSupport.stream(spliterator(), false);
  }
  
  
//  /** COMMENTED OUT CUZ IT THERE ARE CORNER CASES IT DOESN'T WORK;
//   *  WILL TRY LESS CLEVER LOOKAHEAD STRATEGY
//   *
//   * Streams unique pairs of intersecting rows by only returning interections whose
//   * {@linkplain RowIntersection#rowNumber() row number} is the maximum for the given
//   * pair of rows.
//   * <p>
//   * Recall, under our definition, rows (depending on their row numbers) may cause intersections
//   * not just at their own numbers, but at lower numbers too. Thus a pair of rows may intersect at
//   * more than one location. The returned stream filters thru only the highest numbered
//   * intersection for a given pair of source rows.
//   */
//  public Stream<RowIntersection> streamUniquePairs() {
//    return stream().filter(i -> !i.isDerivative());
//  }

}
