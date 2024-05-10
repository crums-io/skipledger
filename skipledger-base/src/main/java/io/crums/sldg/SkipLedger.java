/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.util.EasyList;
import io.crums.util.Lists;
import io.crums.util.hash.Digest;

/**
 * <p>
 * A <em>skip ledger</em> (new terminology). A skip ledger models a tamper proof
 * <a href="https://en.wikipedia.org/wiki/Skip_list">skip list</a>. It's use here
 * is as a tamper proof <em>list</em>, not as a search structure. Here are some of its key
 * differences:
 * </p>
 * <ol>
 * <li><em>Append only.</em> Items are only ever be appended to the <em>end</em> of the
 * list. (In skip list terms, the items are ordered in the ordered in they appear.)</li>
 * <li><em>Hash pointers.</em> In lieu of the handles, pointers and such in skip lists,
 * the pointers in a skip ledger are hash pointers. (A hash pointer is the hash of another
 * node in the skip list.)</li>
 * <li><em>Hash of items only.</em> We only ever append the <em>hash</em> of items,
 * not the items themselves. (Item storage is an orthogonal issue: they might exist in
 * a database table, for example.)</li>
 * <li><em>Deterministic structure.</em> The row number (the index of an item in the
 * list) uniquely determines the number of skip pointers its corresponding node in the
 * skip list has: unlike a skip list, no randomization is involved.</li>
 * <li><em>Efficient verification.</em> Whereas a skip list is efficient at search,
 * a skip ledger's mojo lies in its efficiency in verification. </li>
 * </ol>
 * <p>
 * So a skip ledger is like a tamper proof linked list, but on link steroids. The main
 * advantage it offers over a (singlely) linked tamper proof list is that you don't need
 * to show the whole list (or any substantial part of it) in order to prove an item's position in
 * in the list.
 * </p><p>
 * Which list? In the same way that the root hash of a Merkle tree uniquely identifies
 * the tree, the hash of the last row (node) in a skip ledger uniquely identifies the ledger
 * (the append-only list). To prove that an item is in the ledger (its hash actually--equivalently)
 * one needs to enumerate a list of linked rows (nodes) from the last row in the ledger (whose hash
 * defines the ledger's state) to the row number (index) at which the item is located. With
 * a skip ledger, the number of rows in such hash proofs is on the order of the logarithm
 * of the number of rows in the ledger.
 * </p>
 * <h2>Abbreviated State</h2>
 * <p>
 * Since our ledger is composed entirely of opaque hashes, we <em>could</em> share the entire ledger
 * with 3rd parties for verification purposes (as when verifying the contents of a single row).
 * This is similar to how one verifies items on standard blockchains: you generally need access to
 * the whole chain. Verifying things is still faster using our skip ledger than walking a
 * linked list linearly. But we can do better.
 * </p><p>
 * As discussed above, the hash of a ledger's last row uniquely identifies the ledger itself.
 * A hash is a kind of contraction of state (perhaps why it's also called a digest). But <em>the
 * skip ledger data structure also offers an intermediate contraction of state, namely the
 * shortest path of linked rows from the last to first</em>. Again, the size of this representation is logarithmic
 * in the number of rows in the ledger, so it's compact no matter the ledger size.
 * </p><p>
 * The advantage with advertising the state of a ledger this way is that if it intersects with
 * a previously emitted path from the ledger (when the ledger had fewer rows), then the previous
 * path (e.g. proof that an entry belonged in the ledger at the advertised row number) can be
 * patched with the new state information.
 * </p><p>
 * The issue where this state information comes from, how it is validated, etc., is a can
 * deliberately kicked down the road. It could be simply advertised over HTTPS with the existing
 * certificate authority mechanisms. Or the owner of the ledger might insert their public key
 * as the first row, and then periodically add a public entry (the contents of a row they make
 * public) that is a signature of the previous row's hash. The choices are too many to consider at
 * this juncture and probably best left to evolve.
 * </p>
 * <h2>Row Numbering</h2>
 * <p>
 * A row's number in the ledger uniquely determines how many pointers it has (see {@linkplain #skipCount(long)}
 * (and which row numbers they point to). An important point to note is that the indexing is
 * 1-based, not zero based. The 1st row added to a ledger is numbered 1. This is mostly for convenience:
 * calculating the number of skip pointers is easier in the 1-based system.
 * </p><p>
 * Note conceptually, the zero'th row is a special sentinel in every ledger. Its hash evaluates to a string of
 * zero-bytes (as wide as the hash algo requires), the contents of which.. well who knows. (And
 * for our purposes, it wouldn't matter if it were discovered by chance (practically impossible), by say some Bitcoin miner).
 * </p>
 * <h2>TODO</h2>
 * <ul>
 * <li>Remove {@code Digest} interface. It's not a good way to set the hashing algo
 * and besides.. the code doesn't really use it.</li>
 * </ul>
 */
public abstract class SkipLedger implements Digest, AutoCloseable {
  
  /**
   * <p>Returns the number of skip pointers at the given row number. The
   * returned number is one plus the <em>exponent</em> in the highest power of 2 that is
   * a factor of the row number. For odd row numbers, this is always 1 (since the highest
   * factor here is 2<sup><small>0</small></sup>).</p>
   * 
   * <h4>Strict Law of Averages</h4>
   * 
   * <p>The <em>average</em> number of skip pointers is <em>always</em> less than 2.
   * Equivalently, the total number of skip pointers up to (and including) a given row
   * number, is always less than twice the row number.
   * </p>
   * 
   * @param rowNumber &ge; 1
   * @return &ge; 1 (with average value of 2)
   */
  public static int skipCount(long rowNumber) {
    checkRealRowNumber(rowNumber);
    return 1 + Long.numberOfTrailingZeros(rowNumber);
  }
  
  
  
  /**
   * Determines if any of the 2 rows (from the same ledger) with given row numbers
   * reference (and therefore link to) the other by hash. Since every row knows its own hash,
   * by this definition, every row is linked also linked to itself.
   * 
   * @param rowNumA &ge; 0 (zero OK!)
   * @param rowNumB &ge; 0
   */
  public static boolean rowsLinked(long rowNumA, long rowNumB) {
    long lo, hi;
    if (rowNumA <= rowNumB) {
      lo = rowNumA;
      hi = rowNumB;
    } else {
      lo = rowNumB;
      hi = rowNumA;
    }
    if (lo < 0)
      throw new IllegalArgumentException(
          "row numbers must be non-negative. Actual given: " + rowNumA + ", " + rowNumB);
    
    long diff = hi - lo;
    
    return diff == Long.highestOneBit(diff) && diff <= (1L << (skipCount(hi) - 1));
  }
  

  /**
   * Throws an <code>IllegalArgumentException</code> if the given row number is not &ge; 1.
   * If the row number is zero, the thrown exception details why row <code>0</code> is a bad
   * argument.
   * 
   * @param rowNumber &ge; 1
   * @throws IllegalArgumentException if <code>rowNumber &le; 0</code>
   */
  public static void checkRealRowNumber(long rowNumber) throws IllegalArgumentException {
    
    if (rowNumber < 1) {
      
      String msg;
      if (rowNumber == 0)
        msg =
            "row 0 is an *abstract row that hashes to a string of zeroes; " +
            "at most 63 of rows reference it, but its contents (" +
            "pointers and input hash) are undefined";
      else
        msg = "negative row number " + rowNumber;
      
      throw new IllegalArgumentException(msg);
    }
  }
  

  
  

  /**
   * Returns the rows <em>covered</em> by the specified skip path. The rows covered
   * include both the linking rows and all the rows referenced in the linking rows.
   * 
   * @param lo row number &ge; 1
   * @param hi row number &ge; {@code lo}
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
   * <p>
   * <em>Note the returned set may (and likely does) contain the sentinel row number 0 (zero).
   * </em> The reason why is that the returned set, is the set of row numbers whose
   * hashes are needed to compute the hashes of the given row numbers. And while the contents
   * of the sentinel row are undefined, it's hash <em>is</em>.
   * </p>
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
        covered.add(referencedRowNumber);
      }
    }
    
    return Collections.unmodifiableSortedSet(covered);
  }
  
  
  /**
   * Returns the row numbers the given bag of row numbers reference
   * and which are <em>not</em> already in the bag.
   * 
   * @param rowNumbers bag or row numbers, each &ge; 1. May contain duplicates.
   * @return a set of referenced row numbers not already in the bag. May include
   *         zero (the sentinel row).
   */
  public static SortedSet<Long> refOnlyCoverage(Collection<Long> rowNumbers) {
    TreeSet<Long> fullSet = new TreeSet<>(rowNumbers);  // not changed hereafter
    TreeSet<Long> refOnly = new TreeSet<>();
    
    for (Long rn : fullSet) {
      
      int pointers = skipCount(rn);
      for (int e = 0; e < pointers; ++e) {
        long delta = 1L << e;
        Long refRn = rn - delta;
        assert refRn >= 0;
        if (!fullSet.contains(refRn))
          refOnly.add(refRn);
      }
    }
    return Collections.unmodifiableSortedSet(refOnly);
  }
  
  /**
   * Stitches and returns a sorted list of row numbers that is
   * composed of the given collection interleaved with linking row numbers as necessary.
   * 
   * @param rowNumbers collection of positive row numbers (dups OK)
   * 
   * @return a new, not empty, read-only list of unique, sorted numbers
   * @see #stitch(List)
   */
  public static List<Long> stitchCollection(Collection<Long> rowNumbers) {
    {
      var trivial = checkTrivial(rowNumbers);
      if (trivial != null)
        return trivial;
    }
    
    SortedSet<Long> orderedRns = rowNumbers instanceof SortedSet<Long> sorted ?
        sorted : new TreeSet<>(rowNumbers);
    checkRealRowNumber(orderedRns.first());
    return stitch(
        Lists.asReadOnlyList(
            orderedRns.toArray(new Long[orderedRns.size()])));
  }
  

  /**
   * Stitches and returns a sorted list of row numbers.
   * 
   * @param orderedRns set of positive row no.s
   * 
   * @return a new, not empty, read-only list of unique, sorted numbers
   * @see #stitch(List)
   */
  public static List<Long> stitchSet(SortedSet<Long> orderedRns) {
    {
      var trivial = checkTrivial(orderedRns);
      if (trivial != null)
        return trivial;
    }
    checkRealRowNumber(orderedRns.first());
    return stitch(
        Lists.asReadOnlyList(
            orderedRns.toArray(new Long[orderedRns.size()])));
  }
  
  
  private static List<Long> checkTrivial(Collection<Long> rowNumbers) {
    final int count = rowNumbers.size();
    
    switch (count) {
    case 0:
      throw new IllegalArgumentException("empty rowNumbers");
    case 1:
      var rn = rowNumbers.iterator().next();
      checkRealRowNumber(rn);
      return List.of(rn);
    default:
      return null;
    }
  }
  
  /**
   * Stitches and returns a copy of the given (sorted) list of row numbers that is
   * interleaved with linking row numbers as necessary.
   * If {@code rowNumbers} is already stitched, a new copy is still returned.
   * <p>
   * This method supports an abbreviated path specification.
   * </p>
   * 
   * @param rowNumbers not empty, monotonically increasing, positive row numbers
   * 
   * @return a new, not empty, read-only list of unique, sorted numbers
   */
  public static List<Long> stitch(List<Long> rowNumbers) {
    {
      var trivial = checkTrivial(rowNumbers);
      if (trivial != null)
        return trivial;
    }
    
    Long prev = rowNumbers.get(0);
    
    if (prev < 1)
      throw new IllegalArgumentException(
          "illegal rowNumber " + prev + " at index 0 in list " + rowNumbers);
    
    final int count = rowNumbers.size();
    ArrayList<Long> stitch = new ArrayList<>(count + 16);
    stitch.add(prev);
    
    for (int index = 1; index < count; ++index) {
      
      final Long rn = rowNumbers.get(index);
      
      if (rn <= prev)
        throw new IllegalArgumentException(
            "illegal/out-of-sequence rowNumber " + rn + " at index " + index +
            " in list " + rowNumbers);
      
      if (rowsLinked(prev, rn)) {
        stitch.add(rn);
      } else {
        List<Long> link = skipPathNumbers(prev, rn);
        stitch.addAll(link.subList(1, link.size()));
      }
      
      prev = rn;
    }

    trim(stitch);
    
    return Collections.unmodifiableList(stitch);
  }
  
  
  /**
   * Returns the given path row numbers in pre-stitched form.
   * The returned list is potentially far fewer the input
   * list. It can then be uncompressed via the {@linkplain #stitch(List)}
   * method.
   * 
   * @param pathRns path row no.s
   * @return a subset of the {@code pathRns}
   * 
   */
  public static List<Long> stitchCompress(List<Long> pathRns) {
    final int pSize = pathRns.size();
    if (pSize < 2)
      return pathRns;
    
    var candidate = List.of(pathRns.get(0), pathRns.get(pSize - 1));
    List<Long> skipRns = SkipLedger.stitch(candidate);
    if (skipRns.equals(pathRns))
      return candidate;
    
    var stitchList = new ArrayList<Long>(candidate);
    // invariants: stitchList.get(0) == lo(); stitchList.last() == hi()
    for (int index = 1; index < pSize - 1; ++index) {
      Long rn = pathRns.get(index);
      if (Collections.binarySearch(skipRns, rn) >= 0)
        continue;
      
      final int ssize = stitchList.size();
      if (ssize > 2) {
        // remove the previous target
        // add the new target and see if on expanding
        // whether it includes the prev stitch no.
        
        Long prev = stitchList.remove(ssize - 2);
        // assert stitchList.size() == ssize - 1;
        stitchList.add(ssize - 2, rn);
        skipRns = SkipLedger.stitch(stitchList);
        
        if (Collections.binarySearch(skipRns, prev) < 0) {
          // ..if it doesn't, put it back
          assert stitchList.size() == ssize;
          stitchList.add(ssize - 3, prev);
          skipRns = SkipLedger.stitch(stitchList);
        }
      } else {
        assert stitchList.size() == 2;
        stitchList.add(1, rn);
        skipRns = SkipLedger.stitch(stitchList);
      }
    }
    
    return Collections.unmodifiableList(stitchList);
  }
  

  private final static int AL_TRIM_THRESHOLD = 32;
  
  private static void trim(ArrayList<Long> list) {
    if (list.size() >= AL_TRIM_THRESHOLD)
      list.trimToSize();
  }
  

  /**
   * Finds and returns a structural path connecting the given {@code target} rows stitched
   * together using only the given {@code knownRows}.
   * 
   * @param knownRows known (available) row numbers from which the path may be constructed
   * @param target    target row numbers (must all be contained in {@code knownRows})
   *                in no particular order, but with no duplicates
   * 
   * @return the path, if found; empty, o.w.
   * 
   * @see #stitchPath(SortedSet, Collection)
   * @see #stitchPath(SortedSet, long, long)
   */
  public static Optional<List<Long>> stitchPath(SortedSet<Long> knownRows, Long ... target) {
    return stitchPath(knownRows, Lists.asReadOnlyList(target));
  }
  
  
  /**
   * Finds and returns a structural path connecting the given target rows stitched
   * together using only the given {@code knownRows}.
   * 
   * @param knownRows known (available) row numbers from which the path may be constructed
   * @param targets target row numbers (must all be contained in {@code knownRows})
   *                in no particular order, but with no duplicates
   * 
   * @return the path, if found; empty, o.w.
   * 
   * @see #stitchPath(SortedSet, Long...)
   * @see #stitchPath(SortedSet, long, long)
   */
  public static Optional<List<Long>> stitchPath(SortedSet<Long> knownRows, Collection<Long> targets) {
    
    if (Objects.requireNonNull(targets, "null targets").isEmpty())
      throw new IllegalArgumentException("targets is empty");
    if (!Objects.requireNonNull(knownRows, "null knownRows").containsAll(targets))
      throw new IllegalArgumentException("targets contains one or more unknown rows: " + targets);
    
    EasyList<Long> targetRns;
    {
      SortedSet<Long> t = new TreeSet<>(targets);
      if (t.size() != targets.size())
        throw new IllegalArgumentException("targets contains duplicates: " + targets);
      
      targetRns = new EasyList<>(t.size());
      targetRns.addAll(t);
    }
    
    EasyList<Long> stitched = new EasyList<>(targetRns.size() + 16);
    stitched.add(targetRns.first());
    for (int index = 1; index < targetRns.size(); ++index) {
      Long nextTarget = targetRns.get(index);
      if (rowsLinked(stitched.last(), nextTarget)) {
        stitched.add(nextTarget);
      } else {
        EasyList<Long> pathToNext = stitchPathRecurse(stitched.last(), nextTarget, knownRows);
        if (pathToNext == null)
          return Optional.empty();
        
        stitched.addAll(pathToNext.tailList(1));
      }
    }
    
    trim(stitched);
    
    return Optional.of(Collections.unmodifiableList(stitched));
  }
  
  
  /**
   * Finds and returns a structural path from {@code lo} to {@code hi} (the skip pointers
   * actually go hi to lo, but we present paths in the order they are created) stitched
   * together using only the given {@code knownRows}.
   * <p>
   * Note the skip pointers actually point in the opposite direction, from hi to lo,
   * but we present paths in the order they are created (equivalently, in the order
   * rows are numbered.)
   * </p>
   * 
   * @param knownRows known (available) row numbers from which the path may be constructed
   * @param lo &ge; 1, and contained in (member of) {@code knownRows}
   * @param hi &gt; lo, and contained in {@code knownRows}
   * 
   * @return the path, if found; empty, o.w.
   * 
   * @see #stitchPath(SortedSet, Collection)
   * @see #stitchPath(SortedSet, Long...)
   */
  public static Optional<List<Long>> stitchPath(SortedSet<Long> knownRows, long lo, long hi) {
    if (lo < 1 || hi <= lo)
      throw new IllegalArgumentException("lo " + lo + ", hi " + hi);
    
    if (!Objects.requireNonNull(knownRows, "null knownRows").contains(lo))
      throw new IllegalArgumentException("lo " + lo + " not a member of knownRows " + knownRows);
    else if (!knownRows.contains(hi))
      throw new IllegalArgumentException("hi " + hi + " not a member of knownRows " + knownRows);
    
    if (rowsLinked(lo, hi)) {
      Long[] out = { lo, hi };
      return Optional.of(Lists.asReadOnlyList(out));
    }
    
    ArrayList<Long> out = stitchPathRecurse(lo, hi, knownRows);
    
    if (out == null)
      return Optional.empty();
    
    trim(out);
    
    return Optional.of(Collections.unmodifiableList(out));
  }
  
  
  
  /**
   * Finds and returns an ascending list of linked row numbers if found; {@code null}
   * otherwise.
   * 
   * @param lo &gt; 0, and contained in (member of) {@code known}
   * @param hi &gt; lo, and contained in {@code known}
   * @param known known (available) row numbers set (or subset)
   * 
   * @return stitched path composed of strictly ascending row numbers from {@code lo} to {@code hi} (inc)
   *         or {@code null} if not found
   */
  private static EasyList<Long> stitchPathRecurse(long lo, long hi, SortedSet<Long> known) {
    
    final int skipCount = skipCount(hi);
    
    // we search from highest skip pointer to lowest
    // this way, we discover the shortest path
    for (int exp = skipCount; exp-- > 0; ) {
      long delta = 1L << exp;
      
      long linkedByHi = hi - delta;
      
      if (linkedByHi < lo)
        continue;
      
      if (linkedByHi == lo) {
        // *lo is in *known, no need to check
        EasyList<Long> out = new EasyList<>(8);
        out.add(lo);
        out.add(hi);
        return out;
      }
      
      if (!known.contains(linkedByHi))
        continue;
      
      // lo < linkedByHi < hi
      
      EasyList<Long> upstream = stitchPathRecurse(lo, linkedByHi, known);
      if (upstream != null) {
        upstream.add(hi);
        return upstream;
      }
    }
    
    return null;
  }
  
  
  
  
  
  /**
   * Returns the structural path from a lower
   * (older) row number to a higher (more recent) row number in a ledger.
   * This is just the shortest structural path following the hash pointers in each
   * row from the <code>hi</code> row number to the <code>lo</code> one. The returned list
   * however is returned in reverse order, in keeping with the temporal order of
   * ledgers.
   * 
   * @param lo row number &gt; 0
   * @param hi row number &ge; {@code lo}
   * 
   * @return a monotonically ascending list of numbers from <code>lo</code> to <code>hi</code>,
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
  
  
  
  //    - -  I N S T A N C E  -  M E T H O D S  - -  
  

  
  
  /**
   * Appends one or more hash entries to the end of the ledger.
   * 
   * 
   * @param entryHashes the input hash of the next {@linkplain Row row}
   * 
   * @return the new size of the ledger, or equivalently, the row number of the last
   * entry just added
   * 
   * @see #hashWidth()
   */
  public abstract long appendRows(ByteBuffer entryHashes);
  
  
  
  
  
  
  
  /**
   * Returns the number of rows in this ledger. Recall the row numbers are one-based,
   * so if the ledger is not empty, then the return value also represents that last existing
   * row number.
   * 
   * @return &ge; 0
   */
  public abstract long size();
  
  
  
  public boolean isEmpty() {
    return size() == 0;
  }
  
  

  /**
   * Returns the row with the given number.
   * 
   * @param rowNumber positive (&gt; 0), since the sentinel row is <em>abstract</em> (a row
   *                  whose hash is identically zero)
   * 
   * @return non-null
   */
  public abstract Row getRow(long rowNumber);
  
  
  /**
   * Returns the hash of the row at the given number.
   * 
   * @param rowNumber non-negative (&ge; 0), but note that row zero is a sentinel;
   *        the effective row numbers are 1-based
   * 
   * @return a possibly read-only buffer
   */
  public abstract ByteBuffer rowHash(long rowNumber);
  

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
   * Returns the skip-path from row 1 to the row numbered {@linkplain #size() size},
   * or <code>null</code> if this ledger is empty.
   */
  public Path statePath() {
    long size = size();
    return size == 0 ? null : skipPath(1, size);
  }

  
  
  /**
   * Returns the skip-path (the shortest string of rows) connecting the row with the
   * given <code>lo</code> number from the row with the given <code>hi</code> number.
   */
  public Path skipPath(long lo, long hi) {
    List<Row> rows = getRows(skipPathNumbers(lo, hi));
    return new Path(rows, false);
  }
  
  

  

  /**
   * Returns a path linking the given the target row numbers.
   * 
   * @param targets strictly ascending list of row numbers (&le; {@linkplain #size()})
   * 
   */
  public Path getPath(Long... targets) {
    return getPath(Arrays.asList(targets));
  }
  
  /**
   * Returns a path linking the given the target row numbers.
   * 
   * @param targets non-empty, strictly ascending list of row numbers (&le; {@linkplain #size()})
   */
  public Path getPath(List<Long> targets) {
    List<Long> stitched = stitch(targets);
    if (stitched.get(stitched.size() - 1) > size())
      throw new IllegalArgumentException(
          "targets out-of-bounds, size=" + size() + ": " + targets);
    
    var rows = getRows(stitched);
    return new Path(rows, null);
  }
  
  
  
  /**
   * Bulk {@code getRow} method. By default, this just invokes {@linkplain #getRow(long)}
   * in succession. Overridden when an implementation does better in bulk.
   * 
   * @param rns strictly ascending list of row numbers (&le; {@linkplain #size()})
   * 
   * @return not null
   * 
   * @throws IllegalArgumentException if {@code rns} are out-of-bounds, or not ascending
   */
  public List<Row> getRows(List<Long> rns) throws IllegalArgumentException {
    Row[] rows = new Row[rns.size()];
    long last = 0;
    for (int index = 0; index < rows.length; ++index) {
      long rn = rns.get(index);
      if (rn <= last)
        throw new IllegalArgumentException(
            "out-of-bounds/sequence row number " + rn + " at index " + index +
            ": " + rns);
      rows[index] = getRow(rn);
      last = rn;
    }
    return Lists.asReadOnlyList(rows);
  }
  
  
  
  /**
   * Trims the number of rows in the ledger to the given {@code size}.
   * Optional operation, not implemented by default.
   * 
   * @param newSize positive
   * 
   * @throws UnsupportedOperationException  if not implemented
   */
  public void trimSize(long newSize) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("on trimSize(" + newSize + ")");
  }
  
  
  
  
  
  
  /**
   * Releases resources. Does not throw checked exceptions.
   */
  @Override
  public void close() {  }

}
































