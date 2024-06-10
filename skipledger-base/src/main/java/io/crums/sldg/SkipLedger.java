/*
 * Copyright 2020-2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.buffer.BufferUtils;
import io.crums.util.EasyList;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Strings;
import io.crums.util.hash.Digest;
import io.crums.util.mrkl.FixedLeafBuilder;
import io.crums.util.mrkl.Proof;
import io.crums.util.mrkl.Tree;

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



  public static ByteBuffer rowHash(
      long rn, ByteBuffer inputHash, List<ByteBuffer> prevHashes) {
    
    final int levels = skipCount(rn);

    final boolean power2 = rn == 1L << (levels - 1);


    
    final int pSize = prevHashes.size();

    if (pSize != levels)
      throw new IllegalArgumentException(
          "expected " + Strings.nOf(levels, "hash") + " for row [" + rn +
          "]; actual was " + Strings.nOf(pSize, "hash"));

    if (power2 &&
        !SldgConstants.DIGEST.sentinelHash().equals(
            prevHashes.get(levels - 1)))
      
      throw new IllegalArgumentException(
          "expected sentinel at last prevHashes for row [" + rn + "]");
    
    
    return rowHash(inputHash, levelsMerkleHash(prevHashes));
  }



  public static ByteBuffer rowHash(
      ByteBuffer inputHash, ByteBuffer levelsMerkleHash) {
    
    var digest = DIGEST.newDigest();
    digest.update(dupAndCheck(inputHash));
    digest.update(levelsMerkleHash);
    return ByteBuffer.wrap(digest.digest()).asReadOnlyBuffer();
  }



  /**
   * 
   * @param levelHash   hash of the row numbered {@code rn - (1L << level)}
   * 
   * @see #levelsMerkleFunnel(int, List)
   */
  public static ByteBuffer rowHash(
      long rn, ByteBuffer inputHash,
      int level, ByteBuffer levelHash, List<ByteBuffer> funnel) {

    return
      rowHash(
        inputHash,
        levelsMerkleHash(rn, level, levelHash, funnel));
  }



  public static ByteBuffer levelsMerkleHash(
      long rn, int level, ByteBuffer levelHash, List<ByteBuffer> funnel) {
    
    int levelCount = skipCount(rn);
    Objects.checkIndex(level, levelCount);

    if (levelCount == 1) {
      if (!funnel.isEmpty())
        throw new IllegalArgumentException(
            "expected empty funnel for rn [" + rn + "]; actual size: " +
            funnel.size());
      
      return dupAndCheck(levelHash);
    }

    return ByteBuffer.wrap(
        Proof.merkleRoot(
          levelHash,
          level,
          levelCount,
          funnel,
          DIGEST.newDigest())).asReadOnlyBuffer();
  }






  /**
   * Returns the merkle funnel for the given previous row-hashes.
   * The returned list's size is on the order of log (base 2) of
   * the original size.
   * 
   * @param level       level index
   * @param prevHashes  not empty and fewer than 64
   * 
   * @return empty if {@code prevHashes} is a singleton; not-empty, o.w.
   */
  public static List<ByteBuffer> levelsMerkleFunnel(
      int level, List<ByteBuffer> prevHashes) {
    
    final int levelCount = prevHashes.size();
    Objects.checkIndex(level, levelCount);
    if (levelCount == 1)
      return List.of();
    
    return new Proof(levelsMerkleTree(prevHashes), level).funnel();
  }




  /**
   * Returns the merkle root hash of the given previous-row-hashes.
   * If the collection is a singleton, then the single hash is
   * returned as is (but validated for length).
   * 
   * @param prevHashes not empty, and fewer than 64
   */
  public static ByteBuffer levelsMerkleHash(List<ByteBuffer> prevHashes) {

    switch (prevHashes.size()) {
    case 0:   throw new IllegalArgumentException("empty prevHashes");
    case 1:   return dupAndCheck(prevHashes.get(0));
    default:
              return
                ByteBuffer.wrap(levelsMerkleTree(prevHashes).hash())
                .asReadOnlyBuffer();
    }
    
  }




  static Tree levelsMerkleTree(List<ByteBuffer> prevHashes) {
    int len = prevHashes.size();
    if (len < 2)
      throw new IllegalArgumentException(
          "too few level hashes: " + len + " (min 2)");
    if (len > 63)
      throw new IllegalArgumentException(
          "too many level hashes: " + len + " (max 63)");

          var builder =
          new FixedLeafBuilder(SldgConstants.DIGEST.hashAlgo(), false);
  
    byte[] hash = new byte[SldgConstants.HASH_WIDTH];
    for (var p : prevHashes) {
      dupAndCheck(p).get(hash);
      builder.add(hash);
    }
    return builder.build();
  }







  /**
   * Returns the deepest non-zero hash-pointer from the given
   * list of level hashes. With one exception ({@code rn = 1L}), this always
   * returns a 32-byte buffer; row [1] is the <em>only</em> row that
   * doesn't have such a hash pointer.
   * 
   * @param rn          &ge; 1
   * @param prevHashes  of size {@link #skipCount(long) skipCount(rn)},
   *                    unless the last hash is sentinel (zeroes), in
   *                    which case, it may
   * 
   * @see #hiPtrLevel(long)
   * 
   * @throws IndexOutOfBoundsException
   *         if {@code prevHashes} has fewer than
   *         {@link #hiPtrLevel(long) hiPtrLevelIndex(rn) + 1} hashes
   */
  public static ByteBuffer hiPtrHash(long rn, List<ByteBuffer> prevHashes) {
    int index = hiPtrLevel(rn);
    return
        index < 0 ?
            BufferUtils.NULL_BUFFER :
            dupAndCheck(prevHashes.get(index));
  }


  /**
   * Tests whether all level-row hashes are always present in a
   * row's row-hash calculation. If row's link-pointer-hash is
   * determined by 2 or fewer non-sentinel hashes, then we don't
   * (can't) condense it. This test, then, is equivalent to testing
   * whether the row [no.] has 2 or fewer levels, or is the corner case
   * row [4] which has 3 levels (in order of level, to rows 3, 2, and 0),
   * the last of which, is the sentinel row (whose hash is already known).
   * 
   * @param rn  row no.
   * @return    {@code skipCount(rn) <= 2 || rn == 4L}
   * @see #isCondensable(long)
   */
  public static boolean alwaysAllLevels(long rn) {
    // slightly faster evaluation possible, but not worth
    // the obfuscation / cognitive load
    return skipCount(rn) <= 2 || rn == 4L;
  }



  /**
   * Test whether a row at the row number is condensable. Only row no.s
   * with a lot of levels are compressable.
   * 
   * @param rn  row no.
   * @return    {@code !alwaysAllLevels(rn)}
   * 
   * @see #alwaysAllLevels(long)
   */
  public static boolean isCondensable(long rn) {
    return !alwaysAllLevels(rn);
  }


  /**
   * Returns the number of funnels in the given list of row numbers.
   * The argurment is assumed to be the row no.s in a path, but this
   * is not enforced.
   * 
   * @param rowNos  positive row no.s
   * @return        count of how many of the given row no.s are
   *                {@linkplain #isCondensable(long) condensable}
   */
  public static int countFunnels(List<Long> rowNos) {
    return (int) rowNos.stream().filter(SkipLedger::isCondensable).count();
  }



  /**
   * Returns the row no.s that are condensable. In a condensed path,
   * these are the row no.s that link to previous rows via funnels.
   */
  public static List<Long> filterFunneled(List<Long> rowNos) {
    return rowNos.stream().filter(SkipLedger::isCondensable).toList();
  }



  /**
   * Returns the funnel length between the given row no.s.
   * A funnel is a sequence of SHA-256 hashes which can be
   * combined the hash of row [{@code loRn}] to generate a
   * the link-pointers hash for row [{@code hiRn}] (which
   * together with row [{@code hiRn}]'s input-hash can be
   * used to derive the hi-row's final hash).
   * 
   * @param loRn    low row no. &ge; 0, tho seldom zero in pracice,
   *                linked/referenced by {@code hiRn}
   * @param hiRn    hi row no. &gt; {@code loRn}
   * 
   * @return  the no. of hash cells in the funnel
   * @throws  IllegalArgumentException
   *          if the row no.s are not linked, or if row
   *          [{@code hiRn}], not 
   *          {@linkplain #isCondensable(long) condensable}, or
   *          o.w. invalid (wrong order, negative, etc.)
   */
  public static int funnelLength(long loRn, long hiRn) {
    final int levels = skipCount(hiRn);
    if (levels <= 2 || hiRn == 4L)
      throw new IllegalArgumentException(
          "hiRn [" + hiRn + "] cannot be funneled");

    final long diff = hiRn - loRn;
    if (diff != Long.highestOneBit(diff)) // not a power of 2
      throw new IllegalArgumentException(
          "row no.s not linked: " + loRn + ", " + hiRn);

    final int level = Long.numberOfTrailingZeros(diff);
    if (level >= levels)
      throw new IllegalArgumentException(
          "row no.s not linked (too far): " + loRn + ", " + hiRn);

    return Proof.funnelLength(levels, level);
  }



  /**
   * Returns highest level (deepest) index that <em>does not reference
   * row zero</em>, the sentine hash. With one exception [1], every row no.
   * references a previous row.
   * 
   * @param rn    row no. &ge; 1
   * @return {@code -1} if {@code rn} equals {@code 1}; non-negative, o.w.
   */
  public static int hiPtrLevel(long rn) {
    final int levels = skipCount(rn);
    final boolean power2 = rn == (1L << (levels - 1));
    assert power2 == (rn == Long.highestOneBit(rn));
    return power2 ? levels - 2 : levels - 1;
  }



  /**
   * Returns the <em>smallest</em>, non-zero referenced row no. If
   * there is no such reference, then zero is returned. But note, the
   * only case this happens is for the origin no. (1L).
   * 
   * @param rn    row no. &ge; 1
   * @return  {@code rn == 1L ? 0 : rn - (1L << hiPtrLevel(rn))}
   * @see #hiPtrLevel(long)
   */
  public static long hiPtrNo(long rn) {
    return rn == 1L ? 0 : rn - (1L << hiPtrLevel(rn));
  }





  public static ByteBuffer dupAndCheck(ByteBuffer buffer) {
    var out = buffer.slice();
    if (out.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(
        "expected " + SldgConstants.HASH_WIDTH + " remaining bytes: " + buffer);
    return out;
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
    return levelLinked(rowNumA, rowNumB) >= -1;
  }



  /**
   * Returns the zero-based level-index linking the two given row numbers.
   * 
   * @param rowNumA &ge; 0 (zero OK!)
   * @param rowNumB &ge; 0
   * 
   * @return        -2, if not linked; -1, if the same row no.s;
   *                non-negative (linked), o.w.
   */
  public static int levelLinked(long rowNumA, long rowNumB) {
    final long lo, hi;
    
    if (rowNumA <= rowNumB) {
      lo = rowNumA;
      hi = rowNumB;
    } else {
      lo = rowNumB;
      hi = rowNumA;
    }
    if (lo < 0)
      throw new IllegalArgumentException(
          "negative row no.: " + rowNumA + ", " + rowNumB);

    final long diff = hi - lo;
    
    if (diff == 0L)
      return -1;  // self

    // must be a power of 2
    if (diff != Long.highestOneBit(diff))
      return -2;

    
    int level = Long.numberOfTrailingZeros(diff);
    
    // the hi row no. must have enuf levels
    if (level >= skipCount(hi))
      return -2;
    
    return level;
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
   * Returns the row numbers that the given bag of numbers implicitly reference
   * and which are <em>not</em> already in the bag. Note, the argument is usually
   * the row no.s in a {@linkplain Path}. (And the only reason it's a
   * collection instead of an ordered list, is cuz I'm lazy and don't want to
   * validate.)
   * 
   * @param rowNos not-empty, bag of row numbers, each &ge; 1. May contain duplicates.
   * @return a set of referenced row numbers not already in the bag. May include
   *         zero (the sentinel row).
   */
  public static SortedSet<Long> refOnlyCoverage(Collection<Long> rowNos) {
    TreeSet<Long> fullSet = new TreeSet<>(rowNos);  // not changed hereafter
    TreeSet<Long> refOnly = new TreeSet<>();
    if (fullSet.isEmpty())
      throw new IllegalArgumentException("empty rowNos");
    
    for (long rn : fullSet)
      collectRefs(rn, refOnly, fullSet);
    return Collections.unmodifiableSortedSet(refOnly);
  }


  /**
   * Returns the row numbers that the given bag of to-be-stitched numbers
   * implicitly reference in the condensed representation of a path. These
   * are those row no.s which are (i) <em>not</em> in the [post-stitched]
   * bag and (ii) are not funneled. When a path is condensed, almost all
   * its rows' hashes are linked by "funnels";
   * 
   * 
   * @param rowNos    positive, strictly increasing row no.s defining the [path]
   *                  bag (unpacked after stitching)
   * 
   * @see #countFunnels(List)
   */
  public static SortedSet<Long> refOnlyCondensedCoverage(List<Long> rowNos) {
    SortedSet<Long> fullSet = Sets.sortedSetView(stitch(rowNos));
    TreeSet<Long> refOnly = new TreeSet<>();

    // we must decide what the first row links to.
    // (we could just use the link-pointer's merkle root,
    // but for now, we exclude it in the implementation.)
    // So we set, somewhat arbitrarily, references to the
    // *nearest row no.s by the first row

    final Long loRn = fullSet.first();
    {
      if (alwaysAllLevels(loRn))
        collectRefs(loRn, refOnly, Set.of());
      else 
        // pick the *highest* referenced row no.
        // (which is the row no. at rn's level zero)
        refOnly.add(loRn - 1);
    }
    


    for (long rn : fullSet.tailSet(loRn + 1L)) {
      if (isCondensable(rn))
        continue; // since the full-set is stitched, the
                  // previous row no. in the full-set
                  // is referenced by rn, so we won't
                  // need any more references (row hashes)!
                  // (the prev row no. will be linked via a merkle "funnel")
      
      // unless, it doesn't pay to merklize..
      collectRefs(rn, refOnly, fullSet);
    }
    
    return Collections.unmodifiableSortedSet(refOnly);
  }


  private static void collectRefs(
      long rn, TreeSet<Long> refOnly, Set<Long> fullSet) {

    final int levels = skipCount(rn);
    // assert levels <= 2 || rn == 4L;

    for (int e = 0; e < levels; ++e) {
      long delta = 1L << e;
      Long refRn = rn - delta;
      assert refRn >= 0;
      if (!fullSet.contains(refRn))
        refOnly.add(refRn);
    }
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
      throw new IllegalArgumentException("empty row no.s");
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
   * 
   * @see #stitchCompress(List)
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
































