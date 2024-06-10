/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static java.util.Collections.binarySearch;

import static io.crums.sldg.SkipLedger.dupAndCheck;
import static io.crums.sldg.SkipLedger.skipCount;
import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.util.List;

import io.crums.util.Lists;
import io.crums.util.mrkl.Proof;

/**
 * Models the row's level hash-pointers. A row's hash value is derived
 * from the row's input-hash and the merklized hash of the previous
 * row-hashes (the so-called row's "levels"). This class offers 2 ways for
 * calculating a row's merklized levels' {@linkplain #hash() hash} (which
 * doesn't concern the input-hash):
 * <ol>
 * <li>Using <em>all</em> the levels' row-hashes.</li>
 * <li>Using a merkle-proof consisting of a <em>single</em> level row-hash,
 * and a "funnel" list of hashes.</li>
 * </ol>
 * <p>
 * Tho on average a skip ledger row's hash encapsulates only 2 hash pointers
 * to previous rows, a typical "skip path" has {@code log(n)^2} many
 * (32-byte) hash pointers (log in base 2)
 * (where {@code n} here is the difference of the highest and lowest row no. in
 * the path). Using the second option this class offers (its so called
 * {@linkplain #isCondensed() condensed} version), a skip path typically
 * contains {@code log(log(n)) x log(n)} many hashes.
 * </p>
 */
public class LevelsPointer {

  private final long rn;

  private final int level;

  private final ByteBuffer levelHash;

  private final List<ByteBuffer> hashes;


  /**
   * Creates a {@linkplain #isCondensed() condensed} instance.
   * 
   * @param rn          a {@linkplain SkipLedger#isCondensable(long) condensable} row no. 
   * @param level       level index {@code 0 <=level < SkipLedger.skipCount(rn)}
   * @param levelHash   hash of row numbered {@code rn - (1L << level)}
   *                    (not copied; do not modify contents)
   * @param funnel      not copied; do not modify
   * 
   */
  public LevelsPointer(
    long rn, int level, ByteBuffer levelHash, List<ByteBuffer> funnel) {
    
    this.rn = rn;
    this.level = level;
    this.levelHash = dupAndCheck(levelHash);
    this.hashes = funnel;

    // check args
    final int levels = skipCount(rn);
    if (level < 0 || level >= levels)
      throw new IndexOutOfBoundsException(level + ":" + skipCount(rn));

    if (SkipLedger.alwaysAllLevels(rn))
      throw new IllegalArgumentException("row [" + rn + "] is never condensed");
    
    if (Proof.funnelLength(levels, level) != funnel.size())
      throw new IllegalArgumentException(
        "expected funnel size for " + rn + ":" + level +
        " is " + Proof.funnelLength(levels, level) +
        "; actual size was " + funnel.size());
  }




  /**
   * Creates a full-info ("un-"{@linkplain #isCondensed() condensed}) instance.
   * Note the {@code prevHashes} parameter is indexed by <em>level</em>, so the
   * hashes are actually in <em>reverse</em> order of row number.
   * 
   * @param rn          row no.
   * @param prevHashes  level row hashes of size
   *                    {@link SkipLedger#skipCount(long)
   *                    SkipLedger.skipCount(rn)}
   */
  public LevelsPointer(long rn, List<ByteBuffer> prevHashes) {
    this.rn = rn;
    this.level = -1;
    this.levelHash = null;
    this.hashes = prevHashes;

    final int levels = skipCount(rn);
    if (prevHashes.size() != levels)
      throw new IllegalArgumentException(
        "expected prevHashes size " + skipCount(rn) +
        "; actual was " + prevHashes.size());

    if (rn == Long.highestOneBit(rn) &&
        !DIGEST.sentinelHash().equals(prevHashes.get(levels - 1)))
      throw new IllegalArgumentException(
        "hash for the sentinel row[0] not zeroed -- required by the protocol");
  }
  

  public final boolean isCondensed() {
    return level >= 0;
  }


  /**
   * Returns the row no. this instance generates the merklized pointer
   * {@linkplain #hash() hash} for.
   * 
   * @return a positive no.
   */
  public final long rowNo() {
    return rn;
  }



  /**
   * Returns the referenced row numbers in <em>ascending</em> order.
   * Referenced row no.s are those for which this instance has
   * (or claims to have) their hash. The returned list <em>does not include</em>
   * {@link #rowNo()}.
   * 
   * @return  a singleton no. or strictly ascending row no.s less than
   *          {@link #rowNo()}
   */
  public final List<Long> coverage() {
    final int skipCount = skipCount(rn);
    final int lastIndex = skipCount - 1;
    return
        isCondensed() ?
            List.of(rn - (1L << level)) :
            Lists.functorList(skipCount, li -> rn - (1L << (lastIndex - li)));
  }



  /**
   * Determines whether the hash of the given row no. is one of the
   * instance's level [hash] pointers.
   */
  public final boolean coversRow(long rn) {
    return indexOfCoverage(rn) >= 0;
  }


  private int indexOfCoverage(long rn) {
    return indexOf(coverage(), rn);
  }


  /** Negative values mean non-found, nothing more (ambiguous when -1). */
  private int indexOf(List<Long> coverage, long rn) {
    return
        coverage.size() < 12 ?
            coverage.indexOf(rn) :
            binarySearch(coverage, rn);
  }


  /**
   * 
   * @param level
   * @return
   */
  public ByteBuffer levelHash(int level) {
    if (isCondensed()) {
      if (this.level != level)
        throw new IndexOutOfBoundsException(
            "level arg: " + level + 
            "; available level: " + this.level + " (condensed info)");
      return levelHash();
    }
    return hashes.get(level).asReadOnlyBuffer();
  }
  

  /**
   * Returns the row hash of the given <em>covered row</em> no.
   * 
   * @throws IllegalArgumentException
   *         if {@code rn} is not a covered row no.
   * @see #coversRow(long)
   */
  public final ByteBuffer rowHash(long rn) {
    var coverage = coverage();
    int index = indexOf(coverage, rn);
    if (index < 0)
      throw new IllegalArgumentException(
          "row [" + rn + "not referenced by this: " + this);
    index = coverage.size() - index - 1;
    return isCondensed() ? levelHash() : hashes.get(index).asReadOnlyBuffer();
  }



  /**
   * Returns the level index. Invoke only if instance
   * {@linkplain #isCondensed() is condensed}.
   * 
   * @return {@code > 0} and {@code < SkipLedger.skipCount(rn)}
   * 
   * @throws UnsupportedOperationException
   *         if {@link #isCondensed()} returns {@code false}
   */
  public final int level() throws UnsupportedOperationException {
    assertCondensed();
    return level;
  }


  /**
   * Returns the level hash. Invoke only if instance
   * {@linkplain #isCondensed() is condensed}.
   * 
   * @return a new read-only view
   * 
   * @throws UnsupportedOperationException
   *         if {@link #isCondensed()} returns {@code false}
   */
  public final ByteBuffer levelHash() throws UnsupportedOperationException {
    assertCondensed();
    return levelHash.asReadOnlyBuffer();
  }


  /**
   * Returns the funnel. Invoke only if instance
   * {@linkplain #isCondensed() is condensed}.
   * The {@linkplain #hash()} (at row no. {@link #rowNo()}) is derived
   * from by the returned funnel hashes,
   * together with the {@link #level()}, and the {@link #levelHash()}.
   * 
   * @return a new read-only view
   * 
   * @throws UnsupportedOperationException
   *         if {@link #isCondensed()} returns {@code false}
   */
  public final List<ByteBuffer> funnel() throws UnsupportedOperationException {
    assertCondensed();
    return hashes();
  }



  private void assertCondensed() {
    if (!isCondensed())
      throw new UnsupportedOperationException(
          "instance not condensed: supported only for condensed instances");
  }


  /** Calculates and returns the levels-hash at the {@linkplain #rowNo() row no.} */
  public final ByteBuffer hash() {
    return
        isCondensed() ?
          SkipLedger.levelsMerkleHash(rn, level, levelHash.slice(), hashes()) :
          SkipLedger.levelsMerkleHash(hashes());
    
  }




  private List<ByteBuffer> hashes() {
    return Lists.functorList(
        hashes.size(),
        index -> hashes.get(index).asReadOnlyBuffer());
  }



}





