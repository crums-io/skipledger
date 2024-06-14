/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static java.util.Collections.binarySearch;
import static io.crums.sldg.SkipLedger.alwaysAllLevels;
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
 * never involves the input-hash):
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
 * contains {@code log(log(n)) x log(n)} many 32-byte hashes.
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
    this.hashes = dupAndCheck(funnel);

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
   *                    SkipLedger.skipCount(rn)}; contents not copied:
   *                    do not modify
   */
  public LevelsPointer(long rn, List<ByteBuffer> prevHashes) {
    this.rn = rn;
    this.level = -1;
    this.levelHash = null;
    this.hashes = dupAndCheck(prevHashes);

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



  /**
   * Package-access (trusted caller), full-info constructor.
   * (Offers a way to lazy-load the hashes using a functor-list.)
   * 
   * @param rn          row no.
   * @param prevHashes  <em>neither copied not verified</em>;
   *                    a perhaps functor list
   * @param trustMe     un-used constructor disambiguator
   */
  LevelsPointer(long rn, List<ByteBuffer> prevHashes, boolean trustMe) {
    this.rn = rn;
    this.level = -1;
    this.levelHash = null;
    this.hashes = prevHashes;
  }



  /**
   * Package-access (trusted caller), condensed-instance constructor.
   * (Offers a way to lazy-load the hashes using a functor-list.)
   * 
   * @param rn          row no.
   * @param prevHashes  <em>neither copied not verified</em>;
   *                    a perhaps functor list
   * @param trustMe     un-used constructor disambiguator
   */
  LevelsPointer(
      long rn, int level, ByteBuffer levelHash, List<ByteBuffer> funnel,
      boolean trustMe) {
    this.rn = rn;
    this.level = level;
    this.levelHash = levelHash;
    this.hashes = funnel;
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
   * Determines whether the instance uses a condensed hash proof.
   * When condensed, the instance references the hash of a single
   * previous level. (Technically, these being Merkle proofs, there
   * are usually 2 level row hashes referenced in each proof; for now
   * we haven't supplied a method to convert one condensed version
   * into the other).
   * 
   * @see #isCompressed()
   */
  public final boolean isCondensed() {
    return level >= 0;
  }


  /**
   * Determines whether the instance is compressed. This codifies
   * when an uncondensed instance may be condensed. Specifically,
   * if no more than two (non-sentinel) level row-hashes are used
   * to calculate the level merkle root hash, there is no point
   * in "condensing" the representation.
   * 
   * @return  {@code SkipLedger.alwaysAllLevels(rowNo()) || isCondensed()}
   */
  public final boolean isCompressed() {
    return alwaysAllLevels(rn) || isCondensed();
  }



  /**
   * Returns a compressed version of this instance referencing
   * the given level row no.
   * 
   * @param levelRn   one of {@link #coverage()}
   * 
   * @see #compressToLevel(int)
   */
  public final LevelsPointer compressToLevelRowNo(long levelRn) {
    var coverage = coverage();
    int index = indexOf(coverage, levelRn);
    if (index < 0)
      throw new IllegalArgumentException(levelRn + " not covered; " + this);
    
    int level = skipCount(rn) - 1 - index;
    return compressToLevel(level);
  }


  /**
   * Returns a compressed version of this instance for the specified level.
   * This method fails if the instance is already <em>condensed</em>
   * at a different level.
   * 
   * <h4>Implementation Note</h4>
   * <p>
   * Condensed instances at adjacent {@code even:odd} levels, eg (0,1) or (6,7),
   * share exactly the same hash data. We could derive one from the other,
   * but for now, it's not a priority.
   * </p>
   * 
   * @param level       level index {@code 0 <=level < SkipLedger.skipCount(rowNo())}
   * @return            this instance, if the instance is already compressed;
   *                    a <em>condensed</em> version of this instance, o.w.
   */
  public final LevelsPointer compressToLevel(int level) {
    
    if (isCompressed()) {
      if (isCondensed() && this.level != level)
        throw new IllegalArgumentException(
            "level " + level + " not covered; condensed at level() " +
            this.level + "; " + this);
      return this;
    }

    // assert isCondensable(rn); too obvious

    var fun = SkipLedger.levelsMerkleFunnel(level, hashes());
    var levelHash = hashes.get(level).asReadOnlyBuffer();

    return new LevelsPointer(rn, level, levelHash, fun, true);
  }





  /**
   * Returns the referenced row numbers in <em>ascending</em> order.
   * Referenced row no.s are those for which this instance has
   * (or claims to have) their hash. The returned list <em>does not include</em>
   * {@link #rowNo()}.
   * 
   * @return  not empty, strictly ascending row no.s, &lt; {@link #rowNo()}
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
   * Determines whether the hash of the row at the given row no. is one of the
   * instance's level [hash] pointers.
   * 
   * @param rn    the row no. tested for coverage
   */
  public final boolean coversRow(long rn) {
    return indexOfCoverage(rn) >= 0;
  }


  /**
   * Determines whether the hash of row at the given no. is one of the
   * instance's level [hash] pointers.
   */
  public final boolean coversLevel(int level) {
    if (this.level == level)
      return true;
    
    if (level < 0 || level >= skipCount(rn))
      throw new IllegalArgumentException("level: " + level + "; " + this);
    
    return !isCondensed();
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



  @Override
  public String toString() {
    return "[" + rn + "] --> " + coverage();
  }



}





