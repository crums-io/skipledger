/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SkipLedger.dupAndCheck;
import static io.crums.sldg.SkipLedger.skipCount;

import java.nio.ByteBuffer;
import java.util.List;

import io.crums.util.Lists;
import io.crums.util.mrkl.Proof;

/**
 * 
 */
public class LevelsPointer {

  private final long rn;

  private final int level;

  private final ByteBuffer levelHash;

  private final List<ByteBuffer> hashes;


  /**
   * Creates a {@linkplain #isCondensed() condensed} instance.
   * 
   * @param rn          row no.
   * @param level       {@code 0 <= level < SkipLedger.skipCount(rn)}
   * @param levelHash   hash of row numbered {@code rn - (1L << level)}
   *                    (not copied; do not modify contents)
   * @param funnel      not copied; do not modify
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
    
    if (Proof.funnelLength(levels, level) != funnel.size())
      throw new IllegalArgumentException(
        "expected funnel size for " + rn + ":" + level +
        " is " + Proof.funnelLength(levels, level) +
        "; actual size was " + funnel.size());
  }




  public LevelsPointer(long rn, List<ByteBuffer> prevHashes) {
    this.rn = rn;
    this.level = -1;
    this.levelHash = null;
    this.hashes = prevHashes;

    if (prevHashes.size() != skipCount(rn))
      throw new IllegalArgumentException(
        "expected prevHashes size " + skipCount(rn) +
        "; actual was " + prevHashes.size());
  }
  

  public final boolean isCondensed() {
    return level >= 0;
  }


  public final long rowNo() {
    return rn;
  }



  /**
   * Returns the referenced row numbers in <em>descending</em> order.
   * Referenced row no.s are those for which this instance has
   * (or claims to have) their hash. The returned list <em>does not include</em>
   * {@link #rowNo()}.
   * 
   * @return  a singleton no. or strictly descending row no.s less than
   *          {@link #rowNo()}
   */
  public final List<Long> coverage() {
    return
        isCondensed() ?
            List.of(rn - (1L << level)) :
            Lists.functorList(skipCount(rn), li -> rn - (1L << li));
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


  public final List<ByteBuffer> funnel() throws UnsupportedOperationException {
    assertCondensed();
    return hashes();
  }



  private void assertCondensed() {
    if (!isCondensed())
      throw new UnsupportedOperationException(
          "instance not condensed: supported only for condensed instances");
  }



  public ByteBuffer hash() {
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





