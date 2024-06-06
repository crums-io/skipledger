/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.util.IntegralStrings;

/**
 * Superclass data type for {@linkplain Row} (and extracted from there).
 * Turns out, I need this--at least conceptually.
 */
public abstract class RowHash {

  
  
  // /**
  //  * Returns the row number.
  //  * 
  //  * @return &ge; 1
  //  * 
  //  * @deprecated to be replaced by {@link #no()}
  //  * @see #equals(Object)
  //  */
  // public abstract long rowNumber();


  /**
   * Returns the row no.
   * 
   * @return &ge; 1
   * 
   * @see #equals(Object)
   */
  public abstract long no();
  
  /**
   * Returns the hash of this row.
   */
  public abstract ByteBuffer hash();
  

  /**
   * Returns the number of hash pointers in this row referencing previous rows.
   * These are called levels, because each successive {@linkplain #prevNo(int)}
   * is twice as far away.
   * 
   * @return &ge; 1
   */
  public final int prevLevels() {
    return SkipLedger.skipCount(no());
  }



  /**
   * Returns the [level] index of the highest non-sentinel row.
   * Usually this is just {@code prevLevels() - 1}.
   */
  public final int hiPtrLevel() {
    return SkipLedger.hiPtrLevel(no());
  }



  /**
   * Returns the row number linked to at the given {@code level}.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return {@code no() - (1L << level)}
   */
  public final long prevNo(int level) {
    long rn = no();
    Objects.checkIndex(level, SkipLedger.skipCount(rn));
    return rn - (1L << level);
  }
  

  

  /**
   * Equality semantics depend on {@linkplain #hash() hash}, and
   * row {@linkplain #no() no.}
   * 
   * @see #hashCode()
   */
  @Override
  public final boolean equals(Object o) {
    return o == this
        || o instanceof RowHash other
        && other.no() == no()
        && other.hash().equals(hash());
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}. Implemented for the Java
   * Collections classes.
   */
  @Override
  public final int hashCode() {
    return Long.hashCode(no());
  }
  

  /**
   * Returns a string of the form <em>rn</em>{@code :}<em>abreviated_hash</em>
   * where <em>rn</em> is the row number, and <em>abreviated_hash</em> is the 6
   * leftmost hexadecimal digits of the row's hash. E.g.
   * <p>
   * {@code 55:2e9a08}
   * </p>
   */
  @Override
  public String toString() {
    return no() + ":" + IntegralStrings.toHex(hash().slice().limit(3));
  }
  

  
  // @Override
  // public final int hashWidth() {
  //   return HASH_WIDTH;
  // }

  // @Override
  // public final String hashAlgo() {
  //   return DIGEST.hashAlgo();
  // }

  // @Override
  // public final MessageDigest newDigest() {
  //   return DIGEST.newDigest();
  // }

  // @Override
  // public final ByteBuffer sentinelHash() {
  //   return DIGEST.sentinelHash();
  // }
  

}
