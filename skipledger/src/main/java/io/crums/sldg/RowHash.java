/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.DIGEST;
import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.util.IntegralStrings;
import io.crums.util.hash.Digest;

/**
 * Superclass data type for {@linkplain Row} (and extracted from there).
 * Turns out, I need this--at least conceptually.
 */
public abstract class RowHash implements Digest {

  
  
  /**
   * Returns the row number.
   * 
   * @return &ge; 1
   * 
   * @see #equals(Object)
   */
  public abstract long rowNumber();
  
  /**
   * Returns the hash of this row.
   */
  public abstract ByteBuffer hash();
  

  /**
   * Returns the number of hash pointers in this row referencing previous rows.
   * These are called levels, because each successive {@linkplain #prevRowNumber(int)}
   * is twice as far away.
   * 
   * @return &ge; 1
   */
  public final int prevLevels() {
    return SkipLedger.skipCount(rowNumber());
  }
  
  /**
   * Returns the row number linked to at the given {@code level}.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #prevLevels()}
   * 
   * @return {@code rowNumber() - (1L << level)}
   */
  public final long prevRowNumber(int level) {
    long rn = rowNumber();
    Objects.checkIndex(level, SkipLedger.skipCount(rn));
    return rn - (1L << level);
  }
  

  

  /**
   * Equality semantics depend on {@linkplain #hash() hash}, and
   * {@linkplain #rowNumber() row number}.
   * 
   * @see #hashCode()
   */
  @Override
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    else if (o instanceof RowHash) {
      RowHash other = (RowHash) o;
      return other.rowNumber() == rowNumber() && other.hash().equals(hash());
    } else
      return false;
  }
  

  /**
   * Consistent with {@linkplain #equals(Object)}. Implemented for the Java
   * Collections classes.
   */
  @Override
  public final int hashCode() {
    return Long.hashCode(rowNumber());
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
    return rowNumber() + ":" + IntegralStrings.toHex(hash().slice().limit(3));
  }
  

  
  @Override
  public final int hashWidth() {
    return HASH_WIDTH;
  }

  @Override
  public final String hashAlgo() {
    return DIGEST.hashAlgo();
  }

  @Override
  public final MessageDigest newDigest() {
    return DIGEST.newDigest();
  }

  @Override
  public final ByteBuffer sentinelHash() {
    return DIGEST.sentinelHash();
  }
  

}
