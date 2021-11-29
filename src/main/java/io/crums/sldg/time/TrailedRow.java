/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.time;

import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.sldg.RowHash;

/**
 * A row's witness record is a row-number/crumtrail tuple.
 */
public class TrailedRow extends RowHash {
  
  private final long rowNumber;
  private final CrumTrail trail;
  

  /**
   * 
   * @param rowNumber &ge; 1
   * @param trail non-null crumtrail whose crum-hash
   *              ({@linkplain CrumTrail#crum() trail.crum()}.{@linkplain Crum#hash() hash()})
   *              is the row-hash
   */
  public TrailedRow(long rowNumber, CrumTrail trail) {
    this.rowNumber = rowNumber;
    this.trail = Objects.requireNonNull(trail, "null trail");
    
    if (rowNumber < 1)
      throw new IllegalArgumentException("rowNumber: " + rowNumber);
  }

  /**
   * Returns the row number.
   */
  public final long rowNumber() {
    return rowNumber;
  }

  /**
   * Returns the witness evidence.
   */
  public final CrumTrail trail() {
    return trail;
  }
  
  
  /**
   * Returns the UTC time the row was witnessed.
   * 
   * @return {@code trail().crum().utc()}
   */
  public final long utc() {
    return trail.crum().utc();
  }
  
  
  /**
   * Returns the row hash (the hash witnessed in the crumtrail).
   */
  @Override
  public final ByteBuffer hash() {
    return trail.crum().hash().slice();
  }
  
  

}
