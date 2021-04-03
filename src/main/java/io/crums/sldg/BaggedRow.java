/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.sldg.bags.RowBag;

/**
 * Row backed by data in a {@linkplain RowBag}. This is a departure
 * (loosening up) from previous guarantees. Oh well, it's convenient,
 * plus other classes like {@linkplain Path} validate data on construction,
 * besides.
 */
public class BaggedRow extends BaseRow {
  
  private final long rowNumber;
  private final RowBag bag;
  
  
  public BaggedRow(long rowNumber, RowBag bag) {
    this.rowNumber = rowNumber;
    this.bag = Objects.requireNonNull(bag, "null bag");
    
    if (rowNumber < 1)
      throw new IllegalArgumentException("rowNumber out-of-bounds: " + rowNumber);
  }

  @Override
  public long rowNumber() {
    return rowNumber;
  }

  @Override
  public ByteBuffer inputHash() {
    return bag.inputHash(rowNumber);
  }

  @Override
  public ByteBuffer prevHash(int level) {
    Objects.checkIndex(level, prevLevels());
    long referencedRowNum = rowNumber - (1L << level);
    return bag.rowHash(referencedRowNum);
  }

}
