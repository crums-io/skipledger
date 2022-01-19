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
public class BaggedRow extends Row {
  
  private final long rowNumber;
  private final RowBag bag;

  /**
   * Constructor does not validate that the bag indeed has the necessary data.
   * In order to create a subclass that does validate, invoke {@linkplain #hash()}
   * in the constructor.
   */
  public BaggedRow(long rowNumber, RowBag bag) {
    this.rowNumber = rowNumber;
    this.bag = Objects.requireNonNull(bag, "null bag");
    SkipLedger.checkRealRowNumber(rowNumber);
  }

  @Override
  public final long rowNumber() {
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

