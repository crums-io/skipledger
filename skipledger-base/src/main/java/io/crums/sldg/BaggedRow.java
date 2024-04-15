/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Row backed by data in a {@linkplain RowBag}.
 * Note the base implementation computes the row's hash
 * from the input hash and previous rows' hashes.
 * It does not memo-ise the result.
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

