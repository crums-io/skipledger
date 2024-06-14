/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;

/**
 * Row backed by data in a {@linkplain RowBag}.
 * Note the base implementation computes the row's hash
 * from the input hash and previous rows' hashes.
 * It does not memo-ise the result.
 */
public class BaggedRow extends Row {
  
  private final long rowNo;
  private final LevelsPointer levelsPtr;

  private final RowBag bag;

  /**
   * Constructor does not validate that the bag indeed has the necessary data.
   * In order to create a subclass that does validate, invoke {@linkplain #hash()}
   * in the constructor.
   */
  public BaggedRow(long rowNo, RowBag bag) {
    this.rowNo = rowNo;
    this.levelsPtr = bag.levelsPointer(rowNo);
    this.bag = bag;
  }


  @Override
  public LevelsPointer levelsPointer() {
    return levelsPtr;
  }

  @Override
  public ByteBuffer inputHash() {
    return bag.inputHash(rowNo);
  }


  

}

