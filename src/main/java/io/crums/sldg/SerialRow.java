/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A row loaded from serial form.
 */
public class SerialRow extends BaseRow {

  
  
  
  private final long rowNumber;
  private final ByteBuffer data;
  

  /**
   * Constructs a new instance. This does a defensive copy (since we want a runtime
   * reference to be guarantee immutability).
   * 
   * @param rowNumber the data number
   * @param data       the data's backing data (a sequence of cells representing hashes)
   */
  public SerialRow(long rowNumber, ByteBuffer data) {
    this(rowNumber, ByteBuffer.allocate(data.remaining()).put(data).flip(), false);
  }
  
  /**
   * Copy constructor.
   */
  public SerialRow(Row copy) {
    this.rowNumber = copy.rowNumber();
    this.data = copy.data();
    // we can do above cuz subclass cannot be defined outside package
  }

  SerialRow(long rowNumber, ByteBuffer data, boolean ignored) {
    this.rowNumber = rowNumber; // (bounds checked below)
    int remaining = Objects.requireNonNull(data, "null data").remaining();
    if (remaining != data.capacity())
      data = data.slice();
    this.data = data;
    
    int cellsInRow = 1 + SkipLedger.skipCount(rowNumber); // (bounds checked)
    int expectedBytes = cellsInRow * hashWidth();
    if (remaining != expectedBytes)
      throw new IllegalArgumentException(
          "expected " + expectedBytes + " bytes for rowNumber " + rowNumber +
          "; actual given is " + data);
  }
  
  
  
  
  
  
  @Override
  public final ByteBuffer data() {
    return data.asReadOnlyBuffer();
  }

  @Override
  public final long rowNumber() {
    return rowNumber;
  }

  @Override
  public final ByteBuffer inputHash() {
    return data().limit(hashWidth());
  }

  @Override
  public final ByteBuffer prevHash(int level) {
    Objects.checkIndex(level, prevLevels());
    int cellWidth = hashWidth();
    int pos = (1 + level) * cellWidth;
    int limit = pos + cellWidth;
    return data().position(pos).limit(limit).slice();
  }

}
