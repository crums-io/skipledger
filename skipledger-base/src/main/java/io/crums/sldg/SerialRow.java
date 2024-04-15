/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A self-contained row laid out in serial form.
 */
public class SerialRow extends Row {

  
  /**
   * Returns the given row as an instance of this class, if it already
   * isn't one.
   */
  public static SerialRow toInstance(Row row) {
    return (row instanceof SerialRow) ? (SerialRow) row : new SerialRow(row);
  }
  
  
  
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
   * Copy / promotion constructor.
   */
  public SerialRow(SerialRow copy) {
    this.rowNumber = copy.rowNumber;
    this.data = copy.data;
  }
  
  /**
   * Copy constructor.
   */
  public SerialRow(Row copy) {
    this(copy.rowNumber(), copy.data(), false);
  }

  SerialRow(long rowNumber, ByteBuffer data, boolean ignored) {
    this.rowNumber = rowNumber; // (bounds checked below)
    this.data = data.slice();
    
    int cellsInRow = 1 + SkipLedger.skipCount(rowNumber); // (bounds checked)
    int expectedBytes = cellsInRow * hashWidth();
    if (this.data.capacity() != expectedBytes)
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
