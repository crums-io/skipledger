/*
 * Copyright 2020-2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SkipLedger.skipCount;
import java.nio.ByteBuffer;

import io.crums.util.Lists;

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


  public static ByteBuffer toDataBuffer(Row row) {
    if (row instanceof SerialRow sr)
      return sr.data();
    if (row.isCondensed())
      throw new IllegalArgumentException(
          "insufficient info, row [" + row.no() + "] is condensed: " + row);
    
    final int levels = row.prevLevels();
    final int cellsInRow = 1 + levels;
    int size = cellsInRow * SldgConstants.HASH_WIDTH;
    var buffer = ByteBuffer.allocate(size);
    buffer.put(row.inputHash());
    for (int level = 0; level < levels; ++level)
      buffer.put(row.prevHash(level));

    assert !buffer.hasRemaining();
    return buffer.flip().asReadOnlyBuffer();
  }
  
  
  
  private final long rowNumber;
  private final ByteBuffer data;
  private final LevelsPointer levelsPtr;
  

  /**
   * Constructs a new instance. This does a defensive copy (since we want a runtime
   * reference to guarantee immutability).
   * 
   * @param rowNumber   the row number
   * @param data        the data's backing data (a sequence of cells representing hashes)
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
    this.levelsPtr = copy.levelsPtr;
  }
  
  /**
   * Copy constructor.
   */
  public SerialRow(Row copy) {
    this(copy.no(), toDataBuffer(copy), false);
  }

  SerialRow(long rowNumber, ByteBuffer data, boolean ignored) {
    this.rowNumber = rowNumber; // (bounds checked below)
    this.data = data.slice();
    
    int cellsInRow = 1 + SkipLedger.skipCount(rowNumber); // (bounds checked)
    int expectedBytes = cellsInRow * SldgConstants.HASH_WIDTH;
    if (this.data.capacity() != expectedBytes)
      throw new IllegalArgumentException(
          "expected " + expectedBytes + " bytes for rowNumber " + rowNumber +
          "; actual given is " + data);

    this.levelsPtr = new LevelsPointer(
        rowNumber,
        Lists.functorList(skipCount(rowNumber), this::prevHashImpl));
  }
  
  
  public final LevelsPointer levelsPointer() {
    return levelsPtr;
  }
  
  
  
  public final ByteBuffer data() {
    return data.asReadOnlyBuffer();
  }



  @Override
  public final ByteBuffer inputHash() {
    return data().limit(SldgConstants.HASH_WIDTH);
  }
  

  private ByteBuffer prevHashImpl(int level) {
    int cellWidth = SldgConstants.HASH_WIDTH;
    int pos = (1 + level) * cellWidth;
    int limit = pos + cellWidth;
    return data().position(pos).limit(limit).slice();
  }

}
