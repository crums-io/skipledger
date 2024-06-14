/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import static io.crums.sldg.SkipLedger.levelLinked;

import java.nio.ByteBuffer;

/**
 * 
 */
public class CondensedRow extends Row {


  public static Row compressToLevelRowNo(Row row, long levelRn) {
    
    int level = levelLinked(levelRn, row.no());
    if (level < 0)
      throw new IllegalArgumentException(
          "levelRn " + levelRn + " not covered by row " + row);

    if (row.levelsPointer().coversLevel(level))
      return row.isCompressed() ?
          row : new CondensedRow(row, level, true);

    throw new IllegalArgumentException(
        "levelRn " + levelRn + " not covered by row " + row.levelsPointer());
  }





  private final Row row;

  private final int refLevel;


  private CondensedRow(Row row, int level, boolean trustMe) {
    this.row = row;
    this.refLevel = level;
  }

  public CondensedRow(Row row, int level) {
    this(row, level, true);

    LevelsPointer levelsPtr = row.levelsPointer();
    if (!levelsPtr.coversLevel(level))
      throw new IllegalArgumentException(
          "level " + level + " not covered: " + levelsPtr);
  }


  @Override
  public LevelsPointer levelsPointer() {
    return row.levelsPointer().compressToLevel(refLevel);
  }

  @Override
  public ByteBuffer inputHash() {
    return row.inputHash();
  }


} 
