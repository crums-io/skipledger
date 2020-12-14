/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import java.io.File;
import java.io.IOException;

import io.crums.io.Opening;
import io.crums.sldg.CompactLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;

/**
 * 
 */
public class CompactFileLedger extends CompactLedger {
  
  
  
  public CompactFileLedger(File file) throws IOException {
    this(file, Opening.CREATE_ON_DEMAND);
  }
  
  public CompactFileLedger(File file, Opening mode) throws IOException {
    super(new FileTable(file, mode));
  }

  
  @Override
  public Row getRow(long rowNumber) {
    return new SerialRow(super.getRow(rowNumber));
  }
  
  

}
