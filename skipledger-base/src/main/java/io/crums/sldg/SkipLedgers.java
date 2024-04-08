/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.io.File;
import java.io.IOException;

import io.crums.io.Opening;

/**
 * 
 */
public class SkipLedgers {

  // static-only
  private SkipLedgers() {  }
  
  
  public static SkipLedger newFileInstance(File file, Opening mode, boolean lazy) throws IOException {
    return new SkipLedgerFile(file, mode, lazy);
  }
  
  
  public static SkipLedger inMemoryInstance() {
    return new CompactSkipLedger(new VolatileTable());
  }

}
