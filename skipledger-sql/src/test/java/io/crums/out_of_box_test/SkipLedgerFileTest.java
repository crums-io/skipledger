/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import java.io.File;

import org.junit.jupiter.api.Assertions;

import io.crums.io.Opening;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipTable;
import io.crums.sldg.fs.SkipLedgerFile;

/**
 * 
 */
public class SkipLedgerFileTest extends AbstractSkipLedgerTest {

  
  
  
  
  
  
  @Override
  protected SkipLedger newLedger(Object methodLabel) throws Exception {
    File file = getMethodOutputFilepath(methodLabel);
    return new SkipLedgerFile(file, Opening.CREATE_ON_DEMAND);
  }

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    Assertions.fail();
    return null;
  }

}
