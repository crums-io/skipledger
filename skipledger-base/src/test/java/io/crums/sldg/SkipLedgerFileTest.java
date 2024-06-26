/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.io.File;

import org.junit.jupiter.api.Assertions;

import io.crums.io.Opening;

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
