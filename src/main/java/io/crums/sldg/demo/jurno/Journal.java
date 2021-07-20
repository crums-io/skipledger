/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;

import java.io.File;

import io.crums.sldg.HashLedger;
import io.crums.sldg.Ledger;

/**
 * 
 */
public class Journal extends Ledger {
  
  
  
  
  private final TextFileSource textSource;
  
  
  

  /**
   * @param textSource
   * @param hashLedger
   */
  public Journal(TextFileSource textSource, HashLedger hashLedger) {
    super(textSource, hashLedger);
    this.textSource = textSource;
  }
  
  
  
  public File getTextFile() {
    return textSource.getFile(); 
  }
  
  
  public int getLineNumber(long rowNumber) {
    return textSource.lineNumber(rowNumber);
  }
  
  
  
  

}
