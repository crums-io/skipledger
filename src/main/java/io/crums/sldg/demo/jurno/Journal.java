/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import io.crums.io.FileUtils;
import io.crums.io.Opening;
import io.crums.sldg.HashLedger;
import io.crums.sldg.Ledger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.fs.HashLedgerDir;
import io.crums.sldg.fs.TableSaltFile;
import io.crums.sldg.src.TableSalt;
import io.crums.util.TaskStack;

/**
 * 
 */
public class Journal extends Ledger {
  
  
  public final static String SALT_SEED_FILE = "SECRET_SEED";

  
  private final static String EXTENSION = SldgConstants.DB_EXT;
  
  
  
  private final TextFileSource textSource;
  
  
  
  public static Journal loadInstance(File textFile, Opening mode, File hashLedgerDir) {
    
    Objects.requireNonNull(textFile, "null textFile");
    Objects.requireNonNull(mode, "null mode");
    if (!textFile.isFile())
      throw new IllegalArgumentException("not a file: " + textFile);
    if (hashLedgerDir == null)
      hashLedgerDir = new File(textFile.getPath() + EXTENSION);
    if(hashLedgerDir.isFile())
      throw new IllegalArgumentException(
          "hash ledger directory is a file: " + hashLedgerDir);
    
    FileUtils.ensureDir(hashLedgerDir);
    File seedFile = new File(hashLedgerDir, SALT_SEED_FILE);
    
    try (TaskStack onFailCloser = new TaskStack()) {
      
      TableSalt shaker = TableSaltFile.loadInstance(seedFile, mode);
      onFailCloser.pushClose(shaker);
      
      TextFileSource textSource = new TextFileSource(textFile, shaker);
      onFailCloser.pushClose(textSource);
      
      HashLedgerDir hashLedger = new HashLedgerDir(hashLedgerDir, mode, true);
      onFailCloser.pushClose(hashLedger);
      
      Journal journal = new Journal(textSource, hashLedger);
      onFailCloser.clear();
      
      return journal;
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loadInstance( " + textFile + " , " + mode + ", " + hashLedgerDir + " ): " + iox,
          iox);
    }
  }
  

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
  
  
  public int getForkedLineNumber() {
    long firstConflict = getFirstConflict();
    return firstConflict == 0 ? 0 : textSource.lineNumber(firstConflict);
  }
  
  
  
  

}
