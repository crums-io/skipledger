/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.fs;


import static io.crums.sldg.SldgConstants.DB_LEDGER;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import io.crums.io.Opening;
import io.crums.sldg.HashLedger;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.TaskStack;

/**
 * A {@linkplain HashLedger} with direct storage on the file system. Naturally,
 * this is much faster than on a general purpose SQL engine.
 */
public class HashLedgerDir implements HashLedger {
  

  private final File dir;
  private final SkipLedger skipLedger;
  private WitRepo witnessRepo;
  
  
  
  
  

  /**
   * Creates an instance implemented using an underlying {@linkplain SkipLedgerFile}
   * and {@linkplain WitRepo} in the given directory.
   * 
   * @param dir   directory path where data is kept
   * @param mode  opening mode (e.g. create/read-only, etc.)
   * @param lazy  if {@code true}, then skip ledger rows are created lazily
   *              (usually what you want)
   */
  public HashLedgerDir(File dir, Opening mode, boolean lazy) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");
    
    try (TaskStack onFail = new TaskStack()) {
      
      this.skipLedger = new SkipLedgerFile(new File(dir, DB_LEDGER), mode, lazy);
      onFail.pushClose(skipLedger);
      this.witnessRepo = new WitRepo(dir, mode);
      onFail.clear();
    }

  }
  
  
  
  protected HashLedgerDir(SkipLedger skipLedger, WitRepo witnessRepo) {
    this.dir = witnessRepo.getDir();
    this.skipLedger = Objects.requireNonNull(skipLedger, "null skip ledger");
    this.witnessRepo = witnessRepo;
  }
  
  
  @Override
  public void close() {
    try (TaskStack closer = new TaskStack()) {
      closer.pushClose(skipLedger).pushClose(witnessRepo);
    }
  }
  
  
  /**
   * Returns the directory the data of this instance live in.
   */
  public File getDir() {
    return dir;
  }
  
  

  @Override
  public SkipLedger getSkipLedger() {
    return skipLedger;
  }

  @Override
  public List<Long> getTrailedRowNumbers() {
    return witnessRepo.getTrailedRowNumbers();
  }

  @Override
  public boolean addTrail(WitnessRecord trailedRecord) {
    return witnessRepo.addTrail(trailedRecord);
  }

  @Override
  public int getTrailCount() {
    return witnessRepo.getTrailCount();
  }

  @Override
  public TrailedRow getTrailByIndex(int index) {
    return witnessRepo.getTrailByIndex(index);
  }
  
  

  
  
  public void trimSize(long newSize) {
    skipLedger.trimSize(newSize);
    witnessRepo.trimTrailsByRowNumber(newSize);
  }

}
