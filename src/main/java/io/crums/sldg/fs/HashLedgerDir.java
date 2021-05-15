/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.fs;


import static io.crums.sldg.SldgConstants.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.client.repo.TrailRepo;
import io.crums.io.Opening;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.HashLedger;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.TaskStack;

/**
 * 
 */
public class HashLedgerDir implements HashLedger {
  

  private final File dir;
  private final SkipLedgerFile skipLedger;
  private TrailRepo witnessRepo;
  
  
  
  
  

  /**
   * 
   */
  public HashLedgerDir(File dir, Opening mode, boolean lazy) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");
    
    try (TaskStack onFail = new TaskStack(this)) {
      
      this.skipLedger = new SkipLedgerFile(new File(dir, DB_LEDGER), mode, lazy);
      onFail.pushClose(skipLedger);
      this.witnessRepo = new TrailRepo(dir, mode);
      onFail.clear();
    }

  }
  
  
  @Override
  public void close() {
    try (TaskStack closer = new TaskStack(this)) {
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

  /**
   * Returns the row numbers that have {@linkplain CrumTrail}s. I.e.
   * the rows that have been witnessed.
   * <p>
   * <em>This method is non-standard: it's not part of the interface</em>.
   * (The reason why it's not in the interface, is that while super-efficient
   * here, it's hard to make efficient if implemented on top of a relational
   * database, for example.)
   * </p>
   * 
   * @return strictly ascending list of row numbers, possibly empty
   * 
   * @see #getCrumTrailByIndex(int)
   */
  public List<Long> getTrailedRowNumbers() {
    return witnessRepo.getIds();
  }

  @Override
  public boolean addTrail(WitnessRecord trailedRecord) {
    if (lastWitnessedRowNumber() >= trailedRecord.rowNum())
      return false;
    long rn = trailedRecord.rowNum();
    long sz = getSkipLedger().size();
    if (rn > sz)
      throw new IllegalArgumentException("trailRecord row-number " + rn + " > size " + sz);
    
    if (!skipLedger.getRow(rn).equals(trailedRecord.row()))
      throw new HashConflictException("on attempt to addTrail() on row " + rn);
    
    // TODO: ensure TrailRepo instances only store stuff in increasing utc order
    //       (or at least provide a version that does that)
    
    witnessRepo.putTrail(trailedRecord.record().trail(), rn);
    return true;
  }

  @Override
  public int getTrailCount() {
    return (int) witnessRepo.size();
  }

  @Override
  public TrailedRow getTrailByIndex(int index) {
    CrumTrail trail = witnessRepo.getTrail(index);
    long rowNumber = getTrailedRowNumbers().get(index);
    return new TrailedRow(rowNumber, trail);
  }

  @Override
  public TrailedRow nearestTrail(long rowNumber) {
    long witRn;
    CrumTrail trail;
    {
      List<Long> witRns = getTrailedRowNumbers();
      int sindex = Collections.binarySearch(witRns, rowNumber);
      
      if (sindex < 0) {
        sindex = -1 - sindex;
        if (sindex == witRns.size())
          return null;
      }
      
      witRn = witRns.get(sindex);
      trail = witnessRepo.getTrail(sindex);
    }
    
    assert witRn >= rowNumber;
    
    return new TrailedRow(witRn, trail);
  }

  @Override
  public long lastWitnessedRowNumber() {
    List<Long> ids = getTrailedRowNumbers();
    return ids.isEmpty() ? 0 : ids.get(ids.size() - 1);
  }
  
  

  
  
  public void trimSize(long newSize) {
    skipLedger.trimSize(newSize);
    
    int searchIndex = Collections.binarySearch(witnessRepo.getIds(), newSize);
    if (searchIndex < 0)
      searchIndex = -1 - searchIndex;
    witnessRepo.trimSize(searchIndex);
  }

}
