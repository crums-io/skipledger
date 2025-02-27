/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.fs;


import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import io.crums.client.repo.TrailRepo;
import io.crums.io.Opening;
import io.crums.model.CrumTrail;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.ledgers.WitnessedRowRepo;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;

/**
 * File-based {@linkplain WitnessedRowRepo} implementation.
 * This uses an underyling {@linkplain TrailRepo} in the directory
 * specified at construction.
 */
public class WitRepo implements WitnessedRowRepo {
  
  
  public static boolean isPresent(File dir) {
    return TrailRepo.isPresent(dir);
  }
  
  
  private final TrailRepo repo;

  
  /**
   * Creates an instance implemented using an underlying {@linkplain TrailRepo}
   * in the specified directory. 
   * 
   * @param dir   directory path where data is kept
   * @param mode  opening mode (e.g. create/read-only, etc.)
   */
  public WitRepo(File dir, Opening mode) throws IOException {
    this.repo = new TrailRepo(dir, mode);
  }
  
  
  /** The directory backing data files are kept in. */
  public final File getDir() {
    return repo.getDir();
  }
  
  
  /**
   * <p>
   * The trailed row numbers are maintained in the {@code ID} field
   * of the underlying {@linkplain TrailRepo}.
   * </p>
   * {@inheritDoc}
   * 
   */
  @Override
  public List<Long> getTrailedRowNumbers() {
    return repo.getIds();
  }
  

  @Override
  public boolean addTrail(WitnessRecord trailedRecord) {
    long rn = trailedRecord.rowNum();
    if (lastWitnessedRowNumber() >= rn)
      return false;
    
    // TODO: ensure repo only stores stuff in increasing utc order
    //       (or at least provide a version that does that)
    
    repo.putTrail(trailedRecord.record().trail(), rn);
    return true;
  }
  

  @Override
  public int getTrailCount() {
    return (int) repo.size();
  }

  @Override
  public TrailedRow getTrailByIndex(int index) {
    CrumTrail trail = repo.getTrail(index);
    long rowNumber = getTrailedRowNumbers().get(index);
    return new TrailedRow(rowNumber, trail);
  }
  
  
  /**
   * Trims any stored {@code TrailedRow}s at the given
   * row number and beyond.
   * 
   * @param rowNumber rows numbers greater than or equal to this are
   *                  removed. Must be &ge; 1
   */
  public void trimTrailsByRowNumber(long rowNumber) {
    SkipLedger.checkRealRowNumber(rowNumber);
    int index =
        Collections.binarySearch(getTrailedRowNumbers(), rowNumber);
    if (index < 0)
      index = -1 - index;
    
    trimTrails(index);
  }
  
  
  /**
   * Trims the trails to the given new {@code count}.
   * 
   * @param count &ge; 0
   */
  public void trimTrails(int count) {
    repo.trimSize(count);
  }
  
  
  
  @Override
  public void close() {
    repo.close();
  }

}
