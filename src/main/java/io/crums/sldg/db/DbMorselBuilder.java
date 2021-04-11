/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.model.CrumTrail;
import io.crums.sldg.Ledger;
import io.crums.sldg.Path;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.util.EasyList;


/**
 * 
 */
public class DbMorselBuilder extends MorselPackBuilder {
  
  private final Db db;

  /**
   * 
   */
  public DbMorselBuilder(Db db) {
    this.db = Objects.requireNonNull(db, "null db");
    initState(db.getLedger());
  }
  
  /**
   * 
   * <h2>Additional Override Behavior</h2>
   * <p>
   * Adds the <em>last beacon row before</em> {@code rowNumber}, if any; adds the <em>first
   * crumtrail on or after</em> {@code rowNumber}, if any. Ensures a path from {@linkplain #hi()}
   * to these and {@code rowNumber} exist.
   * </p>
   */
  @Override
  public boolean addEntry(long rowNumber, ByteBuffer content, String meta) {
    checkRowNum(rowNumber);
    Objects.requireNonNull(content, "null content");
    // (null meta allowed)
    
    // ensure a path to this row exists in this morsel
    // find nearest beacon and crumtrail (zero, if not found)
    //
    // FIXME: I shouldn't have to do this.. fix Db api
    //
    final long beaconRn;
    final long beaconUtc;
    {
      List<Long> beaconRns = db.getBeaconRowNumbers();
      int sindex = Collections.binarySearch(beaconRns, rowNumber);
      if (sindex >= 0)
        throw new IllegalArgumentException("attempt to set entry for beacon row " + rowNumber);
      int insertIndex = -1 - sindex;
      if (insertIndex == 0) {
        beaconUtc = beaconRn = 0;
      } else {
        beaconRn = beaconRns.get(insertIndex - 1);
        beaconUtc = db.getBeaconUtcs().get(insertIndex - 1);
      }
    }
    final long witRn;
    final CrumTrail trail;
    {
      List<Long> witRns = db.getRowNumbersWitnessed();
      int sindex = Collections.binarySearch(witRns, rowNumber);
      if (sindex < 0) {
        int insertIndex = -1 - sindex;
        if (insertIndex == 0) {
          witRn = 0;
          trail = null;
        } else {
          witRn = witRns.get(insertIndex);
          trail = db.getCrumTrailByIndex(insertIndex);
        }
      } else {
        witRn = witRns.get(sindex);
        trail = db.getCrumTrailByIndex(sindex);
      }
    }
    
    // assemble the targets
    EasyList<Long> targets = new EasyList<>(4);
    if (beaconRn != 0)
      targets.add(beaconRn);
    targets.add(rowNumber);
    if (witRn != 0 && witRn != rowNumber)
      targets.add(witRn);
    if (targets.last() != hi())
      targets.add(hi());
    
    // add the path to the targets
    Path path = db.getLedger().getPath(targets);
    addPath(path);
    
    if (beaconRn != 0)
      addBeaconRow(beaconRn, beaconUtc);
    
    if (witRn != 0)
      addTrail(witRn, trail);
    
    return super.addEntry(rowNumber, content, meta);
  }
  
  
  
  private void checkRowNum(long rowNumber) {
    Ledger.checkRealRowNumber(rowNumber);
    if (rowNumber > db.size())
      throw new IllegalArgumentException("rowNumber " + rowNumber + " >  size " + db.size());
  }
  

}
