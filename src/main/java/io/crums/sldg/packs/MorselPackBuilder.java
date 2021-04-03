/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.List;

import io.crums.model.CrumTrail;
import io.crums.sldg.Entry;
import io.crums.sldg.EntryInfo;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.bags.MorselBag;
import io.crums.util.Sets;
import io.crums.util.Tuple;

/**
 * 
 */
public final class MorselPackBuilder implements MorselBag {
  
  private final RowPackBuilder rowPackBuilder = new RowPackBuilder();
  
  private final PathPackBuilder pathPackBuilder = new PathPackBuilder();
  
  private final TrailPackBuilder trailPackBuilder = new TrailPackBuilder();
  
  private final EntryPackBuilder entryPackBuilder = new EntryPackBuilder();
  
  
  private Object lock() {
    return rowPackBuilder;
  }
  
  
  /**
   * Adds information from the given {@code pack} to this instance.
   * If the instance is empty, this method accepts all the information;
   * if not empty, however, only <em>linked information</em> is
   * added. I.e. this method only allows adding detail to already known
   * information.
   * 
   * @param pack non-null, but empty OK
   * 
   * @return the number of objects added
   */
  public int addAll(MorselPack pack) throws HashConflictException {
    // FIXME
    int rowsAdded = rowPackBuilder.addAll(pack.rowPack());
    int objectsAdded = pathPackBuilder.addPathPack(pack.pathPack());
    int trailsAdded = trailPackBuilder.addAll(pack.trailPack());
    return rowsAdded + objectsAdded + trailsAdded;
  }
  
  
  
  public int init(MorselPack pack) throws IllegalStateException {
    synchronized (lock()) {
      if (!isEmpty())
        throw new IllegalStateException("attempt to initialize instance while not empty");

      int rowsAdded = rowPackBuilder.addAll(pack.rowPack());
      int objectsAdded = pathPackBuilder.addPathPack(pack.pathPack());
      int trailsAdded = trailPackBuilder.addAll(pack.trailPack());
      return rowsAdded + objectsAdded + trailsAdded;
    }
  }
  
  
  
  public boolean isEmpty() {
    return rowPackBuilder.isEmpty();
  }
  
  
  
  public boolean addRow(Row row) throws HashConflictException {
    return rowPackBuilder.add(row);
  }
  
  

  /**
   * Adds all the rows in the given morsel pack. If this instance {@linkplain #isEmpty() is empty}, then all
   * the rows in the pack are added. Otherwise, only <em>linked information</em> is
   * added. I.e. if not empty, this method only allows adding detail to already known information.
   * 
   * @return the number of full rows added
   * 
   * @throws HashConflictException
   *         if a hash in {@code mp} conflicts with existing hashes.
   */
  public int addRows(MorselPack mp) throws HashConflictException {
    return rowPackBuilder.addAll(mp.rowPack());
  }
  
  
  
  
  public boolean addDeclaredPath(PathInfo decl) {
    synchronized (lock()) {
      if (!Sets.sortedSetView(getFullRowNumbers()).containsAll(decl.rowNumbers()))
        return false;
      
      return pathPackBuilder.addDeclaredPath(decl);
    }
  }
  
  
  
//  public 
  
  
  /**
   * The lowest (full) row number in the bag, or 0 if empty.
   * 
   * @return &ge; 0
   */
  public long lo() {
    return rowPackBuilder.lo();
  }
  

  /**
   * The highest (full) row number in the bag, or 0 if empty.
   *
   * @return &ge; {@linkplain #lo()}
   */
  public long hi() {
    return rowPackBuilder.hi();
  }
  
  

  
  // - - - MorselBag methods - - -


  @Override
  public ByteBuffer rowHash(long rowNumber) {
    return rowPackBuilder.rowHash(rowNumber);
  }

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    return rowPackBuilder.inputHash(rowNumber);
  }

  @Override
  public Row getRow(long rowNumber) {
    return rowPackBuilder.getRow(rowNumber);
  }

  @Override
  public List<Long> getFullRowNumbers() {
    return rowPackBuilder.getFullRowNumbers();
  }
  
  
  
  

  @Override
  public List<Tuple<Long, Long>> beaconRows() {
    return pathPackBuilder.beaconRows();
  }

  @Override
  public List<PathInfo> declaredPaths() {
    return pathPackBuilder.declaredPaths();
  }
  
  
  


  @Override
  public List<Long> trailedRows() {
    return trailPackBuilder.trailedRows();
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    return trailPackBuilder.crumTrail(rowNumber);
  }
  
  
  
  
  

  @Override
  public List<EntryInfo> availableEntries() {
    return entryPackBuilder.availableEntries();
  }

  @Override
  public Entry entry(long rowNumber) {
    return entryPackBuilder.entry(rowNumber);
  }

  
  
  
}
