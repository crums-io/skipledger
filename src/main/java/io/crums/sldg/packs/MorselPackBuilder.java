/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Ledger;
import io.crums.sldg.Path;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.entry.EntryInfo;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Tuple;

/**
 * 
 */
public final class MorselPackBuilder implements MorselBag, Serial {
  
  private final RowPackBuilder rowPackBuilder = new RowPackBuilder();
  
  private final PathPackBuilder pathPackBuilder = new PathPackBuilder();
  
  private final TrailPackBuilder trailPackBuilder = new TrailPackBuilder();
  
  private final EntryPackBuilder entryPackBuilder = new EntryPackBuilder();
  
  
  private Object lock() {
    return rowPackBuilder;
  }
  
  
  /**
   * Adds information from the given morsel {@code pack} to this instance.
   * If {@linkplain #isEmpty() empty}, then the instance is initialized with
   * all the given information; otherwise (if not empty), then only <em>linked
   * information</em> is added. I.e. this method only allows adding detail to already
   * known information.
   * 
   * @param pack non-null, but empty OK
   * 
   * @return the number of objects added
   */
  public int addAll(MorselPack pack) throws HashConflictException {
    synchronized (lock()) {
      
      if (isEmpty())
        return init(pack);
      
      int rows = addRows(pack);
      int declPaths = addDeclaredPaths(pack);
      int beacons = addBeaconRows(pack);
      int trails = addTrails(pack);
      int entries = addEntries(pack);
      return rows + declPaths + beacons + trails + entries;
    }
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
  
  
  
  
  public int initState(Ledger ledger) throws IllegalStateException {
    
    Path path = ledger.statePath();
    
    if (path == null)
      throw new IllegalArgumentException("ledger is empty");
    else if (path.rows().size() < 2)
      throw new IllegalArgumentException("ledger is a single row");
    
    synchronized (lock()) {
      int count = rowPackBuilder.init(path);
      PathInfo stateDecl = new PathInfo(path.loRowNumber(), path.hiRowNumber());
      pathPackBuilder.addDeclaredPath(stateDecl);
      return count + 1;
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
  

  /**
   * Adds the rows in the given {@code path} that can be linked from the highest
   * row in this instance.
   * 
   * @param path a path whose lowest row number is &le; {@linkplain #hi()}
   * 
   * @return the number of rows added (possibly zero)
   * 
   * @throws HashConflictException
   *         if {@code path} is from another ledger
   */
  public int addPath(Path path) throws IllegalArgumentException, HashConflictException {
    return rowPackBuilder.addPath(path);
  }
  
  
  
  /**
   * Adds a path from the state-row ({@linkplain #hi()}) to the row with the
   * given row number.
   * 
   * @param rowNumber &ge; 1 and &le; {@linkplain #hi()}
   * @param ledger the ledger (or a descendant of) that created this instance
   * 
   * @return the number of rows added (possibly zero)
   * 
   * @throws HashConflictException
   *         if this instance is not from the given {@code ledger}
   */
  public int addPathToTarget(long rowNumber, Ledger ledger) throws HashConflictException {
    long hi = Objects.requireNonNull(ledger, "null ledger").size();
    Path path = ledger.skipPath(rowNumber, hi);
    return addPath(path);
  }
  
  
  
  
  public boolean addDeclaredPath(PathInfo decl) {
    synchronized (lock()) {
      if (!Sets.sortedSetView(getFullRowNumbers()).containsAll(decl.rowNumbers()))
        return false;
      
      return pathPackBuilder.addDeclaredPath(decl);
    }
  }
  
  
  public int addDeclaredPaths(MorselPack mp) throws HashConflictException {
    List<PathInfo> declPaths = mp.declaredPaths();
    if (declPaths.isEmpty())
      return 0;
    
    
    synchronized (lock()) {
      int count = 0;
      for (var p : declPaths) {
        if (addDeclaredPath(p))
          ++count;
      }
      return count;
    }
  }
  
  
  
  public boolean addBeaconRow(long rowNumber, long utc) {
    synchronized (lock()) {
      if (!hasFullRow(rowNumber))
        return false;
      
      // check the advertised time is consistent with the crumtrails
      List<Long> trailedRows = trailedRows();
      if (!trailedRows.isEmpty()) {
        
        int searchIndex = Collections.binarySearch(trailedRows, rowNumber);
        if (searchIndex >= 0) {
          CrumTrail trail = crumTrail(rowNumber);
          // the beacon cannot be created *after* it was witnessed
          if (utc > trail.crum().utc())
            throw new IllegalArgumentException(
                "attempt to set beacon status for trailed row " + rowNumber +
                " (witness-utc " + trail.crum().utc() + ") using beacon-utc " + utc);
        } else {
          int insertIndex = -1 - searchIndex;
          // check next trail
          if (insertIndex < trailedRows.size()) {
            Long trailedRn = trailedRows.get(insertIndex);
            CrumTrail nextTrail = crumTrail(trailedRn);
            // the beacon could not be created *after* a subsequent row was witnessed
            if (utc > nextTrail.crum().utc())
              throw new IllegalArgumentException(
                  "attempt to set beacon status for row " + rowNumber + " with beacon-utc " +
                  utc + " while row " + trailedRn + " is recorded as witnessed on " +
                  nextTrail.crum().utc());
          }
        }
      }
      
      
      return pathPackBuilder.addBeaconRow(rowNumber, utc);
    }
  }
  
  
  
  public int addBeaconRows(MorselPack mp) {
    List<Tuple<Long,Long>> beacons = mp.beaconRows();
    if (beacons.isEmpty())
      return 0;
    
    synchronized (lock()) {
      int count = 0;
      for (var b : beacons) {
        if (addBeaconRow(b.a, b.b))
          ++count;
      }
      return count;
    }
  }
  
  
  public boolean addTrail(long rowNumber, CrumTrail trail) throws HashConflictException {
    synchronized (lock()) {
      if (!hasFullRow(rowNumber))
        return false; // note we *could do this with a referenced-only row also
                      // but it would complicate proving witness time for lower row numbers
                      // For now, we disallow it
      
      if (!trail.crum().hash().equals(rowHash(rowNumber)))
        throw new HashConflictException("attempt to add unrelated crumtrail for row " + rowNumber);
      
      if (!trail.verify())
        throw new HashConflictException(
            "attempt to add crumtrail with inconsistent hashes for row " + rowNumber);
      
      // sanity-check the trail against any beacons
      // (again, only a sanity-check since the real check requires network access)
      
      var beaconRows = beaconRows();
      if (!beaconRows.isEmpty()) {
        int searchIndex = Collections.binarySearch(Lists.map(beaconRows, t -> t.a), rowNumber);
        
        if (searchIndex >= 0) {
          long beaconUtc = beaconRows.get(searchIndex).b;
          if (beaconUtc >= trail.crum().utc())
            throw new IllegalArgumentException(
                "attempt to annotate crumtrail with witness-utc " + trail.crum().utc() +
                " to beacon row " + beaconRows.get(searchIndex));
        } else {
          int insertIndex = -1 - searchIndex;
          // check previous
          if (insertIndex > 0) {
            long prevBcnUtc = beaconRows.get(insertIndex - 1).b;
            if (prevBcnUtc >= trail.crum().utc())
              throw new IllegalArgumentException(
                  "attempt to annotate crumtrail with witness-utc " + trail.crum().utc() +
                  " to row " + rowNumber + " while this beacon exists: " + beaconRows.get(insertIndex - 1));
          }
        }
      }
      
      return trailPackBuilder.addTrail(rowNumber, trail);
    }
  }
  
  
  
  public int addTrails(MorselPack mp) throws HashConflictException {
    List<Long> trailedRns = mp.trailedRows();
    if (trailedRns.isEmpty())
      return 0;
    synchronized (lock()) {
      int count = 0;
      for (long rn : trailedRns) {
        if (addTrail(rn, mp.crumTrail(rn)))
          ++count;
      }
      return count;
    }
  }
  
  
  
  /**
   * Like set entry, but only works once per row number.
   * 
   * @see #setEntry(long, ByteBuffer, String)
   */
  public boolean addEntry(long rowNumber, ByteBuffer content, String meta) {
    synchronized (lock()) {
      return
          !entryPackBuilder.hasEntry(rowNumber) &&
          setEntry(rowNumber, content, meta);
    }
  }
  
  

  /**
   * Sets the entry at the given row number if full-hash information is available
   * at the given row.
   * 
   * @param meta non-empty string (optional: has no effect if empty or null)
   * 
   * @return {@code false} if full-hash information is not available; {@code true} o.w.
   */
  public boolean setEntry(long rowNumber, ByteBuffer content, String meta) {
    synchronized (lock()) {
      if (!hasFullRow(rowNumber))
        return false;
      entryPackBuilder.setEntry(rowNumber, content, meta);
      return true;
    }
  }
  
  
  
  public int addEntries(MorselPack mp) {
    List<EntryInfo> infos = mp.availableEntries();
    if (infos.isEmpty())
      return 0;
    
    synchronized (lock()) {
      int count = 0;
      for (var info : infos) {
        ByteBuffer entryContent = mp.entry(info.rowNumber()).content();
        String meta = info.hasMeta() ? info.meta() : null;
        if (addEntry(info.rowNumber(), entryContent, meta))
          ++count;
      }
      return count;
    }
  }
  
  
  
  
  
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
  public boolean hasFullRow(long rowNumber) {
    return rowPackBuilder.hasFullRow(rowNumber);
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
  
  
  
  


  /**
   * @see MorselPack
   */
  @Override
  public int serialSize() {
    int headerBytes = 1 + 4*4;
    return
        headerBytes +
        rowPackBuilder.serialSize() +
        pathPackBuilder.serialSize() +
        trailPackBuilder.serialSize() +
        entryPackBuilder.serialSize();
  }


  /**
   * @see MorselPack
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.put((byte) 4).putInt(rowPackBuilder.serialSize())
      .putInt(pathPackBuilder.serialSize())
      .putInt(trailPackBuilder.serialSize())
      .putInt(entryPackBuilder.serialSize());
    
    rowPackBuilder.writeTo(out);
    pathPackBuilder.writeTo(out);
    trailPackBuilder.writeTo(out);
    entryPackBuilder.writeTo(out);
    return out;
  }

  
  
  
}
