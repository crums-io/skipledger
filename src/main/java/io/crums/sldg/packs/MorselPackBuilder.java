/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Sets;

/**
 * 
 */
public class MorselPackBuilder implements MorselBag, Serial {
  
  
  protected final RowPackBuilder rowPackBuilder = new RowPackBuilder();
  protected final TrailPackBuilder trailPackBuilder = new TrailPackBuilder();
  protected final SourcePackBuilder sourcePackBuilder = new SourcePackBuilder();
  protected final PathPackBuilder pathPackBuilder = new PathPackBuilder();
  
  private MetaPack metaPack = MetaPack.EMPTY;
  
  
  protected final Object lock() {
    return rowPackBuilder;
  }
  
  
  public int init(MorselPack pack) throws IllegalStateException {
    synchronized (lock()) {
      int rowsAdded = rowPackBuilder.init(pack.getRowPack());
      int trailsAdded = trailPackBuilder.addAll(pack.getTrailPack());
      int srcsAdded = sourcePackBuilder.addAll(pack.getSourcePack());
      int infosAdded = pathPackBuilder.addPathPack(pack.getPathPack());
      return rowsAdded + trailsAdded + srcsAdded + infosAdded;
    }
  }
  
  
  
  public int initPath(Path path, String comment) {
    if (Objects.requireNonNull(path, "null path").hiRowNumber() < 2)
      throw new IllegalArgumentException("path is single row number 1");
    
    boolean declare = comment != null && !comment.isEmpty();
    
    
    synchronized (lock()) {
      int count = rowPackBuilder.init(path);
      if (declare) {
        PathInfo info = new PathInfo(path.loRowNumber(), path.hiRowNumber(), comment);
        pathPackBuilder.addDeclaredPath(info);
        ++count;
      }
      return count;
    }
  }
  
  
  public void setMetaPack(SourceInfo sourceInfo) {
    this.metaPack = new MetaPack(sourceInfo);
  }
  
  
  public MetaPack getMetaPack() {
    return metaPack;
  }
  
  
  public int addAll(MorselPack pack) {
    synchronized (lock()) {
      if (isEmpty())
        return init(pack);
      int count = 0;
      count += addRows(pack);
      count += addTrails(pack);
      count += addSourceRows(pack);
      count += addDeclaredPaths(pack);
      return count;
    }
  }
  
  
  
  public boolean isEmpty() {
    return rowPackBuilder.isEmpty();
  }
  
  /**
   * Adds the given row if it can be linked from the last row.
   * 
   * @return {@code rowPackBuilder.add(row)}
   * 
   * @see RowPackBuilder#add(Row)
   */
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
   *         if a hash in {@code pack} conflicts with an existing hash
   */
  public int addRows(MorselPack pack) throws HashConflictException {
    return rowPackBuilder.addAll(pack.getRowPack());
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
  public int addPathToTarget(long rowNumber, SkipLedger ledger) throws HashConflictException {
    long hi = Objects.requireNonNull(ledger, "null ledger").size();
    Path path = ledger.skipPath(rowNumber, hi);
    return addPath(path);
  }
  
  
  
  /**
   * Adds the given path declaration, but only if its full rows are already in this morsel.
   */
  public boolean addDeclaredPath(PathInfo decl) {
    synchronized (lock()) {
      if (!Sets.sortedSetView(getFullRowNumbers()).containsAll(decl.rowNumbers()))
        return false;
      
      return pathPackBuilder.addDeclaredPath(decl);
    }
  }
  
  
  /**
   * Adds the declared paths from the given morsel. Only those whose full row numbers
   * are already in this morsel are added.
   * 
   * @return the number of declarations added
   * 
   * @see #addDeclaredPath(PathInfo)
   */
  public int addDeclaredPaths(MorselPack pack) throws HashConflictException {
    List<PathInfo> declPaths = pack.declaredPaths();
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
  
  

  /**
   * Adds the trail for the given row, but only if a full row at the given number exists
   * in this morsel.
   * <p>FIXME: this should just take a TrailedRow</p>
   * 
   * @throws HashConflictException if the witnessed hash in the crumtrail conflicts
   *          with the hash of the given row number
   */
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
      
      return trailPackBuilder.addTrail(rowNumber, trail);
    }
  }
  
  
  /**
   * Adds the trailed rows from the given morsel. Only those whose full row numbers
   * are already in this morsel are added.
   * 
   * @return the number of crumtrails added
   */
  public int addTrails(MorselPack pack) {
    List<Long> trailedRns = pack.trailedRowNumbers();
    if (trailedRns.isEmpty())
      return 0;
    synchronized (lock()) {
      int count = 0;
      for (long rn : trailedRns) {
        if (addTrail(rn, pack.crumTrail(rn)))
          ++count;
      }
      return count;
    }
  }
  
  
  /**
   * Adds the given source-row, but only if its full row already exists in this
   * morsel.
   */
  public boolean addSourceRow(SourceRow row) {
    synchronized (lock()) {
      long rn = row.rowNumber();
      if (!hasFullRow(rn))
        return false;
      
      if (!inputHash(rn).equals(row.rowHash()))
        throw new HashConflictException("at row " + rn);
      
      return sourcePackBuilder.addSourceRow(row);
    }
  }
  
  
  
  
  public int addSourceRows(MorselPack pack) {
    List<SourceRow> sources = pack.sources();
    if (sources.isEmpty())
      return 0;
    
    synchronized (lock()) {
      int count = 0;
      for (SourceRow src : sources) {
        if (addSourceRow(src))
          ++count;
      }
      return count;
    }
  }
  
  
  
  

  
  //  I N T E R F A C E    M E T H O D S
  
  @Override
  public long lo() {
    return rowPackBuilder.lo();
  }
  
  @Override
  public long hi() {
    return rowPackBuilder.hi();
  }
  
  @Override
  public boolean hasFullRow(long rowNumber) {
    return rowPackBuilder.hasFullRow(rowNumber);
  }
  

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
  public List<Long> trailedRowNumbers() {
    return trailPackBuilder.trailedRowNumbers();
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    return trailPackBuilder.crumTrail(rowNumber);
  }

  @Override
  public List<SourceRow> sources() {
    return sourcePackBuilder.sources();
  }

  @Override
  public List<PathInfo> declaredPaths() {
    return pathPackBuilder.declaredPaths();
  }

  
  
  
  

  /**
   * @see MorselPack
   */
  @Override
  public int serialSize() {
    int headerBytes = 1 + 4 * MorselPack.VER_PACK_COUNT;
    return
        headerBytes +
        rowPackBuilder.serialSize() +
        trailPackBuilder.serialSize() +
        sourcePackBuilder.serialSize() +
        pathPackBuilder.serialSize() +
        metaPack.serialSize();
  }


  /**
   * @see MorselPack
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.put((byte) MorselPack.VER_PACK_COUNT).putInt(rowPackBuilder.serialSize())
    .putInt(trailPackBuilder.serialSize())
    .putInt(sourcePackBuilder.serialSize())
    .putInt(pathPackBuilder.serialSize())
    .putInt(metaPack.serialSize());
  
  rowPackBuilder.writeTo(out);
  trailPackBuilder.writeTo(out);
  sourcePackBuilder.writeTo(out);
  pathPackBuilder.writeTo(out);
  metaPack.writeTo(out);
  return out;
  }
  
}
