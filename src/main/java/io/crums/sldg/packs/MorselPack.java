/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import io.crums.io.buffer.BufferUtils;
import io.crums.io.buffer.Partitioning;
import io.crums.model.CrumTrail;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.db.ByteFormatException;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.entry.EntryInfo;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Tuple;

/**
 * <p>
 * Implementation of a {@code MorselBag} using a composition of row-, path-, trail-,
 * and entry-packs. But note, unlike those classes, this actually must validate the
 * business rules (the subcomponents are kinda ignorant of the larger business rules:
 * they only guarantee well-formed-ness within their own confines).
 * </p>
 * <h2>What's <em>not</em> validated</h2>
 * <p>
 * Beacon rows and Crumtrails reference external resources (hashes) that require a network
 * connection to validate. It would be too onerous to include these checks every time an
 * instance where instantiated, so these are left as a later step in the data validation
 * process.
 * </p><p>
 * Also, we haven't hammered out exactly how an {@linkplain Entry}'s {@linkplain Entry#hash() hash}
 * is computed. For this reason, validation does not include matching entry hashes to
 * that advertised as in the ledger row's {@linkplain Row#inputHash() input hash}.
 * </p>
 * <h2>Serial Format</h2>
 * <p>
 * Rationale for the existence of BAG_SIZES array is that we want to be able to random
 * access any of the morsel's sections without having to read the whole. This is to support
 * <em>future</em> capabilities; we don't actually need this right now. (Recall, each
 * of the bags is self-delimiting anyway.)
 * </p>
 * <p>
 * <pre>
 *    BAG_COUNT   := BYTE (current version is 4)
 *    BAG_SIZES   := INT ^BAG_COUNT
 *    MORSEL_BAG  := BAG_COUNT BAG_SIZES ROW_BAG PATH_BAG TRAIL_BAG ENTRY_BAG
 * </pre>
 * </p>
 */
public final class MorselPack implements MorselBag {
  
  
  
  
  /**
   * Loads and returns an instance from its serial (binary) representation.
   * 
   * @param in  caller must not modify contents since much is not copied.
   *            On return, the position is advanced by the size of the data
   *            used. (Caller is free to modify positional state, just not
   *            the contents of what was just "read").
   * 
   * @throws ByteFormatException if the data appears to be structurally non-sensical (e.g. read a
   *                             a negative count)
   * @throws HashConflictException if the data contains conflicting hashes (aka tampered, or broken)
   */
  public static MorselPack load(ByteBuffer in) throws ByteFormatException, HashConflictException {
    final int bagCount = 0xff & in.get();
    
    if (bagCount < 4)
      throw new ByteFormatException("BAG_COUNT " + bagCount + " < 4");
    
    ArrayList<Integer> packSizes = new ArrayList<>(4);
    int totalSize = 0;
    for (int index = 0; index < 4; ++index) {
      int size = in.getInt();
      if (size < 0)
        throw new ByteFormatException("negative size " + size + " at index " + index);
      packSizes.add(size);
      totalSize += size;
    }
    
    // ignore the parts we don't understand
    // (maybe lame, but a way to future-proof the format)
    for (int index = 4; index < bagCount; ++index)
      in.getInt();
    
    ByteBuffer block = BufferUtils.slice(in, totalSize);
    if (!block.isReadOnly())
      block = block.asReadOnlyBuffer();
    
    // again, we didn't have to do this.. just better bookkeeping this way
    Partitioning parts = new Partitioning(block, packSizes);
    
    RowPack rowPack = RowPack.load(parts.getPart(0));
    PathPack pathPack = PathPack.load(parts.getPart(1));
    TrailPack trailPack = TrailPack.load(parts.getPart(2), false);
    EntryPack entryPack = EntryPack.load(parts.getPart(3));
    
    return new MorselPack(new CachingRowPack(rowPack), pathPack, trailPack, entryPack);
  }
  
  
  
  
  
  
  
  private final RowPack rowPack;
  
  private final PathPack pathPack;
  
  private final TrailPack trailPack;
  
  private final EntryPack entryPack;
  
  
  private MorselPack(RowPack rowPack, PathPack pathPack, TrailPack trailPack, EntryPack entryPack) {
    this.rowPack = rowPack;
    this.pathPack = pathPack;
    this.trailPack = trailPack;
    this.entryPack = entryPack;
    
    // Validate:
    // the rowPack is the backing "given". Other packs are validated against the row pack
    
    // validate pathPack
    SortedSet<Long> rowNums = Sets.sortedSetView(rowPack.getFullRowNumbers());
    for (PathInfo decl : pathPack.declaredPaths()) {
      if (!rowNums.containsAll(decl.rowNumbers()))
          throw new ByteFormatException("declared path " + decl + " references rows not in bag");
    }
    
    var beacons = pathPack.beaconRows();
    for (Tuple<Long,Long> beacon : beacons) {
      Long rn = beacon.a;
      if (!rowNums.contains(rn))
        throw new ByteFormatException("beacon row " + rn + " not found in row pack");
    }
    
    // validate trailPack
    if (!rowNums.containsAll(trailPack.trailedRows()))
      throw new ByteFormatException("trail pack references rows not found in row bag");
    
    List<Long> beaconRows = Lists.map(beacons, b -> b.a);
    for (Long rn : trailPack.trailedRows()) {
      CrumTrail trail = trailPack.crumTrail(rn);
      if (!trail.verify())
        throw new HashConflictException("crumtrail for row " + rn + " is tampered or malformed");
      ByteBuffer witnessedHash = trailPack.crumTrail(rn).crum().hash();
      ByteBuffer rowHash = rowPack.rowHash(rn);
      if (!rowHash.equals(witnessedHash))
        throw new HashConflictException("hash referenced in crumtrail does not match row " + rn);
      {
        int bindex = Collections.binarySearch(beaconRows, rn);
        
        if (bindex < 0) {
          
          int insertIndex = -1 - bindex;
          if (insertIndex != 0 && beacons.get(insertIndex - 1).b > trail.crum().utc())
            throw new ByteFormatException(
                "beacon " + beacons.get(insertIndex - 1) +
                " ahead crumtrail [" + rn + "," + trail.crum().utc() + "]");
          
          if (insertIndex < beacons.size() && beacons.get(insertIndex).b < trail.crum().utc())
            throw new ByteFormatException(
                "beacon " + beacons.get(insertIndex) +
                " behind crumtrail [" + rn + "," + trail.crum().utc() + "]");
        
        } else if (beacons.get(bindex).b > trail.crum().utc())
            throw new ByteFormatException(
                "beacon " + beacons.get(bindex) +
                " ahead crumtrail [" + rn + "," + trail.crum().utc() + "]");
        
      }
    }
    
    // validate entryPack
    if (!rowNums.containsAll(Lists.map(entryPack.availableEntries(), ei -> ei.rowNumber())))
      throw new ByteFormatException("entry pack references rows not found in row bag");
    
    
    // 
  }
  
  
  
  public RowPack rowPack() {
    return rowPack;
  }
  

  
  public PathPack pathPack() {
    return pathPack;
  }
  
  
  public TrailPack trailPack() {
    return trailPack;
  }
  
  
  public EntryPack entryPack() {
    return entryPack;
  }
  


  @Override
  public ByteBuffer rowHash(long rowNumber) {
    return rowPack.rowHash(rowNumber);
  }

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    return rowPack.inputHash(rowNumber);
  }

  @Override
  public Row getRow(long rowNumber) {
    return rowPack.getRow(rowNumber);
  }

  @Override
  public List<Long> getFullRowNumbers() {
    return rowPack.getFullRowNumbers();
  }
  
  
  
  

  @Override
  public List<Tuple<Long, Long>> beaconRows() {
    return pathPack.beaconRows();
  }

  @Override
  public List<PathInfo> declaredPaths() {
    return pathPack.declaredPaths();
  }
  
  
  


  @Override
  public List<Long> trailedRows() {
    return trailPack.trailedRows();
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    return trailPack.crumTrail(rowNumber);
  }
  
  
  
  
  

  @Override
  public List<EntryInfo> availableEntries() {
    return entryPack.availableEntries();
  }

  @Override
  public Entry entry(long rowNumber) {
    return entryPack.entry(rowNumber);
  }

}
