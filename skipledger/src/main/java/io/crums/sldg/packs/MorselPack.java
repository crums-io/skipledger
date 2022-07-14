/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import io.crums.io.buffer.BufferUtils;
import io.crums.io.buffer.Partitioning;
import io.crums.model.CrumTrail;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.MorselFile;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.SourceRow;
import io.crums.util.Sets;

/**
 * <p>
 * Implementation of a {@linkplain MorselBag} using a composition of row-, trail-, source-,
 * and path-packs. But note, unlike those classes, this actually must validate the
 * business rules (the subcomponents are kinda ignorant of the larger business rules:
 * they only guarantee well-formed-ness within their own confines).
 * </p>
 * <h2>Serial Format</h2>
 * <p>
 * Rationale for the existence of PACK_SIZES array is that we want to be able to random
 * access any of the morsel's sections without having to read the whole. This is to support
 * <em>future</em> capabilities; we don't actually need this right now. (Recall, each
 * of the bags is self-delimiting anyway.)
 * </p>
 * <p>
 * <pre>
 *    PACK_COUNT  := BYTE (current version is 5)
 *    PACK_SIZES  := INT ^PACK_COUNT
 *    MORSEL_PACK := PACK_COUNT PACK_SIZES ROW_PACK TRAIL_PACK SRC_PACK PATH_PACK
 * </pre>
 * </p>
 * 
 */
public final class MorselPack implements MorselBag {
  
  public final static int MIN_PACK_COUNT = 4;
  public final static int VER_PACK_COUNT = 5;
  
  
  /** @return {@linkplain #load(InputStream, boolean) load(in, true)} */
  public static MorselPack load(InputStream in) {
    return load(in, true);
  }
  
  
  /**
   * Loads an instance from the given stream.
   * 
   * @param hasHeader if {@code true} then a morsel file header is first parsed
   */
  public static MorselPack load(InputStream in, boolean hasHeader) {
    var bytes = BufferUtils.readFully(in);
    if (hasHeader)
      MorselFile.advanceHeader(bytes, in);
    return load(bytes);
  }
  
  
  public static MorselPack load(ByteBuffer in) {
    final int packCount = 0xff & in.get();
    
    if (packCount < MIN_PACK_COUNT)
      throw new ByteFormatException("PACK_COUNT " + packCount);
    
    ArrayList<Integer> packSizes = new ArrayList<>(packCount);
    int totalSize = 0;
    for (int index = 0; index < packCount; ++index) {
      int size = in.getInt();
      if (size < 0)
        throw new ByteFormatException("negative size " + size + " at index " + index);
      packSizes.add(size);
      totalSize += size;
    }
    
//    for (int index = VER_PACK_COUNT; index < packCount; ++index)
//      in.getInt();
    
    ByteBuffer block = BufferUtils.slice(in, totalSize);
    if (!block.isReadOnly())
      block = block.asReadOnlyBuffer();
    
    // again, we didn't have to do this.. just better bookkeeping this way
    Partitioning parts = new Partitioning(block, packSizes);
    
    RowPack rowPack = RowPack.load(parts.getPart(0));
    TrailPack trailPack = TrailPack.load(parts.getPart(1));
    SourcePack sourcePack = SourcePack.load(parts.getPart(2));
    PathPack pathPack = PathPack.load(parts.getPart(3));
    
    MetaPack metaPack =
        packCount >= VER_PACK_COUNT ?
            MetaPack.load(parts.getPart(4)) : MetaPack.EMPTY;
    
    return new MorselPack(
        new CachingRowPack(rowPack), trailPack, sourcePack, pathPack, metaPack);
  }
  
  
  
  private final RowPack rowPack;
  private final TrailPack trailPack;
  private final SourcePack sourcePack;
  private final PathPack pathPack;
  private final MetaPack metaPack;
  
  
  private MorselPack(
      RowPack rowPack, TrailPack trailPack, SourcePack sourcePack, PathPack pathPack,
      MetaPack metaPack) {
    
    this.rowPack = rowPack;
    this.trailPack = trailPack;
    this.sourcePack = sourcePack;
    this.pathPack = pathPack;
    this.metaPack = metaPack;
    
    // Validate:
    // the rowPack is the backing "given". Other packs are validated against the row pack
    
    final SortedSet<Long> rowNums = Sets.sortedSetView(rowPack.getFullRowNumbers());

    // validate trailPack
    //
    if (!rowNums.containsAll(trailPack.trailedRowNumbers()))
      throw new ByteFormatException("trail pack references rows not found in row-pack");
    
    for (Long rn : trailPack.trailedRowNumbers()) {
      CrumTrail trail = trailPack.crumTrail(rn);
      if (!trail.verify())
        throw new HashConflictException("crumtrail for row " + rn + " is tampered or malformed");
      ByteBuffer witnessedHash = trailPack.crumTrail(rn).crum().hash();
      ByteBuffer rowHash = rowPack.rowHash(rn);
      if (!rowHash.equals(witnessedHash))
        throw new HashConflictException("hash referenced in crumtrail does not match row " + rn);
    }
    
    // validate sourcePack
    //
    for (SourceRow src : sourcePack.sources()) {
      Long srn = src.rowNumber();
      if (!rowNums.contains(srn))
        throw new ByteFormatException("source-row " + srn + " not found in row-pack");
      if (!src.rowHash().equals(rowPack.inputHash(srn)))
        throw new HashConflictException("at source-row " + srn);
    }
    
    
    // validate pathPack
    //
    for (PathInfo decl : pathPack.declaredPaths()) {
      if (!rowNums.containsAll(decl.rowNumbers()))
        throw new ByteFormatException(
            "declared path " + decl + " references rows not found in row-pack");
    }
    
  }


  public RowPack getRowPack() {
    return rowPack;
  }


  public TrailPack getTrailPack() {
    return trailPack;
  }


  public SourcePack getSourcePack() {
    return sourcePack;
  }


  public PathPack getPathPack() {
    return pathPack;
  }

  
  public MetaPack getMetaPack() {
    return metaPack;
  }
  
  
  public Optional<SourceInfo> getSourceInfo() {
    return metaPack.getSourceInfo();
  }
  
  
  //  I N T E R F A C E    M E T H O D S
  
  
  

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
  public List<Long> trailedRowNumbers() {
    return trailPack.trailedRowNumbers();
  }


  @Override
  public CrumTrail crumTrail(long rowNumber) {
    return trailPack.crumTrail(rowNumber);
  }


  @Override
  public List<SourceRow> sources() {
    return sourcePack.sources();
  }


  @Override
  public List<PathInfo> declaredPaths() {
    return pathPack.declaredPaths();
  }


    
  

}




















