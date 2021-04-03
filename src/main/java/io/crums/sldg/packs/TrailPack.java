/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.crums.io.buffer.BufferUtils;
import io.crums.io.buffer.Partitioning;
import io.crums.model.CrumTrail;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.bags.TrailBag;
import io.crums.sldg.db.ByteFormatException;

/**
 * 
 * <h2>Serial Format</h2>
 * <p>
 * The serial format is the concatenation of 2 sections, TRAIL_LIST and TRAILS.
 * </p>
 * 
 * <h4>TRAIL_LIST</h4>
 * <p>
 * <pre>
 *    TRAIL_CNT   := SHORT      // number of crumtrails
 *    
 *    RN          := LONG       // row number
 *    TRAIL_SIZE  := INT        // the size of the row's crumtrail in bytes
 *    
 *    TRAIL_ITEM  := RN TRAIL_SIZE
 *    
 *    TRAIL_LIST  := TRAIL_CNT [TRAIL_ITEM ^TRAIL_CNT]
 * </pre>
 * </p>
 * 
 * <h4>TRAILS</h4>
 * <p>
 * This is just a list of variable-width CRUMTRAILs each of length TRAIL_SIZE.
 * </p>
 * <p>
 * <pre>
 *    CRUMTRAIL   := BYTE ^TRAIL_SIZE      // byte-size of crumtrail
 *    
 *    TRAILS      := CRUMTRAIL ^TRAIL_CNT
 * </pre>
 * </p>
 * 
 * <h4>TRAIL_BAG</h4>
 * 
 * <p>
 * <pre>
 *    TRAIL_BAG   := TRAIL_LIST  TRAILS
 * </pre>
 * </p>
 * 
 */
public class TrailPack implements TrailBag {
  
  
  public final static TrailPack EMPTY = new TrailPack(Collections.emptyList(), Partitioning.NULL);
  
  
  public static TrailPack load(ByteBuffer in) throws ByteFormatException {
    return load(in, true);
  }
  
  
  public static TrailPack load(ByteBuffer in, boolean copy) throws ByteFormatException {
    final int count = 0xffff & in.getShort();
    if (count == 0)
      return EMPTY;
    
    List<Long> rowNums = new ArrayList<>(count);
    ArrayList<Integer> sizes = new ArrayList<>(count);
    
    long lastRn = 0L;
    int sizeTotal = 0;
    for (int index = 0; index < count; ++index) {
      
      long nextRn = in.getLong();
      int size = in.getInt();
      
      if (nextRn <= lastRn)
        throw new ByteFormatException(
            "illegal / out-of-sequence row number " + nextRn + " at index " + index);
      if (size < 3 * SldgConstants.HASH_WIDTH)
        throw new ByteFormatException(
            "illegal crumtrail size " + size + " at index " + index);
      
      rowNums.add(nextRn);
      sizes.add(size);
      
      sizeTotal += size;
      
      lastRn = nextRn;
    }
    
    rowNums = Collections.unmodifiableList(rowNums);
    
    ByteBuffer trailsBlock = BufferUtils.slice(in, sizeTotal);
    if (copy) {
      ByteBuffer buffer = ByteBuffer.allocate(sizeTotal);
      buffer.put(trailsBlock);
      trailsBlock = buffer.flip().asReadOnlyBuffer();
    } else if (!trailsBlock.isReadOnly())
      trailsBlock = trailsBlock.asReadOnlyBuffer();
    
    Partitioning trailParts  = new Partitioning(trailsBlock, sizes);
    
    return new TrailPack(rowNums, trailParts);
  }
  
  
  
  
  
  
  private final List<Long> rows;
  
  private final Partitioning trailParts;
  
  
  
  
  private TrailPack(List<Long> rows, Partitioning trailParts) {
    this.rows = rows;
    this.trailParts = trailParts;
  }
  

  @Override
  public List<Long> trailedRows() {
    return rows;
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    int index = Collections.binarySearch(rows, rowNumber);
    if (index < 0)
      throw new IllegalArgumentException("rowNumber " + rowNumber + " is not a trailed row");
    return CrumTrail.load(trailParts.getPart(index));
  }

}
