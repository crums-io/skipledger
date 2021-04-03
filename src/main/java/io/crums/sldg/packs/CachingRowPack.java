/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.util.HashMap;

import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;

/**
 * 
 */
public class CachingRowPack extends RowPack {
  
  private final HashMap<Long, SerialRow> cache = new HashMap<>();

  public CachingRowPack(RowPack base) {
    super(base);
  }

  @Override
  public Row getRow(long rowNumber) {
    synchronized (cache) {
      Long rn = rowNumber;
      SerialRow cachedRow = cache.get(rn);
      if (cachedRow == null) {
        Row row = super.getRow(rowNumber);
        // this cascades, but note that entering a synchronized
        // block when the monitor is already-held is cheap
        cachedRow = new SerialRow(row);
        cache.put(rn, cachedRow);
      }
      
      return cachedRow;
    }
  }

}
