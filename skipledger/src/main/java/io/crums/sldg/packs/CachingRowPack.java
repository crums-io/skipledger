/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.util.HashMap;

import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;

/**
 * The base implementation is clean but terribly inefficient. A bit of caching,
 * memo-ization, speeds it up.
 */
public class CachingRowPack extends RowPack {
  
  private final HashMap<Long, SerialRow> cache = new HashMap<>();

  /**
   * Promotion constructor.
   * 
   * @param base
   */
  public CachingRowPack(RowPack base) {
    super(base);
    if (base instanceof CachingRowPack)
      throw new IllegalArgumentException(
          "attempt to create nested instance of this class: " + base);
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
