/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import io.crums.io.Serial;
import io.crums.model.CrumTrail;
import io.crums.sldg.bags.TrailBag;
import io.crums.util.Lists;

/**
 * A mutable {@code TrailBag} whose serial form can be loaded as an immutable {@linkplain TrailPack}.
 */
public class TrailPackBuilder implements TrailBag, Serial {
  
  private final SortedMap<Long, CrumTrail> trailedRows = new TreeMap<>();
  
  
  
  
  
  
  public int addAll(TrailPack pack) {
    Objects.requireNonNull(pack, "null pack");
    int count = 0;
    synchronized (trailedRows) {
      if (trailedRows.isEmpty()) {
        pack.trailedRows().forEach(rn -> trailedRows.put(rn, pack.crumTrail(rn)));
        count = trailedRows.size();
      } else {
        checkCapacity(pack.trailedRows().size());
        for (Long rn : pack.trailedRows()) {
          CrumTrail prev = trailedRows.put(rn, pack.crumTrail(rn));
          if (prev == null)
            ++count;
          else
            // undo
            trailedRows.put(rn, prev);
        }
      }
    }
    return count;
  }
  
  
  private void checkCapacity(int adds) {
    if (trailedRows.size() + adds > 0xffff)
      throw new IllegalArgumentException("TrailPack overflow. Current size: " + trailedRows.size());
  }
  
  
  public boolean addTrail(long rowNumber, CrumTrail trail) {
    Objects.requireNonNull(trail, "null trail");
    if (rowNumber < 1)
      throw new IllegalArgumentException("rowNumber " + rowNumber);
    synchronized (trailedRows) {
      checkCapacity(1);
      CrumTrail prev = trailedRows.put(rowNumber, trail);
      if (prev == null)
        return true;
      
      // undo
      trailedRows.put(rowNumber, prev);
      return false;
    }
  }

  
  
  @Override
  public List<Long> trailedRows() {
    synchronized (trailedRows) {
      return Lists.readOnlyCopy(trailedRows.keySet());
    }
  }

  @Override
  public CrumTrail crumTrail(long rowNumber) {
    CrumTrail trail;
    synchronized (trailedRows) {
      trail = trailedRows.get(rowNumber);
    }
    if (trail == null)
      throw new IllegalArgumentException("row " + rowNumber + " is not a trailed row");
    return trail;
  }


  @Override
  public int serialSize() {
    // TODO Auto-generated method stub
    return 0;
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    
    synchronized (trailedRows) {
      
      final int count = trailedRows.size();
      assert count <= 0xffff;
      out.putShort((short) count);
      
      for (Long rn : trailedRows.keySet())
        out.putLong(rn).putInt(trailedRows.get(rn).serialSize());
      
      for (CrumTrail trail : trailedRows.values())
        trail.writeTo(out);
      
    }
    return out;
  }


  public boolean isEmpty() {
    synchronized (trailedRows) {
      return trailedRows.isEmpty();
    }
  }
  
  

}
