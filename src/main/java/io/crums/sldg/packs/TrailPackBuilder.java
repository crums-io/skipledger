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
import io.crums.sldg.HashConflictException;
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
        pack.trailedRowNumbers().forEach(rn -> trailedRows.put(rn, pack.crumTrail(rn)));
        count = trailedRows.size();
      } else {
        checkCapacity(pack.trailedRowNumbers().size());
        for (Long rn : pack.trailedRowNumbers()) {
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
      throw new IllegalArgumentException(
          "TrailPack overflow on attempt to add " + adds + ". Current size is " +
          trailedRows.size() + "; Max size is 0xffff");
  }
  
  
  /**
   * Adds the given {@code trail} for the {@code rowNumber}. Verifies hash structure.
   * Enforces crumtrails paired with rows have witness times in ascending order.
   * 
   * @return {@code true} <b>iff</b> the trail was added.
   * 
   * @throws IllegalArgumentException if {@code trail} is out-of-sequence with existing state
   * @throws HashConflictException if {@code trail} is malformed (not self-consistent)
   */
  public boolean addTrail(long rowNumber, CrumTrail trail)
      throws IllegalArgumentException, HashConflictException {
    
    Objects.requireNonNull(trail, "null trail");
    if (rowNumber < 1)
      throw new IllegalArgumentException("rowNumber " + rowNumber);
    
    if (!trail.verify())
      throw new HashConflictException("attempt to add invalid crumtrail for row " + rowNumber);
    
    synchronized (trailedRows) {
      checkCapacity(1);
      
      var headRows = trailedRows.headMap(rowNumber);
      if (!headRows.isEmpty()) {
        var prevTrail = headRows.get(headRows.lastKey());
        if (prevTrail.crum().utc() > trail.crum().utc())
          throw new IllegalArgumentException(
              "attempt to add out-of-sequence crumtrail (witness-utc " + trail.crum().utc() +
              ") for row " + rowNumber + " while crumtrail for row " + headRows.lastKey() +
              " has witness-utc " + prevTrail.crum().utc());
      }
      
      var tailRows = trailedRows.tailMap(rowNumber);
      if (!tailRows.isEmpty()) {
        long rn = tailRows.firstKey();
        var nextTrail = tailRows.get(rn);
        if (rn == rowNumber) {
          if (trail.crum().utc() != nextTrail.crum().utc())
            throw new IllegalArgumentException(
                "attempt to overwrite trail at row " + rn + " (witness-utc " + nextTrail.crum().utc() +
                ") using witness-utc " + trail.crum().utc());
          
          return false;
        } else if (trail.crum().utc() > nextTrail.crum().utc())
          throw new IllegalArgumentException(
              "attempt to add out-of-sequence crumtrail (witness-utc " + trail.crum().utc() +
              ") for row " + rowNumber + " while crumtrail for row " + rn +
              " has witness-utc " + nextTrail.crum().utc());
        
      }
      trailedRows.put(rowNumber, trail);
      return true;
    }
  }

  
  
  @Override
  public List<Long> trailedRowNumbers() {
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
    int bytes = 2; // TRAIL_CNT
    final int count = trailedRows.size();
    if (count != 0) {
      bytes += count * (8 + 4);
      for (var trail : trailedRows.values())
        bytes += trail.serialSize();
    }
    return bytes;
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
