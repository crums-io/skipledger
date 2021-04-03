/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import io.crums.io.Serial;
import io.crums.model.HashUtc;
import io.crums.sldg.PathInfo;
import io.crums.sldg.Row;
import io.crums.sldg.bags.PathBag;
import io.crums.util.Lists;
import io.crums.util.Tuple;

/**
 * A mutable {@code PathBag} whose serial form can be loaded as an immutable {@linkplain PathPack}.
 */
public class PathPackBuilder implements PathBag, Serial {
  
  /**
   * The maximum number of items per type: beacon rows, or declared paths.
   */
  public final static int MAX_COUNT = 0xffff;
  
  private SortedMap<Long,Long> beaconUtcs = new TreeMap<>();
  
  
  
  private List<PathInfo> declaredPaths = new ArrayList<>();
  
  
  
  /**
   * Adds the given beacon row and its UTC max time. Recall a beacon row contains the
   * root hash of a Merkle tree published at crums.io as its {@linkplain Row#inputHash() input}.
   * This method does rudimentary checks on the consistency of the input arguments.
   * (This is just an opportunity to fail garbage data before it fails downstream.)
   * 
   * @param rowNumber &ge; 1
   * @param utc       the advertised time for the beacon (here the UTC of the youngest
   *                  crum as a leaf node in a crums.io Merkle tree
   * 
   * @return {@code true} <b>iff</b> the given beacon row was added
   */
  public boolean addBeaconRow(long rowNumber, long utc) {
    if (rowNumber <= 0)
      throw new IllegalArgumentException("illegal rowNumber " + rowNumber);
    if (utc < HashUtc.INCEPTION_UTC)
      throw new IllegalArgumentException("illegal UTC " + utc);

    final Long rn = rowNumber;
    
    synchronized (beaconUtcs) {
      
      final int count = beaconUtcs.size();
      
      if (count > 0) {
        if (count == MAX_COUNT)
          throw new IllegalStateException("instance is maxed out at " + MAX_COUNT + " beacons");
        
        // check the beacon row's neigbors
        SortedMap<Long,Long> tail = beaconUtcs.tailMap(rn);
        if (!tail.isEmpty()) {
          final Long first = tail.firstKey();
          if (first.longValue() == rowNumber) { // (No need to explicitly unbox it.. still, clearer this way)
            
            if (tail.get(first).longValue() == utc)
              return false;
            
            throw new IllegalArgumentException(
                "utc " + utc + " for row " + rn + " conflicts with exising utc value " + tail.get(first));
          } else {
            
            assert first > rowNumber;
            long nextUtc = tail.get(first);
            if (nextUtc < utc)
              throw new IllegalArgumentException(
                  "utc " + utc + " for row " + rn + " is greater than utc of next beacon at row " + first);
          }
        }
        
        SortedMap<Long,Long> head = beaconUtcs.headMap(rn);
        if (!head.isEmpty()) {
          Long last = head.lastKey();
          if (head.get(last) > utc)
            throw new IllegalArgumentException(
                "utc " + utc + " for row " + rn + " is less than utc of previous beacon at row " + last);
        }
      }
      
      beaconUtcs.put(rn, utc);
      return true;
    }
  }
  
  /**
   * Adds the given path declaration, if it doesn't already exist.
   * <p>
   * Note adding <em>n</em> of these takes <em>n<sup><tiny>2</tiny></sup></em> operations. We
   * don't expect that many declared paths (our file format maxes out at 64k), but even if usage
   * ever nears that magnitude, we'll need to consider sorting this in order improve performance.
   * </p>
   * 
   * @return {@code true} <b>iff</b> the given {@code path} was added
   */
  public boolean addDeclaredPath(PathInfo path) {
    if (Objects.requireNonNull(path, "null path").declaration().size() > MAX_COUNT)
      throw new IllegalArgumentException(
          "path declaration size (" + path.declaration().size() + ") exceeds implementation maximum " +
          MAX_COUNT);
    
    synchronized (declaredPaths) {
      
      if (declaredPaths.contains(path))
        return false;
      
      if (declaredPaths.size() == MAX_COUNT)
        throw new IllegalStateException("instance is maxed out at " + MAX_COUNT + " declared paths");
        
      declaredPaths.add(path);
      return true;
    }
  }
  
  
  
  /**
   * Adds all the data from the given {@code pathPack} to this instance. When invoked on an
   * empty instance, fewer checks are necessary and is consequently more efficient.
   * 
   * @return the number of objects added (path declarations and beacons)
   */
  public int addPathPack(PathPack pathPack) {
    Objects.requireNonNull(pathPack, "null pathPack");
    
    int bcnCount;
    
    synchronized (beaconUtcs) {
      if (beaconUtcs.isEmpty()) {
        pathPack.beaconRows().forEach(bcn -> beaconUtcs.put(bcn.a, bcn.b));
        bcnCount = pathPack.beaconRows().size();
      } else {
        int[] count = { 0 };
        pathPack.beaconRows().forEach(bcn -> { if (addBeaconRow(bcn.a, bcn.b)) ++count[0]; } );
        bcnCount = count[0];
      }
    }
    
    int newPathCount;
    synchronized (declaredPaths) {
      if (declaredPaths.isEmpty()) {
        List<PathInfo> decl = pathPack.declaredPaths();
        declaredPaths.addAll(decl);
        newPathCount = decl.size();
      } else {
        int[] count = { 0 };
        pathPack.declaredPaths().forEach(p -> { if (addDeclaredPath(p)) ++count[0]; } );
        newPathCount = count[0];
      }
    }
    return bcnCount + newPathCount;
  }

  @Override
  public List<Tuple<Long,Long>> beaconRows() {
    synchronized (beaconUtcs) {
      ArrayList<Tuple<Long,Long>> bcnTuples = new ArrayList<>(beaconUtcs.size());
      beaconUtcs.entrySet().forEach(e -> bcnTuples.add(new Tuple<>(e.getKey(), e.getValue())));
      return Collections.unmodifiableList(bcnTuples);
    }
  }

  @Override
  public List<PathInfo> declaredPaths() {
    synchronized (declaredPaths) {
      return Lists.readOnlyCopy(declaredPaths);
    }
  }

  @Override
  public int serialSize() {
    int bytes = 2;
    synchronized (beaconUtcs) {
      bytes += 16 * beaconUtcs.size();
    }
    bytes += 2;
    synchronized (declaredPaths) {
      for (PathInfo decl : declaredPaths)
        bytes += 2 + 8 * decl.declaration().size();
    }
    return bytes;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    
    // BEACON_LIST
    synchronized (beaconUtcs) {
      out.putShort((short) beaconUtcs.size());
      beaconUtcs.entrySet().forEach(e -> out.putLong(e.getKey()).putLong(e.getValue()));
    }
    
    // PATH_INFOS
    synchronized (declaredPaths) {
      out.putShort((short) declaredPaths.size());
      for (PathInfo decl : declaredPaths) {
        out.putShort((short) decl.declaration().size());
        for (Long rn : decl.declaration())
          out.putLong(rn);
      }
    }
    return out;
  }

  public boolean isEmpty() {
    boolean empty;
    synchronized (beaconUtcs) {
      empty = beaconUtcs.isEmpty();
    }
    if (empty) {
      synchronized (declaredPaths) {
        empty = declaredPaths.isEmpty();
      }
    }
    return empty;
  }

}
