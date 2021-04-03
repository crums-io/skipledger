/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.crums.model.HashUtc;
import io.crums.sldg.PathInfo;
import io.crums.sldg.bags.PathBag;
import io.crums.sldg.db.ByteFormatException;
import io.crums.util.Lists;
import io.crums.util.Tuple;


/**
 * 
 * <h2>Serial Format</h2>
 * <p>
 * The serial format is the concatenation of 2 sections BEACON_LIST and PATH_INFOS.
 * </p>
 * 
 * <h4>BEACON_LIST</h4>
 * <p>
 * <pre>
 *    BEACON_CNT  := SHORT      // number of beacons
 *    RN          := LONG       // row number
 *    UTC         := LONG       // beacon time in UTC milliseconds
 *    
 *    BEACON_ITEM := RN UTC
 *    BEACON_LIST := BEACON_CNT [BEACON_ITEM ^BEACON_CNT]
 * </pre>
 * </p>
 * <h4>PATH_INFOS</h4>
 * <p>
 * 
 * </p><p>
 * <pre>
 *    PATH_CNT    := SHORT                              // number of declared paths (<= 64k)
 *    
 *    // per PathInfo
 *    ROW_CNT     := SHORT                              // declarations too are brief
 *    PATH_INFO   := ROW_CNT [RN ^ROW_CNT]
 *                   
 *    
 *    PATH_INFOS  := PATH_CNT [PATH_INFO ^PATH_CNT]     // but note ea PATH_INFO is var-width
 * </pre>
 * </p>
 * <h4>PATH_BAG</h4>
 * <p>
 * 
 * </p><p>
 * <pre>
 *    PATH_BAG    := BEACON_LIST PATH_INFOS
 * </pre>
 * </p>
 */
public class PathPack implements PathBag {
  
  
  
  
  public static PathPack load(ByteBuffer in) {
    final int beaconCount = 0xffff & in.getShort();
    
    List<Tuple<Long,Long>> beacons;
    if (beaconCount == 0)
      beacons = Collections.emptyList();
    else {

      ArrayList<Tuple<Long,Long>> tuples = new ArrayList<>(beaconCount);
      Tuple<Long,Long> prev = new Tuple<>(0L, HashUtc.INCEPTION_UTC);
      
      for (int countdown = beaconCount; countdown-- > 0; ) {
        Long rn = in.getLong();
        Long utc = in.getLong();
        Tuple<Long,Long> next = new Tuple<>(rn, utc);
        if (rn <= prev.a || utc < prev.b)
          throw new ByteFormatException("illegal / out-of-sequence beacon record: " + next);
        tuples.add(next);
        prev = next;
      }
      
      beacons = Collections.unmodifiableList(tuples);
    }
    
    final int pathCount = 0xffff & in.getShort();
    
    List<PathInfo> declaredPaths;
    if (pathCount == 0)
      declaredPaths = Collections.emptyList();
    else {
      
      ArrayList<PathInfo> paths = new ArrayList<>(pathCount);
      for (int index = 0; index < pathCount; ++index) {
        final int rowCount = 0xffff & in.getShort();
        if (rowCount < 0)
          throw new ByteFormatException(
              "negative row count " + rowCount + " for path-info at index" + index);
        Long[] rns = new Long[rowCount];
        Long prev = 0L;
        for (int r = 0; r < rowCount; ++r) {
          Long next = in.getLong();
          if (next <= prev)
            throw new ByteFormatException(
                "illegal / out-of-sequence row number " + next + " for path-info at index " + index);
          rns[r] = next;
        }
        paths.add( new PathInfo(Lists.asReadOnlyList(rns)) );
      }
      declaredPaths = Collections.unmodifiableList(paths);
      
    }
    
    return new PathPack(beacons, declaredPaths);
  }
  
  
  
  
  
  private final List<Tuple<Long,Long>> beacons;
  
  private final List<PathInfo> declaredPaths;
  
  
  
  private PathPack(List<Tuple<Long,Long>> beacons, List<PathInfo> declaredPaths) {
    this.beacons = beacons;
    this.declaredPaths = declaredPaths;
  }
  

  @Override
  public List<Tuple<Long, Long>> beaconRows() {
    return beacons;
  }

  @Override
  public List<PathInfo> declaredPaths() {
    return declaredPaths;
  }

}
