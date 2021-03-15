/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;
import io.crums.util.Tuple;

/**
 * A collection of paths from a same ledger.
 */
public class Morsel {
  
  
  
  
  
  private final MorselBag bag;
  
  
  public Morsel(MorselBag bag) {
    this.bag = Objects.requireNonNull(bag, "null bag");
  }
  
  
  public List<PathInfo> availablePaths() {
    return bag.availablePaths();
  }
  
  
  public int pathCount() {
    return bag.availablePaths().size();
  }
  
  
  public Path getPath(int index) {
    
    return getPath(availablePaths().get(index));
  }


  public Path getPath(PathInfo info) {

    final List<Long> rowNumbers = Objects.requireNonNull(info, "null info").rowNumbers();
    
    Row[] rows = new Row[rowNumbers.size()];
    
    final List<Tuple<Long,Long>> beacons = bag.beaconRows();
    int bcIndex = 0;
    long nextBeaconRn = beacons.isEmpty() ? Long.MAX_VALUE : beacons.get(0).a;
    
    
    List<Tuple<Long,Long>> outBeacons = null;
    
    for (int index = 0; index < rows.length; ++index) {
      
      final long rn = rowNumbers.get(index);
      
      rows[index] = bag.getRow(rn);
      
      while (nextBeaconRn <= rn) {
        if (rn == nextBeaconRn) {
          // check init
          if (outBeacons == null)
            outBeacons = new ArrayList<>(2);
          
          outBeacons.add(beacons.get(bcIndex));
        }
        ++bcIndex;
        nextBeaconRn =
            bcIndex == beacons.size() ? Long.MAX_VALUE : beacons.get(bcIndex).a;
        
      }
    }
    
    outBeacons =
        outBeacons == null ?
            Collections.emptyList() : Collections.unmodifiableList(outBeacons);
    
    List<Row> roRows = Lists.asReadOnlyList(rows);
    
    // constructor validates
    return info.isTargeted() ?
        new TargetPath(roRows, outBeacons, info.targetRow(), true) :
          new Path(roRows, outBeacons, true);
    
  }
  

}
