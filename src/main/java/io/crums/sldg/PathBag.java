/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.util.List;

import io.crums.util.Tuple;

/**
 * 
 */
public interface PathBag extends RowBag {
  
  
  List<Tuple<Long,Long>> beaconRows();
  
  List<PathInfo> availablePaths();
  
  

}
