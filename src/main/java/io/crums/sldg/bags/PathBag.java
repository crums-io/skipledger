/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;


import java.util.List;

import io.crums.sldg.PathInfo;
import io.crums.util.Tuple;

/**
 * 
 */
public interface PathBag {
  
  
  List<Tuple<Long,Long>> beaconRows();
  
  List<PathInfo> declaredPaths();
  
  

}
