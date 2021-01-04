/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.util.List;
import java.util.Objects;

import io.crums.util.Lists;
import io.crums.util.Tuple;


/**
 * A concatenation of 2 {@linkplain SkipPath}s. The {@linkplain #target() target} row is
 * the {@linkplain #first() first} row in the tail skip path.
 */
public class CamelPath extends TargetPath {
  
  
  
  
  public static CamelPath concatInstance(SkipPath head, SkipPath tail) {
    Objects.requireNonNull(head, "null head");
    Objects.requireNonNull(tail, "null tail");
    if (head.hiRowNumber() > tail.loRowNumber())
      throw new IllegalArgumentException(
          "head [" + head.hiRowNumber() + "] and tail [" + tail.loRowNumber() + "] in wrong order");
    
    long targetRowNumber = tail.loRowNumber();
    Path joined = head.concat(tail);
    return new CamelPath(joined, targetRowNumber);
  }
  
  
  
  
  public static List<Long> rowNumbers(long lo, long target, long hi) {
    if (lo <= 0 || target < lo || target > hi || hi < lo)
      throw new IllegalArgumentException("lo " + lo + "; target " + target + "; hi " + hi);
    
    List<Long> head = Ledger.skipPathNumbers(lo, target);
    List<Long> tail = Ledger.skipPathNumbers(target, hi);
    
    // the target number occurs in both head and tail
    // "remove" one of them (we choose head)
    //
    return Lists.concat(head.subList(0, head.size() - 1), tail);
  }
  












  private CamelPath(Path path, long targetRowNumber) {
    super(path, targetRowNumber);
  }
  

  CamelPath(List<Row> path, List<Tuple<Long, Long>> beacons, long targetRowNumber, boolean ignore) {
    super(path, beacons, targetRowNumber, ignore);
  }
  
  
  
  /**
   * Copy constructor.
   */
  protected CamelPath(CamelPath copy) {
    super(copy);
  }
  
  
  
  
  


}



















