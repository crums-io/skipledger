/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.Collections;
import java.util.List;

import io.crums.util.Lists;

/**
 * 
 */
public class PathInfo {
  
  private final List<Long> rowNumbers;
  private final long targetRn;
  
  
  /**
   * Constructs an un-targeted instance by stitching the given row numbers
   * together.
   * 
   * @param rowNumbers positive, not empty, sorted list of positive row numbers
   */
  public PathInfo(List<Long> rowNumbers) {
    this.rowNumbers = stitch(rowNumbers);
    this.targetRn = this.rowNumbers.get(0);
    
  }
  
  /**
   * Constructs a targeted instance by stitching the given row numbers
   * together.
   * 
   * @param rowNumbers positive, not empty, sorted list of positive row numbers
   * @param targetRn target row number (in {@code rowNumbers} or its stitched version)
   * 
   * @see Ledger#stitch(List)
   */
  public PathInfo(List<Long> rowNumbers, long targetRn) {
    this.rowNumbers = stitch(rowNumbers);
    this.targetRn = targetRn;
    if (Collections.binarySearch(this.rowNumbers, targetRn) < 0)
      throw new IllegalArgumentException(
          "targetRn " + targetRn + " is not in (stitched) list " + rowNumbers);
  }
  
  
  
  
  private List<Long> stitch(List<Long> rowNumbers) {
    return Ledger.stitch(rowNumbers);
  }
  
  
  /**
   * Constructs a skip path instance.
   * 
   * @param lo &ge; 1
   * @param hi &gt; {@code lo}
   */
  public PathInfo(long lo, long hi) {
    this.rowNumbers = Ledger.skipPathNumbers(lo, hi);
    this.targetRn = lo;
  }
  
  
  /**
   * Constructs a camel path instance. A camel path instance is the concatentation
   * of 2 adjacent skip paths meeting at the {@code target} row number.
   * 
   * @param lo &ge; 1
   * @param target &ge; {@code lo}
   * @param hi &gt; {@code target}
   */
  public PathInfo(long lo, long target, long hi) {
    if (lo <= 0 || target < lo || hi <= target)
      throw new IllegalArgumentException(
          "lo " + lo + "; target " + target + "; hi " + hi);
    
    if (target == lo)
      this.rowNumbers = Ledger.skipPathNumbers(lo, hi);
    else {
      List<Long> head = Ledger.skipPathNumbers(lo, target);
      List<Long> tail = Ledger.skipPathNumbers(target, hi);
      tail = tail.subList(1, tail.size());
      this.rowNumbers = Lists.concat(head, tail);
    }
    this.targetRn = target;
  }
  
  
  
  public final List<Long> rowNumbers() {
    return rowNumbers;
  }
  
  
  
  
  public final long targetRow() {
    return targetRn;
  }
  
  
  public final boolean isTargeted() {
    return targetRn != rowNumbers.get(0);
  }

}
