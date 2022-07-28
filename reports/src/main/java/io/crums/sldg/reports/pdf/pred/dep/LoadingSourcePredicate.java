/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred.dep;

import java.util.List;
import java.util.function.Predicate;

import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class LoadingSourcePredicate implements Predicate<SourceRow> {
  
  /** Column indexes in source row */
  private List<Integer> colIndexes;
//  private 
  

  /**
   * 
   */
  public LoadingSourcePredicate() {
  }

  @Override
  public boolean test(SourceRow row) {
    return false;
  }

}
