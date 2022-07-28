/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model.pred;


import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.PNode.Op;
import io.crums.sldg.reports.pdf.pred.dep.ColumnValuePredicate;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.LongValue;

/**
 * 
 */
public class PNodeCellPredicateTest {
  
  @Test
  public void testOrLeaves() {
    
    final long benchmark = 27L;
    
    
    ColumnValuePredicate[] cellPredicates = {
        ColumnValuePredicate.lessThan(benchmark - 2),
        ColumnValuePredicate.equalTo(benchmark),
        ColumnValuePredicate.greaterThan(benchmark + 2),
    };
    
    PNode<ColumnValue, ColumnValuePredicate> branch =
        PNode.branchLeaves(List.of(cellPredicates), PNode.Op.OR);

    assertTrue(branch.test(new LongValue(benchmark - 3)));
    assertFalse(branch.test(new LongValue(benchmark - 2)));
    assertFalse(branch.test(new LongValue(benchmark - 1)));
    assertTrue(branch.test(new LongValue(benchmark)));
    assertFalse(branch.test(new LongValue(benchmark + 1)));
    assertFalse(branch.test(new LongValue(benchmark + 2)));
    assertTrue(branch.test(new LongValue(benchmark + 3)));
    
  }
  

  @Test
  public void testAndLeaves() {
    
    // verify any type of Number works
    final byte benchmark = 27;
    
    
    ColumnValuePredicate[] cellPredicates = {
        ColumnValuePredicate.lessThan(benchmark + 2),
        ColumnValuePredicate.equalTo(benchmark),
        ColumnValuePredicate.greaterThan(benchmark - 2),
    };
    
    PNode<ColumnValue, ColumnValuePredicate> branch =
        PNode.branchLeaves(List.of(cellPredicates), PNode.Op.AND);
    
    assertFalse(branch.test(new LongValue(benchmark - 1)));
    assertTrue(branch.test(new LongValue(benchmark)));
    assertFalse(branch.test(new LongValue(benchmark + 1)));
    
  }
  
  
  @Test
  public void testAndBranches() {
    
    // verify any type of Number works
    final short benchmark = 27;
    
    ColumnValuePredicate[] lhs = {
        ColumnValuePredicate.lessThan(benchmark + 2),
        ColumnValuePredicate.greaterThan(benchmark - 2),
    };
    
    ColumnValuePredicate[] rhs = {
        ColumnValuePredicate.lessThan(benchmark + 1),
        ColumnValuePredicate.equalTo(benchmark),
        ColumnValuePredicate.greaterThan(benchmark - 1),
    };
    
    PNode<ColumnValue, ColumnValuePredicate> leftBranch =
        PNode.branchLeaves(List.of(lhs), Op.AND);
    
    PNode<ColumnValue, ColumnValuePredicate> rightBranch =
        PNode.branchLeaves(List.of(rhs), Op.AND);
    
    PNode<ColumnValue, ColumnValuePredicate> root =
        PNode.branch(List.of(leftBranch, rightBranch), Op.AND);

    
    assertFalse(root.test(new LongValue(benchmark - 1)));
    assertTrue(root.test(new LongValue(benchmark)));
    assertFalse(root.test(new LongValue(benchmark + 1)));
  }

}










