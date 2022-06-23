/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model.pred;


import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.model.pred.CellPredicate;
import io.crums.sldg.reports.pdf.model.pred.PNode;
import io.crums.sldg.reports.pdf.model.pred.PNode.Op;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.LongValue;

/**
 * 
 */
public class PNodeCellPredicateTest {
  
  @Test
  public void testOrLeaves() {
    
    final long benchmark = 27L;
    
    
    CellPredicate[] cellPredicates = {
        CellPredicate.lessThan(benchmark - 2),
        CellPredicate.equalTo(benchmark),
        CellPredicate.greaterThan(benchmark + 2),
    };
    
    PNode<ColumnValue, CellPredicate> branch =
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
    
    
    CellPredicate[] cellPredicates = {
        CellPredicate.lessThan(benchmark + 2),
        CellPredicate.equalTo(benchmark),
        CellPredicate.greaterThan(benchmark - 2),
    };
    
    PNode<ColumnValue, CellPredicate> branch =
        PNode.branchLeaves(List.of(cellPredicates), PNode.Op.AND);
    
    assertFalse(branch.test(new LongValue(benchmark - 1)));
    assertTrue(branch.test(new LongValue(benchmark)));
    assertFalse(branch.test(new LongValue(benchmark + 1)));
    
  }
  
  
  @Test
  public void testAndBranches() {
    
    // verify any type of Number works
    final short benchmark = 27;
    
    CellPredicate[] lhs = {
        CellPredicate.lessThan(benchmark + 2),
        CellPredicate.greaterThan(benchmark - 2),
    };
    
    CellPredicate[] rhs = {
        CellPredicate.lessThan(benchmark + 1),
        CellPredicate.equalTo(benchmark),
        CellPredicate.greaterThan(benchmark - 1),
    };
    
    PNode<ColumnValue, CellPredicate> leftBranch =
        PNode.branchLeaves(List.of(lhs), Op.AND);
    
    PNode<ColumnValue, CellPredicate> rightBranch =
        PNode.branchLeaves(List.of(rhs), Op.AND);
    
    PNode<ColumnValue, CellPredicate> root =
        PNode.branch(List.of(leftBranch, rightBranch), Op.AND);

    
    assertFalse(root.test(new LongValue(benchmark - 1)));
    assertTrue(root.test(new LongValue(benchmark)));
    assertFalse(root.test(new LongValue(benchmark + 1)));
  }

}










