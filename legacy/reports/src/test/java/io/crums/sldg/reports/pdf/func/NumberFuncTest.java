/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.func;


import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class NumberFuncTest {

  
  @Test
  public void testPlusOne() {
    var lhs = NumNode.newArgLeaf();
    var rhs = NumNode.newLeaf(1);
    var result = NumNode.newBranch(NumOp.ADD, List.of(lhs, rhs));
    var func = new BaseNumFunc(result);
    assertEquals(1, func.eval(0));
    assertEquals(2, func.eval(1));
  }

}














