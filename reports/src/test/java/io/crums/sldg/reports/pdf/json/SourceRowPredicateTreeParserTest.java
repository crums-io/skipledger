/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Column;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.sldg.reports.pdf.pred.BoolComp;
import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.sldg.src.SourceRow;
import io.crums.util.json.JsonPrinter;

/**
 * 
 */
public class SourceRowPredicateTreeParserTest extends SelfAwareTestCase {
  
  private final SourceRowPredicateTreeParser parser = SourceRowPredicateTreeParser.INSTANCE;
  
  
  
  @Test
  public void test00() {
    Object label = null; // new Object() { };
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(7)), null);
    var rowPred = new SourceRowPredicate(0, BoolComp.LT, rowFunc);
    assertRoundtrip(PNode.leaf(rowPred), parser.defaultContext(), label);
  }
  
  @Test
  public void test00WithFunc() {
    Object label = new Object() { };

    NumFunc func = BaseNumFunc.divideBy(100.0);
    
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(0, 7)), func);
    var param = new Param<Number>("thingaming");
    
    var rowPred = new SourceRowPredicate(0, BoolComp.GT, rowFunc);
    
    var context = new EditableRefContext();
    context.putNumberArg(param);
    
    assertRoundtrip(PNode.leaf(rowPred), parser.defaultContext(), label);
  }
  
  @Test
  public void testWithBranch() {
    Object label = null; // new Object() { };
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(7)), null);
    var gt = new SourceRowPredicate(rowFunc, BoolComp.GT, 0);
    var lte = new SourceRowPredicate(rowFunc, BoolComp.LTE, 10);
    
    assertRoundtrip(
        PNode.branchLeaves(List.of(gt, lte), PNode.Op.AND),
        parser.defaultContext, label);
  }
  
  
  

  private void assertRoundtrip(PNode<SourceRow, SourceRowPredicate> expected, RefContext context, Object label) {
    var jObj = parser.toJsonObject(expected, context);
    if (label != null) {
      System.out.println("Test: " + method(label));
      JsonPrinter.println(jObj);
    }
    var actual = parser.toEntity(jObj, context);
    assertEquals(expected, actual);
  }

}













