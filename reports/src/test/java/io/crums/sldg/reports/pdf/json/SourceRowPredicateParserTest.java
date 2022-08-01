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
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.sldg.reports.pdf.pred.BoolComp;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.util.json.JsonPrinter;

/**
 * 
 */
public class SourceRowPredicateParserTest extends SelfAwareTestCase {
  
  private final SourceRowPredicateParser parser = SourceRowPredicateParser.INSTANCE;
  
  @Test
  public void test00() {
    Object label = null; // new Object() { };
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(7)), null);
    var rowPred = new SourceRowPredicate(0, BoolComp.LT, rowFunc);
    assertRoundtrip(rowPred, parser.defaultContext(), label);
  }
  
  @Test
  public void test01() {
    Object label = null; // new Object() { };
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(7)), null);
    var rowPred = new SourceRowPredicate(rowFunc, BoolComp.GT, 5);
    assertRoundtrip(rowPred, parser.defaultContext(), label);
  }
  
  @Test
  public void test01WithArg() {
    Object label = null; // new Object() { };
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(7)), null);
    var param = new Param<Number>("thingaming");
    
    var rowPred = new SourceRowPredicate(rowFunc, BoolComp.GT, new NumberArg(param));
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(param));
    assertRoundtrip(rowPred, context, label);
  }

  
  @Test
  public void testWithFunc() {
    Object label = new Object() { };
    NumFunc func = BaseNumFunc.divideBy(100.0);
    
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(new Column(0, 7)), func);
    var param = new Param<Number>("thingaming");
    
    var rowPred = new SourceRowPredicate(rowFunc, BoolComp.GT, new NumberArg(param));
    
    var context = new EditableRefContext();
    context.putNumberArg(param);
    assertRoundtrip(rowPred, context, label);
  }
  
  


  private void assertRoundtrip(SourceRowPredicate rowFunc, RefContext context, Object label) {
    var jObj = parser.toJsonObject(rowFunc, context);
    var regFunc = parser.toEntity(jObj, context);
    assertEquals(rowFunc, regFunc);
    if (label != null) {
      System.out.println("Test: " + method(label));
      JsonPrinter.println(jObj);
    }
  }

}
















