/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.testing.SelfAwareTestCase;

import static org.junit.jupiter.api.Assertions.*;

import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumFunc;
import io.crums.sldg.reports.pdf.func.NumFuncClosure;
import io.crums.sldg.reports.pdf.func.NumFuncClosure.Value;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.util.json.JsonPrinter;
import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Column;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class SourceRowNumFuncParserTest extends SelfAwareTestCase {
  
  private final SourceRowNumFuncParser parser = SourceRowNumFuncParser.INSTANCE;
  
  @Test
  public void testNoFunc() {
    var rowFunc = new SourceRowNumFunc(List.of(new Column(7)), null);
    assertRoundtrip(rowFunc);
  }
  
  
  @Test
  public void testDivideBy100() {
    
    var divideBy100 = BaseNumFunc.divideBy(100.0);
    var rowFunc = new SourceRowNumFunc(List.of(new Column(0, 7)), divideBy100);
    assertRoundtrip(rowFunc);
  }
  
  
  
  @Test
  public void testWithContext() {
//    var label = new Object() { };
    var paramX = new Param<Number>("X", "amount to multiply by");
    NumFunc numFunc;
    {
      var multiply = new BaseNumFunc(NumNode.binaryBranch(NumOp.MULTIPLY));
      numFunc = new NumFuncClosure(multiply, List.of(new Value(1, new NumberArg(paramX))));
    }
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(paramX));
    
    var rowFunc = new SourceRowNumFunc(List.of(new Column(0, 7)), numFunc);
    
    assertRoundtrip(rowFunc, context, null);
  }
  
  
  
  @Test
  public void testWithContextNotClosed() {
    var label = new Object() { };
    var paramX = new Param<Number>("X", "amount to multiply by");
    NumFunc numFunc = new BaseNumFunc(NumNode.binaryBranch(NumOp.MULTIPLY));
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(paramX));
    
    var rowFunc = new SourceRowNumFunc(List.of(new Column(1, 7)), numFunc);
    
    assertRoundtrip(rowFunc, context, label);
  }
  
  
  
  
  
  
  
  
  
  private void assertRoundtrip(SourceRowNumFunc rowFunc) {
    assertRoundtrip(rowFunc, parser.defaultContext(), null);
  }

  private void assertRoundtrip(SourceRowNumFunc rowFunc, RefContext context, Object label) {
    var jObj = parser.toJsonObject(rowFunc, context);
    var regFunc = parser.toEntity(jObj, context);
    assertEquals(rowFunc, regFunc);
    if (label != null) {
      System.out.println("Test: " + method(label));
      JsonPrinter.println(jObj);
    }
  }

}
