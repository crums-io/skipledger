/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import com.gnahraf.test.SelfAwareTestCase;
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

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class NumFuncParserTest extends SelfAwareTestCase {
  
  final NumFuncParser parser = NumFuncParser.INSTANCE;

  @Test
  public void testBasePlusOne() throws Exception {
    var children = List.of(
        NumNode.newArgLeaf(), NumNode.newLeaf(1));
    var func = new BaseNumFunc(NumNode.newBranch(NumOp.ADD, children));
    assertRoundtrip(func);
  }
  
  
  @Test
  public void testBasePlusOneClosure() throws Exception {

    var children = List.of(NumNode.newArgLeaf(), NumNode.newLeaf(1));
    
    var param = new Param<Number>("myParam", "my param description");
    var value = new Value(0, new NumberArg(param));
    
    var func = new NumFuncClosure(
        new BaseNumFunc(NumNode.newBranch(NumOp.ADD, children)),
        List.of(value));
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(param));
    
    assertRoundtrip(func, context, null);
  }
  
  
  @Test
  public void testXPlusOneTimesY() {
    NumNode root;
    {
      var xPlus1 = NumNode.newBranch(
          NumOp.ADD,
          List.of(NumNode.newArgLeaf(), NumNode.newLeaf(1)));
      root = NumNode.newBranch(
          NumOp.MULTIPLY,
          List.of(xPlus1, NumNode.newArgLeaf()));
    }
    
    var paramX = new Param<Number>("X", "param X description");
    var paramY = new Param<Number>("Y", "param Y description");
    var func = new NumFuncClosure(
        new BaseNumFunc(root),
        List.of(
            new Value(0, new NumberArg(paramX)),
            new Value(1, new NumberArg(paramY))));
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(paramX));
    context.putNumberArg(new NumberArg(paramY));
    
//    var label = new Object() { };
    assertRoundtrip(func, context, null);
  }
  
  
  @Test
  public void testXPlusOneTimesYTimesZMinusOne() {
    NumNode root;
    {
      var xPlus1 = NumNode.newBranch(
          NumOp.ADD,
          List.of(NumNode.newLeaf(1), NumNode.newArgLeaf()));
      var zMinus1 = NumNode.newBranch(
          NumOp.SUBTRACT,
          List.of(NumNode.newArgLeaf(), NumNode.newLeaf(1)));
      root = NumNode.newBranch(
          NumOp.MULTIPLY,
          List.of(
              xPlus1,
              NumNode.newArgLeaf(),
              zMinus1));
    }

    
    var paramX = new Param<Number>("X", "param X description");
    var paramY = new Param<Number>("Y", null, 7);
    var paramZ = new Param<Number>("Z", "Zzzzz.. ");
    
    var func = new NumFuncClosure(
        new BaseNumFunc(root),
        List.of(
            new Value(0, new NumberArg(paramX)),
            new Value(0, new NumberArg(paramY)),
            new Value(1, new NumberArg(paramZ))));
    
    var context = new EditableRefContext();
    context.putNumberArg(new NumberArg(paramX));
    context.putNumberArg(new NumberArg(paramY));
    context.putNumberArg(new NumberArg(paramZ));
    
    var label = new Object() { };
    assertRoundtrip(func, context, label);
    
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private void assertRoundtrip(NumFunc func) {
    assertRoundtrip(func, parser.defaultContext(), null);
  }
  
  private void assertRoundtrip(NumFunc func, RefContext context, Object label) {
    var jObj = parser.toJsonObject(func, context);
    var regFunc = parser.toEntity(jObj, context);
    assertEquals(func, regFunc);
    if (label != null) {
      System.out.println("Test: " + method(label));
      JsonPrinter.println(jObj);
    }
  }

}


