/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.NumberFunc;
import io.crums.sldg.reports.pdf.func.NumberOp;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class NumberFuncParserTest implements ParserRoundtripTest<NumberFunc> {

  
  @Test
  public void testPlusOne() throws Exception {
    var children = List.of(
        NumNode.newArgLeaf(), NumNode.newLeaf(1));
    var func = new NumberFunc(NumNode.newBranch(NumberOp.ADD, children));
    testRoundtrip(func);
  }
  
  
  @Override
  public void observeJson(JSONObject jObj, NumberFunc expected) {
    JsonPrinter.println(jObj);
  }
  
  
  @Override
  public JsonEntityParser<NumberFunc> parser() {
    return NumberFuncParser.INSTANCE;
  }

}





