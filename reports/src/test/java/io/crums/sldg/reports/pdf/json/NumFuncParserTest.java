/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class NumFuncParserTest implements ParserRoundtripTest<BaseNumFunc> {

  
  @Test
  public void testPlusOne() throws Exception {
    var children = List.of(
        NumNode.newArgLeaf(), NumNode.newLeaf(1));
    var func = new BaseNumFunc(NumNode.newBranch(NumOp.ADD, children));
    testRoundtrip(func);
  }
  
  
  @Override
  public void observeJson(JSONObject jObj, BaseNumFunc expected) {
    JsonPrinter.println(jObj);
  }
  
  
  @Override
  public JsonEntityParser<BaseNumFunc> parser() {
    return BaseNumFuncParser.INSTANCE;
  }

}





