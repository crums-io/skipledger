/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import io.crums.sldg.reports.pdf.func.NumNode;
import io.crums.sldg.reports.pdf.func.NumFunc;
import io.crums.sldg.reports.pdf.func.NumOp;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class NumberFuncParser implements JsonEntityParser<NumFunc> {
  
  
  public final static NumberFuncParser INSTANCE = new NumberFuncParser();
  
  
  private final NumNodeParser nodeParser = new NumNodeParser();
  
  

  @Override
  public JSONObject injectEntity(NumFunc func, JSONObject jObj) {
    nodeParser.injectEntity(func.evaluationTree(), jObj);
    return jObj;
  }
  
  
  

  @Override
  public NumFunc toEntity(JSONObject jObj) throws JsonParsingException {
    var root = nodeParser.toEntity(jObj);
    try {
      return new NumFunc(root);
    } catch (Exception x) {
      throw new JsonParsingException(x);
    }
  }
  
  
  public static class NumNodeParser implements JsonEntityParser<NumNode> {

    
    
    public final static String OP = "op";
    
    public final static String SUB = "sub";
    
    public final static String ARG = "arg";
    
    public final static String VAL = "val";

    @Override
    public JSONObject injectEntity(NumNode node, JSONObject jObj) {
      if (node.isLeaf()) {
        var leaf = node.asLeaf();
        Object value = leaf.isSettable() ? ARG : leaf.value();
        jObj.put(VAL, value);
      
      } else {
        var branch = node.asBranch();
        jObj.put(OP, branch.op().symbol());
        var jChildren = toJsonArray(branch.children());
        jObj.put(SUB, jChildren);
      }
      
      return jObj;
    }

    @Override
    public NumNode toEntity(JSONObject jObj) throws JsonParsingException {
      var opSym = JsonUtils.getString(jObj, OP, false);
      
      // if leaf
      if (opSym == null) {
        Object value = jObj.get(VAL);
        if (value == null)
          throw new JsonParsingException("missing value for '" + VAL + "'");
        if (ARG.equals(value))
          return NumNode.newArgLeaf();
        if (value instanceof Number number)
          return NumNode.newLeaf(number);
        
        throw new JsonParsingException("illegal value for '" + VAL + "': " + value);
      
      // else a branch
      } else {
        try {
          NumOp op = NumOp.forSymbol(opSym);
          var jArray = JsonUtils.getJsonArray(jObj, SUB, true);
          var children = toEntityList(jArray);
          return NumNode.newBranch(op, children);
        } catch (JsonParsingException jpx) {
          throw jpx;
        } catch (Exception x) {
          throw new JsonParsingException(x.getMessage(), x);
        }
      }
    }
    
  }

}
