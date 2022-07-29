/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.List;
import java.util.TreeSet;

import io.crums.sldg.reports.pdf.func.NumFuncClosure;
import io.crums.sldg.reports.pdf.func.NumFuncClosure.Value;
import io.crums.util.Lists;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class NumFuncClosureParser implements ContextedParser<NumFuncClosure> {

  public final static NumFuncClosureParser INSTANCE = new NumFuncClosureParser();

  public final static String ARGS = "args";
  public final static String FUNC = "func";
  
  
  private final ValueParser valueParser = new ValueParser();
  
  
  @Override
  public NumFuncClosure toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    var values = valueParser.toEntityList(
        JsonUtils.getJsonArray(jObj, ARGS, true),
        context);
    var func = NumFuncParser.INSTANCE.toEntity(
        JsonUtils.getJsonObject(jObj, FUNC, true),
        context);
    try {
      return new NumFuncClosure(func, values);
    
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("on parsing number func closure: " + iax.getMessage());
    }
  }

  @Override
  public JSONObject injectEntity(NumFuncClosure func, JSONObject jObj, RefContext context) {
    jObj.put(ARGS, valueParser.toJsonArray(func.getBoundValues(), context));
    jObj.put(FUNC, NumFuncParser.INSTANCE.toJsonObject(func, context));
    return jObj;
  }
  
  
  
  
  static class ValueParser implements ContextedParser<NumFuncClosure.Value> {
    
    public final static String IDX = "idx";
    public final static String VAL = "val";

    @Override
    public Value toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
      int index = JsonUtils.getInt(jObj, IDX);
      var value = ArgParsers.getNumber(jObj, VAL, context, true);
      try {
        return new Value(index, value);
      } catch (IndexOutOfBoundsException x) {
        throw new JsonParsingException("bad '" + IDX + "': " + x.getMessage());
      }
    }

    /** @param context actually, not used on the write-path */
    @Override
    public JSONObject injectEntity(Value value, JSONObject jObj, RefContext context)
        throws JsonParsingException {
      jObj.put(IDX, value.index());
      ArgParsers.putNumber(VAL, value.value(), jObj);
      return jObj;
    }
    
    
    /** Returns sorted, ensures no dups. */
    @Override
    public List<Value> toEntityList(JSONArray jArray, RefContext context) throws JsonParsingException {
      var unsorted = ContextedParser.super.toEntityList(jArray, context);
      if (Lists.isSortedNoDups(unsorted))
        return unsorted;
      var sorted = new TreeSet<>(unsorted);
      if (sorted.size() != unsorted.size())
        throw new JsonParsingException("duplicate indexes: " + jArray);
      return Lists.readOnlyCopy(sorted);
    }
    
  }

}
