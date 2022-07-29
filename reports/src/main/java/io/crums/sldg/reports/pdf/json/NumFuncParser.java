/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.sldg.reports.pdf.func.BaseNumFunc;
import io.crums.sldg.reports.pdf.func.NumFunc;
import io.crums.sldg.reports.pdf.func.NumFuncClosure;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;


/**
 * Delegates to either {@linkplain BaseNumFuncParser} or {@linkplain NumFuncClosureParser}.
 * Prepends the {@linkplain #FUNC_TYPE} on the write-path, in order to disambiguate.
 */
public class NumFuncParser implements ContextedParser<NumFunc> {
  
  /** The {@linkplain NumFuncParser#FUNC_TYPE} values. */
  public enum FuncType {
    
    /** For {@linkplain BaseNumFunc}. */
    BASE,
    /** For {@linkplain NumFuncClosure}. */
    CLSR;
    
    static FuncType getType(JSONObject jObj) {
      var name = JsonUtils.getString(jObj, FUNC_TYPE, true);
      try {
        return valueOf(name);
      } catch (Exception x) {
        throw new JsonParsingException("illegal '" + FUNC_TYPE + "': " + name);
      }
    }
    
    JSONObject putType(JSONObject jObj) {
      jObj.put(FUNC_TYPE, name());
      return jObj;
    }
  }

  
  public final static NumFuncParser INSTANCE = new NumFuncParser();

  
  public final static String FUNC_TYPE = "funcType";
  
  
  
  
  
  @Override
  public NumFunc toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    switch (FuncType.getType(jObj)) {
    case BASE:
      return BaseNumFuncParser.INSTANCE.toEntity(jObj);
    case CLSR:
      return NumFuncClosureParser.INSTANCE.toEntity(jObj, context);
    }
    throw new RuntimeException("Unaccounted " + FuncType.getType(jObj));
  }

  @Override
  public JSONObject injectEntity(NumFunc func, JSONObject jObj, RefContext context) {
    if (func instanceof BaseNumFunc base)
      return BaseNumFuncParser.INSTANCE.injectEntity(
          base,
          FuncType.BASE.putType(jObj));
    else if (func instanceof NumFuncClosure clsr)
      return NumFuncClosureParser.INSTANCE.injectEntity(
          clsr,
          FuncType.CLSR.putType(jObj));
    
    throw new RuntimeException("unaccounted NumFunc implementation: " + func.getClass());
  }

}








