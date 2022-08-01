/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.List;
import java.util.TreeSet;

import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Column;
import io.crums.util.Lists;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class SourceRowNumFuncParser implements ContextedParser<SourceRowNumFunc> {
  
  
  public final static SourceRowNumFuncParser INSTANCE = new SourceRowNumFuncParser();
  
  
  public final static String FUNC_COLS = "funcRowCols";
  public final static String FUNC = "func";
  
  
  private final ColumnParser columnParser = new ColumnParser();
  

  @Override
  public SourceRowNumFunc toEntity(JSONObject jObj, RefContext context)
      throws JsonParsingException {
    
    var cols = columnParser.toEntityList(JsonUtils.getJsonArray(jObj, FUNC_COLS, true));
    var func = NumFuncParser.INSTANCE.parseIfPresent(jObj, FUNC, context);
    try {
      return new SourceRowNumFunc(cols, func);
      
    } catch (IllegalArgumentException | NullPointerException x) {
      throw new JsonParsingException("on creating source row number function: " + x.getMessage());
    }
  }

  
  @Override
  public JSONObject injectEntity(
      SourceRowNumFunc rowFunc, JSONObject jObj, RefContext context) {
    
    jObj.put(FUNC_COLS, columnParser.toJsonArray(rowFunc.getColumns()));
    rowFunc.getFunc().ifPresent(
        func -> jObj.put(FUNC, NumFuncParser.INSTANCE.toJsonObject(func, context)));
    return jObj;
  }
  
  
  
  
  
  public static class ColumnParser implements JsonEntityParser<Column> {
    
    
    public final static ColumnParser INSTANCE = new ColumnParser();
    
    public final static String IDX = "idx";
    /** 1-based column number. Zero denotes row-number. */
    public final static String RCN = "rcn";

    @Override
    public JSONObject injectEntity(Column column, JSONObject jObj) {
      jObj.put(IDX, column.funcIndex());
      jObj.put(RCN, column.srcIndex() + 1);
      return jObj;
    }

    @Override
    public Column toEntity(JSONObject jObj) throws JsonParsingException {
      int funcIndex = JsonUtils.getInt(jObj, IDX);
      if (funcIndex < 0)
        throw new JsonParsingException("negative function index '" + IDX + "': " + funcIndex);
      int cn = JsonUtils.getInt(jObj, RCN);
      if (cn < 0)
        throw new JsonParsingException("negative row column number '" + RCN + "': " + cn);
      
      return new Column(funcIndex, cn - 1);
    }
    
    
    /** Ensures returned list is sorted with no duplicates. */
    @Override
    public List<Column> toEntityList(JSONArray jArray) throws JsonParsingException {
      var cols = JsonEntityParser.super.toEntityList(jArray);
      if (Lists.isSortedNoDups(cols))
        return cols;
      var colSet = new TreeSet<Column>(cols);
      if (colSet.size() != cols.size())
        throw new JsonParsingException(
            "duplicate column number to func index mappings: " + jArray);
      return Lists.asReadOnlyList(colSet.toArray(new Column[colSet.size()]));
    }
    
  }


}
