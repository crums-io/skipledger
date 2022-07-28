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
    
    return null;
  }

  @Override
  public JSONObject injectEntity(
      SourceRowNumFunc func, JSONObject jObj, RefContext context) {
    jObj.put(FUNC_COLS, columnParser.toJsonArray(func.getColumns()));
    
    return jObj;
  }
  
  
  
  
  
  public static class ColumnParser implements JsonEntityParser<Column> {
    
    
    public final static ColumnParser INSTANCE = new ColumnParser();
    
    public final static String FN_IDX = "fnIdx";
    public final static String ROW_CN = "rowCn";

    @Override
    public JSONObject injectEntity(Column column, JSONObject jObj) {
      jObj.put(FN_IDX, column.funcIndex());
      jObj.put(ROW_CN, column.srcIndex() + 1);
      return jObj;
    }

    @Override
    public Column toEntity(JSONObject jObj) throws JsonParsingException {
      int funcIndex = JsonUtils.getInt(jObj, FN_IDX);
      if (funcIndex < 0)
        throw new JsonParsingException("negative function index '" + FN_IDX + "': " + funcIndex);
      int cn = JsonUtils.getInt(jObj, ROW_CN);
      if (cn < 0)
        throw new JsonParsingException("negative row column number '" + ROW_CN + "': " + cn);
      
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
