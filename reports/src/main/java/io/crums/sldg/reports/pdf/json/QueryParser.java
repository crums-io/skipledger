/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;

import io.crums.sldg.reports.pdf.input.Query;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class QueryParser implements ContextedParser<Query> {
  
  public final static QueryParser INSTANCE = new QueryParser();
  
  public final static String PRED_TREE = "predTree";

  
  private final SourceRowPredicateTreeParser treeParser = SourceRowPredicateTreeParser.INSTANCE;
  
  @Override
  public Query toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    var jTree = JsonUtils.getJsonObject(jObj, PRED_TREE, true);
    var predTree = treeParser.toEntity(jTree, context);
    try {
      return new Query(predTree);
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("on creating Query: " + iax.getMessage(), iax);
    }
  }

  @Override
  public JSONObject injectEntity(Query query, JSONObject jObj, RefContext context) {
    jObj.put(PRED_TREE, treeParser.toJsonObject(query.predicateTree(), context));
    return jObj;
  }
  
  

}
