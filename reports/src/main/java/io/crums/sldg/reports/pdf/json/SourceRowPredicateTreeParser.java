/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.sldg.src.SourceRow;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;
import io.crums.sldg.reports.pdf.pred.PNode.Leaf;

/**
 * 
 */
public class SourceRowPredicateTreeParser extends PNodeParser<SourceRow, SourceRowPredicate> {
  
  public final static SourceRowPredicateTreeParser INSTANCE = new SourceRowPredicateTreeParser();
  
  
  public final static String ROW_PREDICATE = "rowPredicate";
  
  
  

  public SourceRowPredicateTreeParser() {  }

  
  public SourceRowPredicateTreeParser(RefContext context) {
    super(context);
  }




  @Override
  protected void injectLeaf(
      Leaf<SourceRow, SourceRowPredicate> leaf, JSONObject jObj, RefContext context) {
    
    jObj.put(
        ROW_PREDICATE,
        SourceRowPredicateParser.INSTANCE.toJsonObject(leaf.getPredicate(), context));
  }


  @Override
  protected PNode<SourceRow, SourceRowPredicate> toLeaf(JSONObject jObj, RefContext context) {
    var jPredicate = JsonUtils.getJsonObject(jObj, ROW_PREDICATE, true);
    return PNode.leaf(
        SourceRowPredicateParser.INSTANCE.toEntity(jPredicate, context));
  }

}





