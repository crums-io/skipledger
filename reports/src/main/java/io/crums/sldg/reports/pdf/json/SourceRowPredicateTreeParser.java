/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.sldg.src.SourceRow;
import io.crums.util.json.simple.JSONObject;
import io.crums.sldg.reports.pdf.pred.PNode.Leaf;

/**
 * 
 */
public class SourceRowPredicateTreeParser extends PNodeParser<SourceRow, SourceRowPredicate> {

  public SourceRowPredicateTreeParser() {  }

  
  public SourceRowPredicateTreeParser(RefContext context) {
    super(context);
  }




  @Override
  protected void injectLeaf(
      Leaf<SourceRow, SourceRowPredicate> leaf, JSONObject jObj, EditableRefContext context) {
    
    var srcPredicate = leaf.getPredicate();
    // 
  }


  @Override
  protected PNode<SourceRow, SourceRowPredicate> toLeaf(JSONObject jObj, RefContext context) {
    return null;
  }

}
