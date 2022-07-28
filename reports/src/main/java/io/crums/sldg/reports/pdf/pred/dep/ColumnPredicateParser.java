/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred.dep;


import io.crums.sldg.reports.pdf.json.EditableRefContext;
import io.crums.sldg.reports.pdf.json.PNodeParser;
import io.crums.sldg.reports.pdf.json.RefContext;
import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.src.SourceRow;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * Column predicate parser tree.
 * 
 * @see PNodeParser
 */
public class ColumnPredicateParser extends PNodeParser<SourceRow, ColumnPredicate> {

  /**
   * Stateless instance. If you use this instance on the write-path,
   * then you must supply your own {@linkplain EditableRefContext instance};
   * otherwise the {@code toJsonXxx} methods will fail with an {@code IllegalArgumentException}.
   */
  public final static ColumnPredicateParser INSTANCE = new ColumnPredicateParser();
  
  public final static String COL = "col";
  public final static String COND = "cond";
  
  
  private ColumnPredicateParser() {  }

  /** @param context the default context (not null) */
  public ColumnPredicateParser(RefContext context) {
    super(context);
  }

  

  @Override
  protected PNode<SourceRow, ColumnPredicate> toLeaf(JSONObject jObj, RefContext context) {
    int colNo = JsonUtils.getInt(jObj, COL);
    if (colNo <= 0)
      throw new JsonParsingException("illegal column number (" + COL + "): " + colNo);
    
    var jCellPredicate = JsonUtils.getJsonObject(jObj, COND, true);
    var cellPredicate = ColumnValuePredicateParser.INSTANCE.toEntity(jCellPredicate, context);
    var colPredicate = new ColumnPredicate(colNo, cellPredicate);
    return PNode.leaf(colPredicate);
  }



  @Override
  protected void injectLeaf(
      PNode.Leaf<SourceRow, ColumnPredicate> pNode, JSONObject jObj, EditableRefContext context) {
    
    var columnPredicate = pNode.getPredicate();
    jObj.put(COL, columnPredicate.columnNumber());
    var cellPredicate = pNode.getPredicate().getCellPredicate();
    jObj.put(COND, ColumnValuePredicateParser.INSTANCE.toJsonObject(cellPredicate, context));
  }

}
