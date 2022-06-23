/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

import java.util.Optional;

import io.crums.sldg.reports.pdf.model.NumberArg;
import io.crums.sldg.reports.pdf.model.pred.CellPredicate;
import io.crums.sldg.reports.pdf.model.pred.PNode;
import io.crums.sldg.src.ColumnValue;


/**
 * Cell predicate tree parser.
 * 
 * @see PNodeParser
 */
public class CellPredicateParser extends PNodeParser<ColumnValue, CellPredicate> {

  
  /**
   * Stateless instance. If you use this instance on the write-path,
   * then you must supply your own {@linkplain EditableRefContext instance};
   * otherwise the {@code toJsonXxx} methods will fail with an {@code IllegalArgumentException}.
   */
  public final static CellPredicateParser INSTANCE = new CellPredicateParser();
  
  
  public final static String EQ = "=";
  public final static String NEQ = "!=";
  public final static String GT = ">";
  public final static String GTE = ">=";
  public final static String LT = "<";
  public final static String LTE = "<=";
  
  public final static String RHS = "rhs";
  public final static String RHS_REF = "rhsRef";
  

  
  private CellPredicateParser() {  }
  

  /** @param context the default context (not null) */
  public CellPredicateParser(RefContext context) {
    super(context);
  }
  
  

  // - - -   R E A D   P A T H   - - -
  

  @Override
  protected PNode<ColumnValue, CellPredicate> toLeaf(JSONObject jObj, RefContext context) {
    Number rhs = JsonUtils.getNumber(jObj, RHS, false);
    if (rhs == null) {
      String ref = JsonUtils.getString(jObj, RHS_REF, true);
      rhs = context.getNumberArg(ref);
    }
    var op = JsonUtils.getString(jObj, OP, true);
    var cellPredicate = toCellPredicate(op, rhs);
    return PNode.leaf(cellPredicate);
  }
  
  
  private CellPredicate toCellPredicate(String op, Number rhs) {
    switch (op) {
    case EQ:
      return CellPredicate.equalTo(rhs);
    case NEQ:
      return CellPredicate.notEqualTo(rhs);
    case GT:
      return CellPredicate.greaterThan(rhs);
    case GTE:
      return CellPredicate.greaterThanOrEqualTo(rhs);
    case LT:
      return CellPredicate.lessThan(rhs);
    case LTE:
      return CellPredicate.lessThanOrEqualTo(rhs);
    default:
      throw new JsonParsingException("unknown op: " + op);
    }
  }

  
  
  
  
  
  
  // - - -   W R I T E   P A T H   - - -
  
  
  

  /**
   * @param pNode     in fact, a leaf
   * @param argNames  arguments are gathered into this map
   * @return  {@code argNames} (so to remind it's being updated!)
   */
  protected void injectLeaf(
      PNode.Leaf<ColumnValue, CellPredicate> pNode, JSONObject jObj, EditableRefContext context) {
    
    CellPredicate cellPredicate = pNode.getPredicate();
    
    if (cellPredicate.rhs().isPresent())
      injectNumberPredicate(cellPredicate, jObj, context);
    else
      throw new JsonParsingException("failed to write JSON for cell predicate: " + cellPredicate);
  }
  
  // TODO: add String predicate support
  private void injectNumberPredicate(
      CellPredicate numPredicate, JSONObject jObj, EditableRefContext context) {
    
    injectType(numPredicate, jObj);
    Number rhs;
    {
      Optional<?> expected = numPredicate.rhs();
      if (expected.isEmpty())
        throw new JsonParsingException("expected RHS for cell predicate missing");
      if (expected.get() instanceof Number n)
        rhs = n;
      else
        throw new JsonParsingException("unsupported RHS for cell predicate: " + expected.get());
    }
    if (rhs instanceof NumberArg argNum) {
      var name = argNum.param().name();
      context.numberArgs().put(name, argNum); // for now, ignoring any prev booted value
      jObj.put(RHS_REF,  name);
    } else {
      jObj.put(RHS, rhs);
    }
  }

  private void injectType(CellPredicate numPredicate, JSONObject jObj) {
    if (numPredicate instanceof CellPredicate.NumberEquals)
      jObj.put(OP, EQ);
    else if (numPredicate instanceof CellPredicate.NotNumberEquals)
      jObj.put(OP, NEQ);
    else if (numPredicate instanceof CellPredicate.Greater)
      jObj.put(OP, GT);
    else if (numPredicate instanceof CellPredicate.GreaterOrEqual)
      jObj.put(OP, GTE);
    else if (numPredicate instanceof CellPredicate.Lesser)
      jObj.put(OP, LT);
    else if (numPredicate instanceof CellPredicate.LesserOrEqual)
      jObj.put(OP, LTE);
    else
      throw new JsonParsingException("unsupported cell predicate: " + numPredicate);
  }
  
  
  

}
