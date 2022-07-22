/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

import java.util.Optional;

import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.pred.ColumnValuePredicate;
import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.src.ColumnValue;


/**
 * Column value predicate tree parser.
 * 
 * @see PNodeParser
 */
public class ColumnValuePredicateParser extends PNodeParser<ColumnValue, ColumnValuePredicate> {

  
  /**
   * Stateless instance. If you use this instance on the write-path,
   * then you must supply your own {@linkplain EditableRefContext instance};
   * otherwise the {@code toJsonXxx} methods will fail with an {@code IllegalArgumentException}.
   */
  public final static ColumnValuePredicateParser INSTANCE = new ColumnValuePredicateParser();
  
  
  public final static String EQ = "=";
  public final static String NEQ = "!=";
  public final static String GT = ">";
  public final static String GTE = ">=";
  public final static String LT = "<";
  public final static String LTE = "<=";
  
  public final static String RHS = "rhs";
  public final static String RHS_REF = "rhsRef";
  

  
  private ColumnValuePredicateParser() {  }
  

  /** @param context the default context (not null) */
  public ColumnValuePredicateParser(RefContext context) {
    super(context);
  }
  
  

  // - - -   R E A D   P A T H   - - -
  

  @Override
  protected PNode<ColumnValue, ColumnValuePredicate> toLeaf(JSONObject jObj, RefContext context) {
    Number rhs = JsonUtils.getNumber(jObj, RHS, false);
    if (rhs == null) {
      String ref = JsonUtils.getString(jObj, RHS_REF, true);
      rhs = context.getNumberArg(ref);
    }
    var op = JsonUtils.getString(jObj, OP, true);
    var cellPredicate = toCellPredicate(op, rhs);
    return PNode.leaf(cellPredicate);
  }
  
  
  private ColumnValuePredicate toCellPredicate(String op, Number rhs) {
    switch (op) {
    case EQ:
      return ColumnValuePredicate.equalTo(rhs);
    case NEQ:
      return ColumnValuePredicate.notEqualTo(rhs);
    case GT:
      return ColumnValuePredicate.greaterThan(rhs);
    case GTE:
      return ColumnValuePredicate.greaterThanOrEqualTo(rhs);
    case LT:
      return ColumnValuePredicate.lessThan(rhs);
    case LTE:
      return ColumnValuePredicate.lessThanOrEqualTo(rhs);
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
      PNode.Leaf<ColumnValue, ColumnValuePredicate> pNode, JSONObject jObj, EditableRefContext context) {
    
    ColumnValuePredicate cellPredicate = pNode.getPredicate();
    
    if (cellPredicate.rhs().isPresent())
      injectNumberPredicate(cellPredicate, jObj, context);
    else
      throw new JsonParsingException("failed to write JSON for cell predicate: " + cellPredicate);
  }
  
  // TODO: add String predicate support
  private void injectNumberPredicate(
      ColumnValuePredicate numPredicate, JSONObject jObj, EditableRefContext context) {
    
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

  private void injectType(ColumnValuePredicate numPredicate, JSONObject jObj) {
    if (numPredicate instanceof ColumnValuePredicate.NumberEquals)
      jObj.put(OP, EQ);
    else if (numPredicate instanceof ColumnValuePredicate.NotNumberEquals)
      jObj.put(OP, NEQ);
    else if (numPredicate instanceof ColumnValuePredicate.Greater)
      jObj.put(OP, GT);
    else if (numPredicate instanceof ColumnValuePredicate.GreaterOrEqual)
      jObj.put(OP, GTE);
    else if (numPredicate instanceof ColumnValuePredicate.Lesser)
      jObj.put(OP, LT);
    else if (numPredicate instanceof ColumnValuePredicate.LesserOrEqual)
      jObj.put(OP, LTE);
    else
      throw new JsonParsingException("unsupported cell predicate: " + numPredicate);
  }
  
  
  

}
