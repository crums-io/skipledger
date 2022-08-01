/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Supplied;
import io.crums.sldg.reports.pdf.pred.BoolComp;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class SourceRowPredicateParser implements ContextedParser<SourceRowPredicate> {
  
  
  public final static SourceRowPredicateParser INSTANCE = new SourceRowPredicateParser();
  
  public final static String LEFT_NUM = "leftNum";
  public final static String LEFT_FUNC = "leftFunc";
  public final static String OP = "op";
  public final static String RIGHT_NUM = "rightNum";
  public final static String RIGHT_FUNC = "rightFunc";
  
  

  
  
  @Override
  public SourceRowPredicate toEntity(JSONObject jObj, RefContext context) throws JsonParsingException {
    var leftNum = ArgParsers.getNumber(jObj, LEFT_NUM, context, false);
    var leftFunc = SourceRowNumFuncParser.INSTANCE.parseIfPresent(jObj, LEFT_FUNC, context);
    
    BoolComp op;
    try {
      op = BoolComp.forSymbol(JsonUtils.getString(jObj, OP, true));
    
    } catch (NoSuchElementException badSymbol) {
      throw new JsonParsingException(
          "bad '" + OP + "' symbol: " + badSymbol.getMessage());
    }
    
    var rightNum = ArgParsers.getNumber(jObj, RIGHT_NUM, context, false);
    var rightFunc = SourceRowNumFuncParser.INSTANCE.parseIfPresent(jObj, RIGHT_FUNC, context);
    
    try {
      
      if (leftNum != null) {
        return new SourceRowPredicate(leftNum, op, toSupplied(rightFunc, RIGHT_FUNC));
      
      } else {  // leftFunc != null
        var leftSp = toSupplied(leftFunc, LEFT_FUNC);
        return rightNum != null ?
            new SourceRowPredicate(leftSp, op, rightNum) :
              new SourceRowPredicate(leftSp, op, toSupplied(rightFunc, RIGHT_FUNC));
      }
      
    } catch (IllegalArgumentException | NullPointerException e) {
      
      throw new JsonParsingException(
          "on creating source row predicate: " + e.getMessage(), e);
    }
  }
  
  
  private SourceRowNumFunc.Supplied toSupplied(SourceRowNumFunc func, String name) {
    Objects.requireNonNull(func, "missing " + name);
    return new SourceRowNumFunc.Supplied(func);
  }

  
  
  @Override
  public JSONObject injectEntity(SourceRowPredicate rowPredicate, JSONObject jObj, RefContext context) {
    injectSide(rowPredicate.leftNum(), rowPredicate.leftFunc(), LEFT_NUM, LEFT_FUNC, jObj, context);
    jObj.put(OP, rowPredicate.op().symbol());
    injectSide(rowPredicate.rightNum(), rowPredicate.rightFunc(), RIGHT_NUM, RIGHT_FUNC, jObj, context);
    return jObj;
  }
  
  
  private void injectSide(
      Optional<Number> num, Optional<Supplied> func,
      String numTag, String funcTag,
      JSONObject jObj, RefContext context) {
    if (num.isPresent()) {
      ArgParsers.putNumber(numTag, num.get(), jObj);
    } else {
      var jFunc = SourceRowNumFuncParser.INSTANCE.toJsonObject(func.get(), context);
      jObj.put(funcTag, jFunc);
    }
  }


}





















