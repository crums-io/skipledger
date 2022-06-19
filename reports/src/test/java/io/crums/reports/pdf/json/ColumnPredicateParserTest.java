/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.reports.pdf.model.Param;
import io.crums.reports.pdf.model.NumberArg;
import io.crums.reports.pdf.model.pred.CellPredicate;
import io.crums.reports.pdf.model.pred.ColumnPredicate;
import io.crums.reports.pdf.model.pred.PNode;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class ColumnPredicateParserTest extends SelfAwareTestCase
    implements ParserRoundtripTest<PNode<SourceRow, ColumnPredicate>> {
  
  private Object methodLabel;
  
  private EditableRefContext writeContext;
  
  private RefContext readContext = RefContext.EMPTY;
  
  @Test
  public void testSimple() throws Exception {
    this.methodLabel = new Object() { };
    // prepare
    PNode<ColumnValue, CellPredicate> equalsRhs = PNode.leaf(CellPredicate.equalTo(3));
    PNode<SourceRow, ColumnPredicate> colNode = PNode.leaf(new ColumnPredicate(2, equalsRhs));
    
    testRoundtrip(colNode);
  }
  
  @Test
  public void testSimpleCellPredicateTree() throws Exception {
    this.methodLabel = new Object() { };
    // prepare
    PNode<ColumnValue, CellPredicate> gt = PNode.leaf(CellPredicate.greaterThan(555));
    PNode<ColumnValue, CellPredicate> lt = PNode.leaf(CellPredicate.lessThanOrEqualTo(777));
    PNode<ColumnValue, CellPredicate> range = PNode.and(List.of(gt, lt));
    PNode<SourceRow, ColumnPredicate> colNode = PNode.leaf(new ColumnPredicate(4, range));
    
    testRoundtrip(colNode);
  }
  
  
  @Test
  public void testSimpleNumberArg() throws Exception {
    this.methodLabel = new Object() { };
    final int notEqRhs = 666;
    var name = "txnid.neq";
    var desc = "Don't bait the superstitious";
    int defaultVal = 13;
    // prepare
    final NumberArg rhs = new NumberArg(new Param<Number>(name, desc, defaultVal), notEqRhs);
    PNode<ColumnValue, CellPredicate> notEq = PNode.leaf(CellPredicate.notEqualTo(rhs));
    PNode<SourceRow, ColumnPredicate> colNode = PNode.leaf(new ColumnPredicate(2, notEq));
    
    this.readContext = RefContext.patchArgs(
        Map.of(name, new NumberArg(new Param<Number>(name, desc, defaultVal), notEqRhs)), null);
    
    testRoundtrip(colNode);
  }
  
  
  @Test
  public void testaBitOfEverything() throws Exception {
    this.methodLabel = new Object() { };
    final int notEqRhs = 666;
    var name = "txnid.neq";
    var desc = "Don't bait the superstitious";
    int defaultVal = 13;
    // prepare
    final NumberArg rhs = new NumberArg(new Param<Number>(name, desc, defaultVal), notEqRhs);

    PNode<SourceRow, ColumnPredicate> col2Node;
    {
      PNode<ColumnValue, CellPredicate> gt = PNode.leaf(CellPredicate.greaterThan(555));
      PNode<ColumnValue, CellPredicate> notEq = PNode.leaf(CellPredicate.notEqualTo(rhs));
      PNode<ColumnValue, CellPredicate> lte = PNode.leaf(CellPredicate.lessThanOrEqualTo(777));
      
      var rangeWithQuirk = PNode.and(List.of(gt, notEq, lte));
      col2Node = PNode.leaf(new ColumnPredicate(2, rangeWithQuirk));
    }
    
    PNode<SourceRow, ColumnPredicate> col5Node;
    {
      PNode<ColumnValue, CellPredicate> equalsRhs = PNode.leaf(CellPredicate.equalTo(7));
      col5Node = PNode.leaf(new ColumnPredicate(5, equalsRhs));
    }
    
    var rootPredicate = PNode.or(List.of(col2Node, col5Node));
    
    final var rhsCopy = new NumberArg(new Param<Number>(name, desc, defaultVal), notEqRhs);
    this.readContext = RefContext.patchArgs(Map.of(name, rhsCopy), null);
    
    testRoundtrip(rootPredicate);
    
    
  }
  
  
  
  
  
  
  

  

  @Override
  public JsonEntityParser<PNode<SourceRow, ColumnPredicate>> writeParser() {
    return new ColumnPredicateParser(writeContext = new EditableRefContext());
  }
  

  @Override
  public JsonEntityParser<PNode<SourceRow, ColumnPredicate>> parser() {
    return new ColumnPredicateParser(readContext);
  }
  

  @Override
  public void observeJson(JSONObject jObj, PNode<SourceRow, ColumnPredicate> expected) {
    if (methodLabel != null) {
      var out = System.out;
      out.println(" - - - " + method(methodLabel) + " - - -");
      new JsonPrinter(out).print(jObj);
      out.println();
    }
  }
  
  
  
  public void assertTripEquals(
      PNode<SourceRow, ColumnPredicate> expected, PNode<SourceRow, ColumnPredicate> actual) {
    if (expected.isLeaf())
      assertLeafEquals(expected, actual);
    else
      assertBranchEquals(expected, actual);
  }


  
  
  private void assertNumberArgCaptured(NumberArg expected) {
    var captured = writeContext.getNumberArg(expected.param().name());
    assertEquals(expected, captured);   // (redundant, but hey we're testing!)
    assertEquals(expected.param(), captured.param());
  }
  
  
  

  private void assertLeafEquals(
      PNode<SourceRow, ColumnPredicate> expected, PNode<SourceRow, ColumnPredicate> actual) {
    assertTrue(actual.isLeaf(), "actual not a leaf");
    
    var expColPred = expected.asLeaf().getPredicate();
    var actColPred = actual.asLeaf().getPredicate();
    
    assertEquals(expColPred.columnNumber(), actColPred.columnNumber());
    
    var expCellPred = expColPred.getCellPredicate();
    if (expCellPred.isLeaf())
      assertCellLeafEquals(expCellPred, actColPred.getCellPredicate());
    else
      assertCellBranchEquals(expCellPred, actColPred.getCellPredicate());
  }
  
  
  



  private void assertCellLeafEquals(
      PNode<ColumnValue,CellPredicate> expected, PNode<ColumnValue,CellPredicate> actual) {
    assertTrue(actual.isLeaf(), "actual not a leaf");
    var expCellPred = expected.asLeaf().getPredicate();
    var actCellPred = actual.asLeaf().getPredicate();
    assertEquals(expCellPred.getClass(), actCellPred.getClass());
    assertEquals(expCellPred.rhs(), actCellPred.rhs());
    if (expCellPred.rhs().isPresent()) {
      if (expCellPred.rhs().get() instanceof NumberArg expArg) {
        if (actCellPred.rhs().get() instanceof NumberArg actArg)
          assertEquals(expArg.param(), actArg.param());
        else
          fail("expected was " + expArg + "; actual was " + actCellPred.rhs().get());
      }
    }
  }
  
  

  private void assertCellBranchEquals(
      PNode<ColumnValue,CellPredicate> expected, PNode<ColumnValue,CellPredicate> actual) {
    assertBranchEqualsImpl(
        expected, actual,
        this::assertCellLeafEquals, this::assertCellBranchEquals);
  }



  private void assertBranchEquals(
      PNode<SourceRow, ColumnPredicate> expected, PNode<SourceRow, ColumnPredicate> actual) {
    assertBranchEqualsImpl(
        expected, actual,
        this::assertLeafEquals, this::assertBranchEquals);
  }
  
  
  private <T, P extends Predicate<T>> void assertBranchEqualsImpl(
      PNode<T, P> expected, PNode<T, P> actual,
      BiConsumer<PNode<T, P>, PNode<T, P>> leafValidator,
      BiConsumer<PNode<T, P>, PNode<T, P>> branchValidator) {

    assertTrue(actual.isBranch(), "actual not a branch");
    var expBranch = expected.asBranch();
    var actBranch = actual.asBranch();
    assertEquals(expBranch.op(), actBranch.op());
    var expSubs = expBranch.getChildren();
    var actSubs = actBranch.getChildren();
    assertEquals(expSubs.size(), actSubs.size());
    for (int index = 0; index < expSubs.size(); ++index) {
      var expSub = expSubs.get(index);
      var actSub = actSubs.get(index);
      if (expSub.isLeaf())
        leafValidator.accept(expSub, actSub);
      else
        branchValidator.accept(expSub, actSub);
    }
  }
  
}



