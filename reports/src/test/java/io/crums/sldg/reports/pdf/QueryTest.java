/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import static io.crums.sldg.reports.pdf.ReportTemplateTest.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.reports.pdf.func.SourceRowNumFunc;
import io.crums.sldg.reports.pdf.func.SourceRowNumFunc.Column;
import io.crums.sldg.reports.pdf.input.NumberArg;
import io.crums.sldg.reports.pdf.input.Param;
import io.crums.sldg.reports.pdf.input.Query;
import io.crums.sldg.reports.pdf.pred.BoolComp;
import io.crums.sldg.reports.pdf.pred.PNode;
import io.crums.sldg.reports.pdf.pred.PNode.Op;
import io.crums.sldg.reports.pdf.pred.SourceRowPredicate;
import io.crums.util.Lists;

/**
 * 
 */
public class QueryTest {
  
  
  @Test
  public void testRowNumberEquals() {
    final int RN = 261;
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(Column.ROW_NUM), null);
    var param = new Param<Number>("row-num");
    var rowPred = new SourceRowPredicate(rowFunc, BoolComp.EQ, new NumberArg(param));
    var query = new Query(rowPred);
    assertEquals(1, query.getNumberArgs().size());
    query.getNumberArgs().get(0).set(261);
    var srcRows = ReportTemplateTest.getChinookMorsel().sources();
    var resultSet = srcRows.stream().filter(query).toList();
    assertEquals(1, resultSet.size());
    assertEquals(RN, resultSet.get(0).rowNumber());
  }
  
  @Test
  public void testRowNumberRange() {
    final int LO_RN = 260;
    final int HI_RN = 263;
    var rowFunc = new SourceRowNumFunc.Supplied(List.of(Column.ROW_NUM), null);
    var gte = new Param<Number>("row-num >=");
    var lte = new Param<Number>("row-num <=");
    var gtePred = new SourceRowPredicate(rowFunc, BoolComp.GTE, new NumberArg(gte));
    var ltePred = new SourceRowPredicate(rowFunc, BoolComp.LTE, new NumberArg(lte));
    var query = new Query(PNode.branchLeaves(List.of(gtePred, ltePred), Op.AND));
    assertEquals(2, query.getNumberArgs().size());
    assertEquals(gte, query.getNumberArgs().get(0).param());
    assertEquals(lte, query.getNumberArgs().get(1).param());
    query.getNumberArgs().get(0).set(LO_RN);
    query.getNumberArgs().get(1).set(HI_RN);
    
    var srcRows = getChinookMorsel().sources();
    var resultSet = srcRows.stream().filter(query).toList();
    assertEquals(HI_RN - LO_RN + 1, resultSet.size());
    for (int index = 0; index < resultSet.size(); ++index)
      assertEquals(LO_RN + index, resultSet.get(index).rowNumber());
  }
  
  
  @Test
  public void testColumnValuePredicate() {
//    final int invoiceId = 47;
    final int minTrackId = 1505;
    final int maxTrackId = 1562;
    
//    final int invoiceIdColIndex = 1;
    final int trackIdColIndex = 2;
    
    var invoiceFunc = new SourceRowNumFunc.Supplied(
        List.of(new Column(INVOICE_ID_COL_INDEX)),
        null /* column value not manipulated */);
    
    var trackIdFunc = new SourceRowNumFunc.Supplied(
        List.of(new Column(trackIdColIndex)),
        null /* column value not manipulated */);
    
    var invoiceParam = new Param<Number>("invoice-id");
    var minParam = new Param<Number>("min-track-id-exc", "Min track ID (exc)");
    var maxParam = new Param<Number>("max-track-id-inc", "Max track ID (inc)");
    
    var minTrackPred = new SourceRowPredicate(
        trackIdFunc, BoolComp.GT, new NumberArg(minParam));
    var maxTrackPred = new SourceRowPredicate(
        trackIdFunc, BoolComp.LTE, new NumberArg(maxParam));
    var invoicePred = new SourceRowPredicate(
        invoiceFunc, BoolComp.EQ, new NumberArg(invoiceParam));
    
    var predTree = PNode.branchLeaves(
        List.of(minTrackPred, maxTrackPred, invoicePred), Op.AND);
    
    
    var query = new Query(predTree);

    assertEquals(3, query.getNumberArgs().size());
    assertEquals(List.of(minParam, maxParam, invoiceParam), Lists.map(query.getNumberArgs(), NumberArg::param));
    
    query.getNumberArgs().get(0).set(minTrackId - 1);
    query.getNumberArgs().get(1).set(maxTrackId);
    query.getNumberArgs().get(2).set(INVOICE_47);
    

    var srcRows = getChinookMorsel().sources();
    var resultSet = srcRows.stream().filter(query).toList();
    
    // specific to *this morsel
    assertEquals(6, resultSet.size());
    
    for (var row : resultSet) {
      long trackId = (Long) row.getColumns().get(trackIdColIndex).getValue();
      assertTrue(trackId >= minTrackId);
      assertTrue(trackId <= maxTrackId);
      assertEquals(INVOICE_47, ((Long) row.getColumns().get(INVOICE_ID_COL_INDEX).getValue()).intValue());
    }
  }
  

}










