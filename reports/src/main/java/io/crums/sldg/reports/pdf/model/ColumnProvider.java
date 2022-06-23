/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model;


import java.util.Objects;

import io.crums.sldg.reports.pdf.CellData;
import io.crums.sldg.reports.pdf.CellFormat;
import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.ColumnValue;

/**
 * 
 */
public abstract class ColumnProvider implements CellDataProvider<ColumnValue> {
  
  
  private final ColumnType type;
  

  
  ColumnProvider(ColumnType type) {
    this.type = Objects.requireNonNull(type, "null type");
  }
  

  @Override
  public CellData getCellData(ColumnValue value, CellFormat cellFormat) {
    if (value.getType() != type)
      throw new IllegalArgumentException(
          "unmatched type: expected " + type + "; actual " + value.getType());
    
    return getCellDataImpl(value, cellFormat);
  }
  
  
  abstract CellData getCellDataImpl(ColumnValue value, CellFormat cellFormat);
  
  
  
//  /** @param ignore disambiguation hack  */
//  private ColumnProvider(ColumnType type, CellDataProvider<?> primitiveProvider, boolean ignore) {
//    this.type = Objects.requireNonNull(type, "null type");
//    this.primitiveProvider = Objects.requireNonNull(primitiveProvider, "null cell data provider");
//  }
  
  


  
  
  public static class NumberColumnProvider extends ColumnProvider {
    
    private NumberProvider numProvider;
    
    
    
    public NumberColumnProvider(ColumnType type, NumberProvider numProvider) {
      super(type);
      this.numProvider = Objects.requireNonNull(numProvider, "null number provider");
      
      if (type != ColumnType.LONG && type != ColumnType.DOUBLE)
        throw new IllegalArgumentException("illegal type using number provider: " + type);
    }

    
    @Override
    CellData getCellDataImpl(ColumnValue value, CellFormat cellFormat) {
      
      return null;
    }
    
    
    
  }

}
