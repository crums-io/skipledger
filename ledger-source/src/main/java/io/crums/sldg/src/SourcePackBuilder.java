/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.HASH_WIDTH;
import static io.crums.sldg.src.SourcePack.*;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.TreeMap;

import io.crums.io.Serial;

/**
 * 
 */
public class SourcePackBuilder implements Serial {
  
  private final static int HEADER_SIZE_EST = 32;
  
  private final TreeMap<Long, SourceRow> rows = new TreeMap<>();
  private final SaltScheme saltScheme;
  
  private boolean fixedCellCount;
  private int maxCellCount;
  private int maxVarSize;
  private int sizeEstimate = HEADER_SIZE_EST;
  
  
  public SourcePackBuilder(SaltScheme saltScheme) {
    this.saltScheme = Objects.requireNonNull(saltScheme);
  }
  
  
  public SaltScheme saltScheme() {
    return saltScheme;
  }
  
  
  
  
  
  public boolean add(SourceRow row) {
    
    var cells = row.cells();
    var types = row.cellTypes();
    final int count = cells.size();
    
    if (types.size() != count)
      throw new IllegalArgumentException(
          "cells/types size mismatch -- implementation class: " +
          row.getClass().getName());
    
    int maxDataSize = 0;        // really, max var data size
    
    final boolean hasRowSalt = row.rowSalt().isPresent();
    sizeEstimate +=             // 12 = row no. 8 + row-status 1 + cell-count 3
        hasRowSalt ? 32 + 12 : 12;
    
    for (int index = 0; index < count; ++index) {
      
      
      Cell cell = cells.get(index);
      if (cell.isRedacted()) {
        if (hasRowSalt)
          throw new IllegalArgumentException(
              "row [%d] has both row-salt and redacted cell [%d] -- %s"
              .formatted(row.no(), index, row.getClass().getName()));
        sizeEstimate += 1 + HASH_WIDTH;
        continue;
      }
      if (cell.hasSalt() != saltScheme.isSalted(index))
        throw new IllegalArgumentException(
            "cell [%d] must be %s: %s"
            .formatted(index, cell.hasSalt() ? "unsalted" : "salted", row));
      if (!hasRowSalt && cell.hasSalt())
        sizeEstimate += HASH_WIDTH;
      
      int dataSize = cell.dataSize();
      if (dataSize > maxDataSize)
        maxDataSize = dataSize;
      
      sizeEstimate += dataSize;
      
      var type = types.get(index);
      if (type.isFixedSize()) {
        if (dataSize != type.size())
          throw new IllegalArgumentException(
              "cell data-size (%d) / type (%s) mismatch at index %d -- %s"
              .formatted(dataSize, type, index, row.getClass().getName()));
      } else
        sizeEstimate += 3;      // max cell size is 0xff_ff_ff
      
    }
    
    var existing = rows.put(row.no(), row);
    if (existing != null) {
      rows.put(existing.no(), existing);
      return false;
    }
    
    // added. Update the stats we keep track of..
    
    if (count > maxCellCount) {
      fixedCellCount = maxCellCount == 0 && count <= 0xff_ff;
      maxCellCount = count;
    }
    if (maxDataSize > maxVarSize)
      maxVarSize = maxDataSize;
    
    return true;
  }
  
  
  public SourcePack build() {
    
    if (rows.isEmpty())
      throw new IllegalStateException("instance is empty");
    
    var buffer = ByteBuffer.allocate(estimateSize());
    writeTo(buffer).flip();
    
    
    return SourcePack.load(buffer);
  }

  
  

  
  @Override
  public int estimateSize() {
    return sizeEstimate;
  }


  /**
   * Expensive operation.
   * 
   * @see #estimateSize()
   */
  @Override
  public int serialSize() {
    
    var buffer = ByteBuffer.allocate(sizeEstimate);
    return writeTo(buffer).position();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out)
      throws BufferOverflowException, IllegalStateException {
    
    if (rows.isEmpty())
      throw new IllegalStateException("empty instances cannot be serialized");
    
//    final var debug = System.out;
//    debug.println();
//    debug.println("class:          " + getClass().getName());
//    debug.println("maxCellCount:   " + maxCellCount);
//    debug.println("fixedCellCount: " + fixedCellCount);
//    debug.println("maxVarSize:     " + maxVarSize);
//    debug.println("sizeEstimeate:  " + sizeEstimate);
//    debug.println("==============");
    
    var ccWriter = fixedCellCount ?
        new CellCountCodec(maxCellCount) :
            CellCountCodec.forMaxCount(maxCellCount);
    
    var varSizeWriter = VarSizeCodec.forMaxSize(maxVarSize);
    
    long schemaFlag = 0L;
    if (fixedCellCount)
      schemaFlag |= SchemaFlag.ISO_COUNT;
    
    if (saltScheme.cellIndices().length == 0) {
      
      if (!saltScheme.isPositive())
        schemaFlag |= SchemaFlag.SALTED_ALL;
    
    } else {
      
      if (saltScheme.isPositive())
        schemaFlag |= SchemaFlag.SALTED_IDX;
      else
        schemaFlag |= SchemaFlag.UNSALTED_IDX;
    }
    
    out.putLong(schemaFlag);
    
    // write the cell [un]salt indices, if any
    {
      final int count = saltScheme.cellIndices().length;
      if (count > 0) {
        assert count <= 0xff_ff;
        out.putShort((short) count);
        int[] si = saltScheme.cellIndices();
        int prev = -1;
        for (int i = 0; i < count; ++i) {
          int next = si[i];
          assert next <= 0xff_ff;
          assert next > prev;
          prev = next;
          out.putShort((short) next);
        }
      }
    }
    
    if (ccWriter.isFixed())
      out.put((byte) ccWriter.fixed);
    else
      out.put((byte) ccWriter.countSize);
    
    out.put(varSizeWriter.width());
    
    
    final int rowCount = rows.size();
    out.putInt(rowCount);

    
    for (var row : rows.values()) {
      
      out.putLong(row.no());
      var cells = row.cells();
      final int cellCount = cells.size();
      ccWriter.putCount(cellCount, out);
      
      
      
      final boolean hasRowSalt;
      {
        var rowSalt = row.rowSalt();
        hasRowSalt = rowSalt.isPresent();
        out.put((byte) (hasRowSalt ? RowFlag.HAS_ROW_SALT : 0));
        if (hasRowSalt)
          out.put(rowSalt.get());
      }
      
      var types = row.cellTypes();
      
      for (int index = 0; index < cellCount; ++index) {
        
        Cell cell = cells.get(index);
        if (cell.isRedacted()) {
          out.put((byte) 0).put(cell.hash());
          continue;
        }
        
        var type = types.get(index);
        out.put(CellFlag.encodeUnredacted(type));
        
        assert cell.hasSalt() == saltScheme.isSalted(index);
        if (!hasRowSalt && cell.hasSalt())
          out.put(cell.salt());
        
        if (type.isNull())
          continue;
        
        if (type.isVarSize())
          varSizeWriter.putSize(cell.dataSize(), out);
        
        out.put(cell.data());
        
      } // for cell
      
    } // for row
      
    return out;
  }
  
  
  

}






















