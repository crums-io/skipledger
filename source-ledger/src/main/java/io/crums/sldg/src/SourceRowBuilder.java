/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;

import io.crums.sldg.salt.TableSalt;
import io.crums.util.Lists;
import io.crums.util.SuppliedValue;

/**
 * Builder for {@linkplain SourceRow}s.
 * 
 * <h2>Permitted Value Types</h2>
 * <p>
 * These are the few pre-defined value types.
 * </p>
 * <ul>
 * <li>{@linkplain DataType#STRING STRING}: {@code java.lang.String}</li>
 * <li>{@linkplain DataType#LONG LONG}: {@code long}</li>
 * <li>{@linkplain DataType#DATE DATE}: a LONG value encoding UTC millis ({@code long} )</li>
 * <li>{@linkplain DataType#BYTES BYTES}: {@code byte[]} or {@code java.nio.ByteBuffer}</li>
 * <li>{@linkplain DataType#HASH HASH}: BYTES of length 32 ({@code byte[]} or {@code java.nio.ByteBuffer})</li>
 * <li>{@linkplain DataType#NULL NULL}: {@code null} reference</li>
 * </ul>
 */
public class SourceRowBuilder {
  
  private final SaltScheme saltScheme;
  private final TableSalt shaker;
  
  
  
  /**
   * No salt constructor.
   */
  public SourceRowBuilder() {
    this(SaltScheme.NO_SALT, null);
  }
  
  
  /**
   * Full constructor.
   * 
   * @param saltScheme not {@code null}
   * @param tableSalt not {@code null} if {@linkplain SaltScheme#hasSalt() saltScheme.hasSalt()}
   */
  public SourceRowBuilder(SaltScheme saltScheme, TableSalt tableSalt) {
    this.saltScheme = saltScheme;
    this.shaker = tableSalt;
    if (saltScheme.hasSalt() && shaker == null)
      throw new IllegalArgumentException(
          "saltScheme requires non-null TableSalt");
  }
  
  
  /**
   * Returns the saltScheme.
   */
  public SaltScheme saltScheme() {
    return saltScheme;
  }
  
  

  /**
   * Builds and returns a row using the given cell values. Cell
   * {@linkplain DataType data types} are inferred from cell {@code values}.
   * 
   * <h4>Supported {@code value} types</h4>
   * <p>
   * Various Java types map to individual {@linkplain DataType}s. However,
   * this method does not infer 32-byte sequences as {@linkplain DataType#HASH};
   * this method never generates that data type -- use
   * {@linkplain #buildRow(long, List, List)} to specify a cell of
   * {@code DataType.HASH}.
   * </p>
   * <ol>
   * <li>{@code java.lang.CharSequence} maps to {@linkplain DataType#STRING}. </li>
   * <li>Fixed precision numeric primitives map to 8-byte {@linkplain DataType#LONG}s.
   *   However, floating point types are not supported. </li>
   * <li>{@code java.util.Date} maps to {@linkplain DataType#DATE}, which is
   *   a {@code LONG} subtype encoding UTC milliseconds. </li>
   * <li>{@code java.math.BigDecimal} and {@code java.math.BigInteger} types are
   * encoded as {@linkplain DataType#BIG_DEC} and {@linkplain DataType#BIG_INT},
   * resp.</li> 
   * <li>{@code byte[]} and {@code java.nio.ByteBuffer} map to {@linkplain DataType#BYTES}. </li>
   * </ol>
   * 
   * @param no          &ge; 1
   * @param values      one or more values of the above supported types.
   * @return
   */
  public SourceRow buildRow(long no, Object... values) {
    return
        buildRowImpl(
            no,
            values,
            saltScheme.hasSalt() ? DIGEST.newDigest() : null);
  }
  
  
  /**
   * Builds and returns a row using the given cell values. Cell
   * {@linkplain DataType data types} are inferred from cell {@code values}.
   * This is the same as {@linkplain #buildRow(long, Object...)}, except
   * it allows the caller to reuse the digester.
   * 
   * @param no          &ge; 1
   * @param digest      not {@code null}, if salt scheme has salt
   * @param values      one or more values of the supported types.
   * 
   * @see #buildRow(long, Object...)
   */
  public SourceRow buildRow(long no, MessageDigest digest, Object... values) {
    return buildRowImpl(no, values, digest);
  }
  
  
  private SourceRow buildRowImpl(long no, Object[] values, MessageDigest digest) {

    if (no < 1L)
      throw new IllegalArgumentException("no: " + no);
    
    final int cc = values.length;
    assert cc > 0;
    
    Cell[] cells = new Cell[cc];
    
    // if we might need salt the cells create the row salt
    // (slightly more complicated design option commented out below)
    final var rowSalt = saltScheme.hasSalt() ?
        ByteBuffer.wrap(shaker.rowSalt(no, digest)) : null;
    
//    final var rowSalt = SuppliedValue.of(
//        () -> ByteBuffer.wrap(shaker.rowSalt(no, digest)).asReadOnlyBuffer());
    
    boolean hasSalt = false;
    
    for (int index = 0; index < cc; ++index) {
      Object value = values[index];
      final boolean salt = saltScheme.isSalted(index);
      hasSalt |= salt;
      
      if (value == null) {
        cells[index] = salt ?
            new Cell.RowSaltedNull(rowSalt, index) : Cell.UNSALTED_NULL;
        continue;
      }

      DataType type = DataType.guessType(value);
      ByteBuffer valBuf = type.toByteBuffer(value);
      
      cells[index] = salt ?
          new Cell.RowSaltedCell(rowSalt, index, type, valBuf) :
            new Cell.UnsaltedReveal(type, valBuf, false);
    }
    
    return new RevealedRow(no, cells, hasSalt ? rowSalt : null);
   
  }

  
  /**
   * 
   * @param no
   * @param types
   * @param cellValues
   * @return
   */
  public SourceRow buildRow(long no, List<DataType> types, List<?> cellValues) {
    return buildRow(no, types, cellValues, DIGEST.newDigest());
  }
  
  /**
   * Creates and returns and new {@code SourceRow} instance using the given
   * typed values. If any cells are salted (per the instance's
   * {@linkplain #saltScheme}), then the returned instance's
   * {@linkplain SourceRow#rowSalt()} will be present.
   * 
   * @param no          row number &ge; 1
   * @param types       list of data types corresponding 
   * @param cellValues  no larger in size than that of {@code types}
   * @param digest      SHA-256 work digester (reusing this is more efficient)
   * 
   * @return a revealed source row with no cells redacted
   */
  public SourceRow buildRow(
      long no, List<DataType> types, List<?> cellValues, MessageDigest digest) {
    
    if (digest.getDigestLength() != HASH_WIDTH)
      throw new IllegalArgumentException("digest: " + digest);
    
    if (no < 1L)
      throw new IllegalArgumentException("no: " + no);
    
    final int typesLen = types.size();
    if (typesLen == 0)
      throw new IllegalArgumentException("empty types list");
    
    final int cc = cellValues.size();   // cell-count
    if (cc == 0)
      throw new IllegalArgumentException("empty values list");
    
    
    if (typesLen < cc)
      throw new IllegalArgumentException(
          "types list (size %d) may not be shorter than values list (%d)"
          .formatted(typesLen, cc));
    
    
    final var rowSalt = SuppliedValue.of(
        () -> ByteBuffer.wrap(shaker.rowSalt(no, digest)).asReadOnlyBuffer());
    
    final Cell[] cells = new Cell[cc];
    
    for (int index = 0; index < cc; ++index) {
      
      final boolean isSalted = saltScheme.isSalted(index);
      
      var type = types.get(index);
      
      var value = cellValues.get(index);
      
      if (value == null) {
        cells[index] = isSalted ?
            new Cell.RowSaltedNull(rowSalt.get(), index) :
              Cell.UNSALTED_NULL;
        continue;
      }
      
      ByteBuffer rawValue = type.toByteBuffer(value);
      
      cells[index] = isSalted ?
          new Cell.RowSaltedCell(rowSalt.get(), index, type, rawValue) :
            new Cell.UnsaltedReveal(type, rawValue);
      
    }
    
    
    
    var rowSaltOpt = rowSalt.peek();
    
    return new SourceRow() {
      
      @Override
      public long no() {
        return no;
      }
      
      @Override
      public List<Cell> cells() {
        return Lists.asReadOnlyList(cells);
      }
      
      @Override
      public Optional<ByteBuffer> rowSalt() {
        return rowSaltOpt.map(ByteBuffer::asReadOnlyBuffer);
      }
      
    };
  }

  
  
  private static class RevealedRow extends SourceRow {
    
    private final long no;
    private final Cell[] cells;
    
    private final ByteBuffer rowSalt;
    

    RevealedRow(long no, Cell[] cells, ByteBuffer rowSalt) {
      this.no = no;
      this.cells = cells;
      this.rowSalt = rowSalt;
      
      assert no > 0;
      assert rowSalt == null || rowSalt.remaining() == rowSalt.capacity();
    }
    @Override
    public long no() {
      return no;
    }

    @Override
    public List<Cell> cells() {
      return Lists.asReadOnlyList(cells);
    }
    
    
    @Override
    public Optional<ByteBuffer> rowSalt() {
      return rowSalt == null ?
          Optional.empty() : Optional.of(rowSalt.asReadOnlyBuffer());
    }
    
  }

}
