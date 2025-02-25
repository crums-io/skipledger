/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SourcePack.SaltSchemeR;
import io.crums.util.Lists;
import io.crums.util.Strings;
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
  
  
  
  public SourceRowBuilder() {
    this(SaltSchemeR.NO_SALT, null);
  }
  
  
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
   * <li>{@code java.lang.String} maps to {@linkplain DataType#STRING}. </li>
   * <li>{@code java.lang.Number} maps to an 8-byte {@linkplain DataType#LONG}.
   *   However, floating point types are not supported. </li>
   * <li>{@code java.util.Date} maps to {@linkplain DataType#DATE}, which is
   *   a {@code LONG} subtype encoding UTC milliseconds. </li>
   * <li>{@code byte[]} maps to {@linkplain DataType#BYTES}. </li>
   * <li>{@code java.nio.ByteBuffer} maps to {@linkplain DataType#BYTES}. </li>
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
    DataType[] types = new DataType[cc];
    
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
        types[index] = DataType.NULL;
        cells[index] = salt ?
            new Cell.RowSaltedNull(rowSalt, index) : Cell.UNSALTED_NULL;
        continue;
      }

      ByteBuffer valBuf;
      if (value instanceof String) {
        types[index] = DataType.STRING;
        valBuf = Strings.utf8Buffer(value.toString());
      } else if (value instanceof Long n) {
        types[index] = DataType.LONG;
        valBuf = toLongBuffer(n);
      } else if (value instanceof Integer n) {
        types[index] = DataType.LONG;
        valBuf = toLongBuffer(n.longValue());
      } else if (value instanceof Number n) {
        if (n instanceof Double || n instanceof Float)
          throw new IllegalArgumentException(
              "floating point numbers not supported in ledgers: " + n);
        types[index] = DataType.LONG;
        valBuf = toLongBuffer(n.longValue());
      } else if (value instanceof java.util.Date d) {
        types[index] = DataType.DATE;
        valBuf = toLongBuffer(d.getTime());
      } else if (value instanceof Boolean bool) {
        types[index] = DataType.BOOL;
        byte val = bool.booleanValue() ? (byte) 1 : 0;
        valBuf = ByteBuffer.wrap(new byte[] { val } );
      } else if (value instanceof byte[] b) {
        types[index] = DataType.BYTES;
        valBuf = ByteBuffer.wrap(b);
      } else if (value instanceof ByteBuffer b) {
        types[index] = DataType.BYTES;
        valBuf = b.slice();
      } else {
        throw new IllegalArgumentException(
            "unrecognized value type (class: %s) at index [%d]: %s"
            .formatted(value.getClass().getName(), index, Arrays.asList(values)));
      }
      
      cells[index] = salt ?
          new Cell.RowSaltedCell(rowSalt, index, valBuf) :
            new Cell.UnsaltedReveal(valBuf, false);
    }
    
    return new RevealedRow(no, types, cells, hasSalt ? rowSalt : null);
   
  }
  
  
  private ByteBuffer toLongBuffer(long n) {
    return ByteBuffer.allocate(8).putLong(n).flip();
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
    
    Cell[] cells = new Cell[cc];
    
    DataType[] cellTypes = new DataType[cc];
    
    for (int index = 0; index < cc; ++index) {
      
      final boolean isSalted = saltScheme.isSalted(index);
      
      var type = types.get(index);
      
      var value = cellValues.get(index);
      
      if (value == null) {
        cellTypes[index] = DataType.NULL;
        cells[index] = isSalted ?
            new Cell.RowSaltedNull(rowSalt.get(), index) :
              Cell.UNSALTED_NULL;
        continue;
      }
      
      cellTypes[index] = type;
      
      ByteBuffer rawValue = toRawValue(value, type, index);
      assert rawValue != null;
      
      if (type.isFixedSize() && rawValue.remaining() != type.size())
        throw new IllegalArgumentException(
            "fixed size cell type (%) / data-size mismatch (expected %d; actual %d) index %d"
            .formatted(type.name(), type.size(), rawValue.remaining(), index));
      
      cells[index] = isSalted ?
          new Cell.RowSaltedCell(rowSalt.get(), index, rawValue) :
            new Cell.UnsaltedReveal(rawValue);
      
    }
    
    var rowSaltOpt = rowSalt.peek().map(ByteBuffer::asReadOnlyBuffer);
    
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
      public List<DataType> cellTypes() {
        return Lists.asReadOnlyList(types);
      }
      
      @Override
      public Optional<ByteBuffer> rowSalt() {
        return rowSaltOpt.map(ByteBuffer::asReadOnlyBuffer);
      }
      
    };
  }
  
  
  private ByteBuffer toRawValue(Object value, DataType type, int index) {
    return switch (type) {
    case STRING       -> Strings.utf8Buffer((String) value);
    case LONG         -> ByteBuffer.allocate(8).putLong(
                            ((Number) value).longValue()).flip();
    case DATE         -> ByteBuffer.allocate(8).putLong(
        value instanceof Date date ? date.getTime() : (Long) value).flip();
    case BOOL         -> ByteBuffer.wrap(
        new byte[] { ((Boolean) value).booleanValue() ? 0 : (byte) 1 });
    case BYTES        -> fromByteSequence(value, type, index);
    case HASH         -> fromByteSequence(value, type, index);
    case NULL         -> assertNullValue(value, index);
    };
  }
  
  
  private ByteBuffer assertNullValue(Object value, int index) {
    if (value != null)
      throw new IllegalArgumentException(
          "NULL type with non-null value %s at index %d"
          .formatted(value, index));
    return null;
  }
  
  private ByteBuffer fromByteSequence(Object value, DataType hashOrBytes, int index) {
    if (value instanceof ByteBuffer buf)
      return buf.slice();
    else if (value instanceof byte[] array)
      return ByteBuffer.wrap(array);
    else
      throw new IllegalArgumentException(
          "value class %s (%s) with data type %s for cell [%d]"
          .formatted(
              value.getClass().getName(), value, hashOrBytes.name(), index));
          
  }
  
  
  private static class RevealedRow extends SourceRow {
    
    private final long no;
    private final Cell[] cells;
    private final DataType[] types;
    
    private final ByteBuffer rowSalt;
    

    RevealedRow(long no, DataType[] types, Cell[] cells, ByteBuffer rowSalt) {
      this.no = no;
      this.types = types;
      this.cells = cells;
      this.rowSalt = rowSalt;
      
      assert no > 0;
      assert types.length == cells.length;
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
    public List<DataType> cellTypes() {
      return Lists.asReadOnlyList(types);
    }
    
    
    @Override
    public Optional<ByteBuffer> rowSalt() {
      return rowSalt == null ?
          Optional.empty() : Optional.of(rowSalt.asReadOnlyBuffer());
    }
    
  }

}
