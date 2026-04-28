/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.DIGEST;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;

import io.crums.util.IntegralStrings;
import io.crums.util.Lists;

/**
 * A ledger row (at its source).
 * 
 * <h2>Design Tradeoffs</h2>
 * <p>
 * This is a fairly rich structure: cells, salt, data-types, row number, etc. so
 * holding a large number of these in memory will be taxing. Instead, we will
 * encapsulate the collection of rows as a lazily loaded list. In order for the
 * lazy-loading semantics to work with the java collections API,
 * {@linkplain #equals(Object) object equality} is also implemented
 * (since the lazily loaded instances are not cached and
 * every <em>get</em> on the list returns a new object).</p>
 */
public abstract class SourceRow {
  
  
  /**
   * Returns a <em>null</em> row with no cells. The hash of a null-row
   * is a sequence of zero-ed bytes.
   * 
   * @see #hash()
   */
  public static SourceRow nullRow(long rowNo) {
    if (rowNo <= 0L)
      throw new IllegalArgumentException("rowNo (" + rowNo + ") <= 0");
    return new SourceRow() {
      
      @Override
      public long no() {
        return rowNo;
      }
      
      @Override
      public List<Cell> cells() {
        return List.of();
      }
      
      @Override
      public List<DataType> cellTypes() {
        return List.of();
      }
      
      public ByteBuffer hash() {
        return DIGEST.sentinelHash();
      }
    };
  }

  
  /**
   * Returns the 1-based row no.
   * 
   * @return &ge; 1
   */
  public abstract long no();

  /**
   * Returns the ordered list of cells. Some cell values
   * may be redacted.
   */
  public abstract List<Cell> cells();
  
  /**
   * Returns the cell data types in {@linkplain #cells()}.
   * 
   * @return of same size as that of {@code cells()}
   */
  public List<DataType> cellTypes() {
    return Lists.map(cells(), Cell::dataType);
  }
  
  
  /**
   * Returns {@code true} if one or more cells are
   * {@linkplain Cell#isRedacted() redacted}; {@code false}, otherwise.
   */
  public boolean hasRedactions() {
    var cells = cells();
    int i = cells.size();
    for (; i-- > 0 && cells.get(i).hasData(); );
    return i != -1;
  }
  
  
  
  /**
   * Returns the row-salt. If revealed, then individual cell salts are derivable
   * from the returned value. This value is only made available if <em>all</em>
   * cell values have been revealed.
   * 
   * @return
   */
  public Optional<ByteBuffer> rowSalt() {
    return Optional.empty();
  }
  
  
  /**
   * Instances are equal if they have the same {@linkplain #no()} and
   * equal {@linkplain #cells()}.
   */
  public final boolean equals(Object o) {
    return
        o == this ||
        o instanceof SourceRow other &&
        no() == other.no() &&
        cells().equals(other.cells());
  }
  
  
  /**
   * Consistent with {@linkplain #equals(Object)}.
   * 
   * @return {@code Long.hashCode(no())}
   */
  public final int hashCode() {
    return Long.hashCode(no());
  }
  

  /**
   * Returns the hash of this row. The returned value corresponds to the
   * <em>input-hash</em> of the row under the skip ledger commitment scheme.
   * <p>
   * The base implementation returns the SHA-256 of the concatenation of the
   * {@linkplain Cell#hash() cell hashes} -- with <em>2 exceptions</em>:
   * </p>
   * <ol>
   * <li><em>Singleton</em>. If a row has only a single cell, then the hash of
   *   the cell is returned as-is.</li>
   * <li><em>Empty</em>. If there are no cells in the row (and if the ledger's
   *   business rules allow it), then the sentinel hash (zero) is returned.</li>
   * </ol>
   */
  public ByteBuffer hash() {
    
    var cells = cells();
    
    switch (cells.size()) {
    case 0:     return DIGEST.sentinelHash();
    case 1:     return cells.get(0).hash();
    default:
    }
    
    
    MessageDigest accumulator = DIGEST.newDigest();
    var workDigest = DIGEST.newDigest();
    
    for (var cell : cells)
      accumulator.update(cell.hash(workDigest));
    
    return ByteBuffer.wrap(accumulator.digest()).asReadOnlyBuffer();
  }
  
  
  
  public String toString() {
    var s = new StringBuilder().append('[').append(no()).append("]:<");
    var cells = cells();
    var types = cellTypes();
    for (int i = 0; i < cells.size(); ++i) {
      Cell cell = cells.get(i);
      if (cell.isRedacted())
        s.append('X');
      else {
        var data = cell.data();
        var type = types.get(i);
        Object value = type.toValue(data);


        switch (type) {
        case STRING:
          s.append('"').append(value).append('"'); break;
        case DATE:
          s.append("UTC");
        case LONG:
          s.append(value); break;
        case BIG_INT:
          s.append('G').append(value); break;
        case BIG_DEC:
          s.append('D').append(value); break;
        case HASH:
          s.append('H').append(IntegralStrings.toHex(data.slice().limit(3)));
          break;
        case BYTES:
          s.append("B(").append(data.remaining()).append(')'); break;
        default:
          s.append(value);
        }
      }
      s.append(',');
    }
    s.setLength(s.length() - 1);
    return s.append('>').toString();
  }
  
  
  /**
   * @return always a {@code SourceRow} instance
   */
  public SourceRow redact(int cell) throws IndexOutOfBoundsException {
    var cellsList = cells();
    if (cellsList.get(cell).isRedacted())
      return this;
    
    Cell[] cells = cellsList.toArray(new Cell[cellsList.size()]);
    cells[cell] = cells[cell].redact();
    
    
    
    
    // this would be an anonymous type, if I were sure it would compile to a _static_
    // type (w/o a pointer to this)
    return new RedactedSource(no(), cells);
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  private final static class RedactedSource extends SourceRow {
    
    private final long no;
    private final Cell[] cells;
    
    private RedactedSource(long no, Cell[] cells) {
      this.no = no;
      this.cells = cells;
      assert no > 0L && cells.length > 0;
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
    public boolean hasRedactions() {
      return true;
    }
    
  }

}












