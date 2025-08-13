/*
 * Copyright 2021-2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.HASH_WIDTH;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.util.BigShort;
import io.crums.util.Lists;

/**
 * Supports mixed type, jagged ledgers. This is an all-sizes-fit-all
 * approach with not too much overhead.
 * 
 * <h2>Serial Format</h2>
 * <p>
 * Rows are modeled as a list of cells (see {@linkplain SourceRow}). Each cell 
 * incurs at least one byte of overhead--needed to distinguish redacted cells
 * from revealed ones. This <em>cell-code</em> is also used to encode the
 * the cell's {@linkplain DataType data type}. (Note the corner-case type
 * {@linkplain DataType#NULL NULL} is meant to represent SQL NULL, but does
 * not distinguish which type has been set to null. Its value is a zero byte.)
 * </p>
 * <p>See also: SourcePackBinaryFormat.txt (in sub-module top directory).
 */
public class SourcePack implements SourceBag, Serial {
  
  public final static int MAX_CELLS_PER_ROW = BigShort.MAX_VALUE;
  
  public final static int MAX_CELL_DATA_SIZE = BigShort.MAX_VALUE;
  
  public final static class SchemaFlag {
    private SchemaFlag() {  } // nobody calls
    /** Mono cell count. Every row has the same no. of cells. */
    public final static long ISO_COUNT = 1L;
    /** Array of salted cell indices. */
    public final static long SALTED_IDX = 2L;
    /** Array of unsalted cell indices. */
    public final static long UNSALTED_IDX = 4L;
    /** All cells salted (no indices). */
    public final static long SALTED_ALL = SALTED_IDX + UNSALTED_IDX;
    
    /** Exclusive max value for bit-field. */
    private final static long MAX_CODE = 8L;
    
    public static boolean isoCount(long code) {
      return (code & ISO_COUNT) != 0L;
    }
    
    
    
    public static boolean saltedIndices(long code) {
      return (code & SALTED_ALL) == SALTED_IDX;
    }
    
    public static boolean unsaltedIndices(long code) {
      return (code & SALTED_ALL) == UNSALTED_IDX;
    }
    
    public static boolean saltedAll(long code) {
      return (code & SALTED_ALL) == SALTED_ALL;
    }
    
    public static boolean noSalt(long code) {
      return (code & SALTED_ALL) == 0L;
    }
    
  }
  
  /**
   * Presently, this fits in a byte.
   */
  static class RowFlag {
    private RowFlag() {  }      // nobody calls

    final static int HAS_ROW_SALT = 1;
    final static int WHITESPACE_TOKENIZED = 2;
    
//    /** One or more cells in the row are redacted.  */
//    final static boolean redacted(int code) {
//      return (code & HAS_ROW_SALT) == 0;
//    }
    /** No cells in the row are redacted. */
    static boolean hasRowSalt(int code) {
      return (code & HAS_ROW_SALT) != 0;
    }
    
  }
  
  static class CellFlag {
    private CellFlag() {  }     // nobody calls
    
    static boolean redacted(int code) {
      return code == 0;
    }
    
    static boolean isNull(int code) {
      return code == DataType.NULL.ordinal() + 1;
    }
    
    static DataType decodeType(int code) {
      return DataType.forOrdinal(code - 1);
    }
    
    static byte encodeUnredacted(DataType type) {
      return (byte) (type.ordinal() + 1);
    }
    
  }
  
  
  static class CellCountCodec {
    
    
    static CellCountCodec forMaxCount(int maxCellCount) {
      if (maxCellCount <= 0)
        throw new IllegalArgumentException("maxCellCount " + maxCellCount);
      
      byte countSize;
      if (maxCellCount <= 0xff)
        countSize = 1;
      else if (maxCellCount <= 0xff_ff)
        countSize = 2;
      else if (maxCellCount <= 0xff_ff_ff)
        countSize = 3;
      else
        throw new IllegalArgumentException("maxCellCount " + maxCellCount);
      
      return new CellCountCodec(countSize);
    }
    
    final int fixed;
    final int countSize;
    
    /**
     * Creates a fixed-count instance (which basically does nothing).
     * 
     * @param fixedCount &ge; 1 and &le; 255
     */
    CellCountCodec(int fixedCount) {
      this.fixed = fixedCount;
      this.countSize = 0;
      if (fixedCount < 1)
        throw new SourcePackException(
            "illegal fixed-count declaration in header: " + fixedCount);
      if (fixed > 0xff)
        throw new IllegalArgumentException("fixed (%d) > 255".formatted(fixed));
    }
    
    CellCountCodec(byte countSize) {
      this.fixed = 0;
      this.countSize = countSize;
      if (countSize < 1 || countSize > 3)
        throw new SourcePackException(
            "illegal cell-count-size declaration in header: " + countSize);
    }
    
    
    boolean isFixed() {
      return fixed != 0;
    }
    
    
    int getCount(ByteBuffer in) {
      switch (countSize) {
      case 0:           return fixed;
      case 1:           return 0xff & in.get();
      case 2:           return 0xff_ff & in.getShort();
      case 3:           return BigShort.getBigShort(in);
      default:
        assert false;
        return -1;
      }
    }
    
    void putCount(int count, ByteBuffer out) {
      assert count >= 0;
      switch (countSize) {
      case 0:
        assert count == fixed;
        break;
      case 1:
        assert count <= 0xff;
        out.put((byte) count);
        break;
      case 2:
        assert count <= 0xff_ff;
        out.putShort((short) count);
        break;
      case 3:
        assert count <= BigShort.MAX_VALUE;
        BigShort.putBigShort(out, count);
        break;
      default:
        assert false;
      }
    }
  }
  
  
  
  record VarSizeCodec(byte width) {
    
    
    static VarSizeCodec forMaxSize(int maxSize) {
      if (maxSize < 0)
        throw new IllegalArgumentException("maxSize " + maxSize);
      byte w;
      if (maxSize <= 0xff_ff)
        w = 2;
      else if (maxSize <= 0xff_ff_ff)
        w = 3;
      else
        w = 4;
      return new VarSizeCodec(w);
    }
    
    VarSizeCodec {
      if (width < 2 || width > 4)
        throw new SourcePackException(
            "illegal var-size width declaration in header: " + width);
    }
    
    int getSize(ByteBuffer in) {
      switch (width) {
      case 2:           return 0xff_ff & in.getShort();
      case 3:           return BigShort.getBigShort(in);
      case 4:           return intSize(in);
      default:
        assert false;
        return -1;
      }
    }
    
    void putSize(int size, ByteBuffer out) {
      assert size >= 0;
      switch (width) {
      case 2:
        assert size <= 0xff_ff;
        out.putShort((short) size);
        break;
      case 3:
        assert size <= BigShort.MAX_VALUE;
        BigShort.putBigShort(out, size);
        break;
      case 4:
        out.putInt(size);
        break;
      default:
        assert false;
      }
    }
    
    private int intSize(ByteBuffer in) {
      int size = in.getInt();
      if (size < 0)
        throw new IllegalArgumentException("read negative size: " + size);
      return size;
    }
  }
  
  
  
  
  
  
  
  record SaltSchemeR(int[] cellIndices, boolean isPositive)
      implements SaltScheme {
    
//    final static SaltSchemeR SALT_ALL = new SaltSchemeR(new int[0], false);
//    
//    final static SaltSchemeR NO_SALT = saltOnlyInstance(new int[0]);
    
    
    static SaltSchemeR saltOnlyInstance(int[] indices) {
      return new SaltSchemeR(indices, true);
    }
    
    static SaltSchemeR saltAllExceptInstance(int[] indices) {
      return new SaltSchemeR(indices, false);
    }
    
    SaltSchemeR {
      Objects.requireNonNull(cellIndices);
    }
  }
  
  
  
  private record Config(
      CellCountCodec ccReader,
      VarSizeCodec varSizeReader,
      SaltScheme saltScheme) {
    
    Config {
      Objects.requireNonNull(ccReader);
      Objects.requireNonNull(varSizeReader);
      Objects.requireNonNull(saltScheme);
    }
    
  }
  
  
   
  public static SourcePack load(ByteBuffer in) throws SourcePackException {
    final int initPos = in.position();
    try {
      
      return loadImpl(in);
      
    } catch (SourcePackException spx) {
      throw spx;
    
    } catch (Exception cause) {
      int offset = in.position() - initPos;
      var error = cause.getMessage();
      if (error == null)
        error = cause.getClass().getSimpleName();
      throw new SourcePackException(
          "failed to load: %s -- %d bytes read, buffer: %s"
          .formatted(error, offset, in.toString()), cause);
    }
  }
  
  
  /**
   * Impl. note: when throwing, use "untyped" exception in order to inherit
   * buffer state in final "typed" exception.
   */
  private static SourcePack loadImpl(ByteBuffer in)
      throws BufferUnderflowException {
    
    final int initPos = in.position();
    
    ByteBuffer packBlock = in.slice();
    
    final long schemaCode = in.getLong();
    if (schemaCode >= SchemaFlag.MAX_CODE)
      throw new SourcePackException(
          "illegal schema code %d (0x%s)"
          .formatted(schemaCode, Long.toHexString(schemaCode)));
    
    
    final SaltScheme saltScheme;
    
    if (SchemaFlag.saltedAll(schemaCode)) {
      saltScheme = SaltScheme.SALT_ALL;
    } else if (SchemaFlag.noSalt(schemaCode)) {
      saltScheme = SaltScheme.NO_SALT;
    } else {
      final int count = 0xff_ff & in.getShort();
      if (count == 0)
        throw new SourcePackException(
            "array length zero on attempting to read [un]salted index array");
      
      int[] indices = new int[count];
      
      for (int i = 0, prev = -1; i < count; ++i) {
        int next = 0xff_ff & in.getShort();
        if (next <= prev)
          throw new IllegalArgumentException(
              "out-of-sequence indices: %d, %d".formatted(prev, next));
        indices[i] = next;
        prev = next;
      }
      
      saltScheme =
          SchemaFlag.saltedIndices(schemaCode) ?
              SaltSchemeR.saltOnlyInstance(indices) :
                SaltSchemeR.saltAllExceptInstance(indices);
    }


    final CellCountCodec ccReader;
    
    if (SchemaFlag.isoCount(schemaCode)) {
      ccReader = new CellCountCodec(0xff & in.get());
    } else {
      ccReader = new CellCountCodec(in.get());
    }
    
    final VarSizeCodec varSizeReader = new VarSizeCodec(in.get());
    
    
    final int rowCount = in.getInt();
    
//    var debug = System.out;
//    debug.println();
//    debug.println("class:    " + SourcePack.class.getName());
//    debug.println("rowCount: " + rowCount);
//    debug.println("==========");

    // Not sure it's a good idea to allow empty instances after all
    // that ceremony.. maybe this will aid in packaging empty layouts (?)
    // Disallowing it for now..
    //
    // if (rowCount == 0)
    //   return new SourcePack(List.of());
    
    if (rowCount <= 0)
      throw new IllegalArgumentException("row count: " + rowCount);
    
    
    // sanity check the row count before wasting or crashing memory..
    {
      int minRowSize = 8;  // row no.
      minRowSize += ccReader.countSize;
      
      if (saltScheme.saltAll())
        minRowSize += Math.min(1, ccReader.fixed) * (HASH_WIDTH + 1);
      else
        minRowSize  += Math.min(1, ccReader.fixed);
      
      
      if (in.remaining() < rowCount * minRowSize)
        throw new IllegalArgumentException(
            "insufficient remaining bytes (%d); row-count %d, min-row-size %d"
            .formatted(in.remaining(), rowCount, minRowSize));
    }
    
    
    int[] rowOffsets = new int[rowCount];
        
    long prevNo = 0L;
    
    Config config = new Config(ccReader, varSizeReader, saltScheme);
    
    for (int index = 0; index < rowCount; ++index) {
      rowOffsets[index] = in.position() - initPos;
      Row row = loadRow(in, config);
      long nextNo = row.no();
      if (nextNo <= prevNo)
        throw new IllegalStateException(
            "illegal or out-of-sequence row no.s: %d, %d"
            .formatted(prevNo, nextNo));
      prevNo = nextNo;
    }
    
    // so we know how to serialize it back
    packBlock = packBlock.limit(in.position() - initPos).slice();
    
    return new SourcePack(packBlock, rowOffsets, config);
  }
  
  
  
  /** Loads a row from the given buffer positioned at the beginning of the row. */
  private static Row loadRow(ByteBuffer in, Config config) {
    
    final long no = in.getLong();
    if (no < 1L)
      throw new IllegalArgumentException("illegal row no. " + no);
    
    // read the cell count
    final int cc = config.ccReader.getCount(in);
    if (cc == 0)
      throw new IllegalArgumentException("zero cell-count for row " + no);
    
    final int rowStatus = 0xff & in.get();
    
    Cell[] cells = new Cell[cc];
    DataType[] types = new DataType[cc];
    
      
    final boolean hasRowSalt = RowFlag.hasRowSalt(rowStatus);
    // some cells are redacted
    final ByteBuffer rowSalt = hasRowSalt ? BufferUtils.slice(in, HASH_WIDTH) : null;
    
//    final var debug = System.out;
//    debug.println();
//    debug.println("class: " + SourcePack.class.getName());
//    debug.println("rowStatus: " + Integer.toHexString(rowStatus));
//    debug.println("===========");
    
    
    for (int c = 0; c < cc; ++c) {
      
      final int cellCode = 0xff & in.get();
//      debug.println("cellCode: " + Integer.toHexString(cellCode));
      if (CellFlag.redacted(cellCode)) {
        if (hasRowSalt && config.saltScheme.isSalted(c))
          throw new IllegalStateException(
              "cell-code [%d] %s encodes redacted while row-status %s reveals row salt"
              .formatted(
                  c,
                  Integer.toHexString(cellCode),
                  Integer.toHexString(rowStatus)));
        
        cells[c] = Cell.Redacted.load(in);
        types[c] = DataType.HASH;     // just a convention
        continue;
      }
      
      final var type = CellFlag.decodeType(cellCode);
//      debug.printf("type[%d]: %s%n", c, type);
      types[c] = type;
      
      if (type.isNull()) {
        if (config.saltScheme.isSalted(c)) {
          if (hasRowSalt)
            cells[c] = new Cell.RowSaltedNull(rowSalt, c);
          else
            cells[c] = Cell.SaltedNull.load(in);
        } else
          cells[c] = Cell.UNSALTED_NULL;
        
        continue;
      }
      
      int dataSize = type.isFixedSize() ?
          type.size() : config.varSizeReader.getSize(in);
      
//      debug.println("dataSize: " + dataSize);
      
      if (config.saltScheme.isSalted(c)) {
        if (hasRowSalt) {
          cells[c] = new Cell.RowSaltedCell(
              rowSalt, c, BufferUtils.slice(in, dataSize), false);
        } else {
          cells[c] = Cell.SaltedCell.load(in, dataSize);
        }
      } else
        cells[c] = Cell.UnsaltedReveal.load(in, dataSize);
      
    }
//    debug.println("===========");
    
    return !hasRowSalt ?
        new Row(no, cells, types) :
          new Row(no, cells, types) {
            @Override public Optional<ByteBuffer> rowSalt() {
              return Optional.of(rowSalt.asReadOnlyBuffer());
            }
          };
  }
 
  
  private final Config config;
  private final ByteBuffer packBuffer;
  private final int[] rowOffsets;
  
  
  
  
  private SourcePack(ByteBuffer packBuffer, int[] rowOffsets, Config config) {
    this.packBuffer = packBuffer;
    this.rowOffsets = rowOffsets;
    this.config = config;
  }
  
  @Override
  public SaltScheme saltScheme() {
    return config.saltScheme();
  }

  @Override
  public List<? extends SourceRow> sources() {
    return Lists.functorList(
        rowOffsets.length,
        i -> loadRow(packBuffer.duplicate().position(rowOffsets[i]), config));
  }

  @Override
  public int serialSize() {
    assert packBuffer.remaining() == packBuffer.capacity();
    return packBuffer.capacity();
  } 

  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    return out.put(packBuffer.duplicate());
  }
  
  
  
  
  public static class Row extends SourceRow {

    
    private final long no;
    private final Cell[] cells;
    private final DataType[] types;
    
    Row(long no, Cell[] cells, DataType[] types) {
      this.no = no;
      this.cells = cells;
      this.types = types;
      assert no > 0 && cells.length == types.length && cells.length > 0;
    }
    

    @Override
    public final long no() {
      return no;
    }


    @Override
    public final List<DataType> cellTypes() {
      return Lists.asReadOnlyList(types);
    }


    @Override
    public final List<Cell> cells() {
      return Lists.asReadOnlyList(cells);
    }
    
  }

}










