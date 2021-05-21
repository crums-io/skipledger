/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.SldgConstants.DIGEST;
import static io.crums.util.hash.Digest.bufferDigest;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.src.ColumnValue.BytesValue;
import io.crums.sldg.src.ColumnValue.NumberValue;
import io.crums.sldg.src.ColumnValue.StringValue;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * A simple model for the source data that makes up the <em>input-hash</em> for a
 * skip ledger row.
 */
public class SourceRow implements Serial {
  
  

  
  
  public static SourceRow load(ByteBuffer in) {
    final long rowNumber = in.getLong();
    final int count = 0xffff & in.getShort();
    ColumnValue[] columns = new ColumnValue[count];
    for (int index = 0; index < count; ++index)
      columns[index] = ColumnValue.loadValue(in);
    return new SourceRow(rowNumber, columns);
  }
  
  
  
  
  
  private final long rowNumber;
  private final List<ColumnValue> columns;
  
  
  public SourceRow(long rowNumber, Object... colValues) {
    this(
        rowNumber,
        Lists.map(Arrays.asList(colValues), o -> ColumnValue.newInstance(o)));
  }
  
  public SourceRow(long rowNumber, ColumnValue... colValues) {
    this(rowNumber, Arrays.asList(colValues));
  }

  /**
   * 
   */
  public SourceRow(long rowNumber, List<ColumnValue> colValues) {
    this.columns = Lists.readOnlyCopy(colValues);
    this.rowNumber = rowNumber;
    if (columns.isEmpty())
      throw new IllegalArgumentException("empty column values");
    if (columns.size() > 0xffff)
      throw new IllegalArgumentException(
          "number of columns exceeds 0xffff: " + columns.size());
    SkipLedger.checkRealRowNumber(rowNumber);
    int nullCount = 0;
    for (int index = columns.size(); index-- > 0;) {
      if (columns.get(index).getType() == ColumnType.NULL)
        ++nullCount;
    }
    if (nullCount == columns.size())
      throw new IllegalArgumentException("row consists of all nulls: " + columns);
  }
  
  
  private SourceRow(long rowNumber, List<ColumnValue> columns, boolean ignored) {
    this.rowNumber = rowNumber;
    this.columns = columns;
  }
  
  /**
   * Returns the row's row-number. The row number does not figure in the
   * computation of the row's hash (altho, it's something to consider).
   */
  public final long rowNumber() {
    return rowNumber;
  }
  
  /**
   * Returns a read-only list of column values. The order is significant
   * since it governs the row's {@linkplain #rowHash() hash} value.
   */
  public final List<ColumnValue> getColumns() {
    return columns;
  }
  
  
  /**
   * Returns a redacted version of this instance with the given column replaced
   * by its hash. However, if the given column's type is either
   * {@linkplain ColumnType#HASH} or {@linkplain ColumnType#NULL}, then the column
   * is considered to be already redacted, and so, this instance is returned.
   * 
   * @param col 1-based column index (in keeping w/ SQL conventions)
   * 
   * @return a copy of this class, with the specified {@code col} redacted;
   *         {@code this} instance, if the column is already redacted
   */
  public SourceRow redactColumn(int col) {
    if (col < 1)
      throw new IllegalArgumentException("col" + col);
    if (col > columns.size())
      throw new IllegalArgumentException("attempt to redact non-existent col " + col + ": " + this);
    
    int index = col - 1;
    switch (columns.get(index).getType()) {
    case HASH:
    case NULL:
      return this;
    default:
    }
    ColumnValue[] values = columns.toArray(new ColumnValue[columns.size()]);
    values[index] = new ColumnValue.HashValue(values[index].getHash());
    return new SourceRow(rowNumber, Lists.asReadOnlyList(values), false);
  }
  
  
  
  public boolean isRedacted() {
    int i = columns.size();
    while (i-- > 0 && columns.get(i).getType() != ColumnType.HASH);
    return i != -1;
  }
  
  
  public ByteBuffer rowHash() {
    MessageDigest digest = DIGEST.newDigest();
    MessageDigest work = DIGEST.newDigest();
    
    for (var col : columns)
      digest.update(col.getHash(work));
    
    return bufferDigest(digest);
  }

  /**
   * <p>Expensive.</p>
   * {@inheritDoc}
   */
  @Override
  public int serialSize() {
    int size = 10;
    for (int index = columns.size(); index-- > 0; )
      size += columns.get(index).serialSize();
    return size;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.putLong(rowNumber);
    final int count = columns.size();
    out.putShort((short) count);
    for (int index = 0; index < count; ++index)
      columns.get(index).writeTo(out);
    return out;
  }
  
  
  
  /**
   * Equality semantics by {@linkplain #rowNumber()} and {@linkplain #rowHash()}.
   */
  public final boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof SourceRow))
      return false;
    
    SourceRow other = (SourceRow) o;
    return rowNumber == other.rowNumber && other.rowHash().equals(rowHash());
  }
  
  
  /**
   * <p>By {@linkplain #rowNumber()} only. Consistent with {@linkplain #equals(Object)}.
   * </p>
   * {@inheritDoc}
   */
  public final int hashCode() {
    return Long.hashCode(rowNumber);
  }
  
  
  
  
  
  
  public void writeSource(WritableByteChannel ch, byte[] colSeparator, byte[] rowSep) throws IOException {
    
    final int colCount = columns.size();
    
    writeColumn(ch, columns.get(0));
    
    if (colCount > 1) {
      ByteBuffer colSep =
          colSeparator == null || colSeparator.length == 0 ?
              BufferUtils.NULL_BUFFER : ByteBuffer.wrap(colSeparator);
      
      for (int index = 1; index < colCount; ++index) {
        ChannelUtils.writeRemaining(ch, colSep.clear());
        writeColumn(ch, columns.get(index));
      }
    }
    
    
    if (rowSep != null && rowSep.length > 0)
      ChannelUtils.writeRemaining(ch, ByteBuffer.wrap(rowSep));
  }
  
  
  private void writeColumn(WritableByteChannel ch, ColumnValue col) throws IOException {
    switch (col.getType()) {
    case NULL:
      break;
    case BYTES:
    case HASH:
      ChannelUtils.writeRemaining(ch, ((BytesValue) col).getBytes());
      break;
    case NUMBER:
      {
        ByteBuffer buf = ByteBuffer.wrap(new byte[8]).putLong(0, ((NumberValue) col).getNumber());
        ChannelUtils.writeRemaining(ch, buf);
      }
      break;
    case STRING:
      {
        byte[] utf8 = ((StringValue) col).getString().getBytes(Strings.UTF_8);
        ChannelUtils.writeRemaining(ch, ByteBuffer.wrap(utf8));
      }
      break;
      
    default:
      throw new RuntimeException("unaccounted type: " + col.getType());
    }
  }
  
  
  
  
  public void writeSource(Writer writer, String colSeparator, String rowSep) throws IOException {
    
    final int colCount = columns.size();
    
    writeColumn(writer, columns.get(0));
    
    if (colCount > 1) {
      if (colSeparator == null)
        colSeparator = "";
      
      for (int index = 1; index < colCount; ++index) {
        writer.write(colSeparator);
        writeColumn(writer, columns.get(index));
      }
    }
    
    if (rowSep != null)
      writer.write(rowSep);
  }
  
  
  
  
  private void writeColumn(Writer writer, ColumnValue col) throws IOException {
    switch (col.getType()) {
    case NULL:
      return;
    case BYTES:
    case HASH:
      writer.write(IntegralStrings.toHex(((BytesValue) col).getBytes()));
      break;
    case NUMBER:
      writer.write(String.valueOf(((NumberValue) col).getNumber()));
      break;
    case STRING:
      writer.write(((StringValue) col).getString());
      break;
    default:
      throw new RuntimeException("unaccounted type: " + col.getType());
    }
    
  }
  
  
  /**
   * 
   * Returns a string representation of the object. This can be quite big. (It doesn't
   * trim the columns if they're big.) Use sparingly.
   * 
   * @return {@code toString(", ")}
   * 
   * @see #safeDebugString()
   */
  public String toString() {
    return toString(", ");
  }
  
  
  /**
   * Returns a string representation of the object. This can be quite big. (It doesn't
   * trim the columns if they're big.) Use sparingly.
   * 
   * @param sep the column separator
   */
  public String toString(String sep) {
    if (sep == null)
      sep = "";
    try {
      StringWriter sw = new StringWriter(64);
      writeSource(sw, sep, null);
      return sw.toString();
    } catch (IOException impossible) {
      throw new RuntimeException("assertion failed: " + impossible, impossible);
    }
  }
  
  
  public String safeDebugString() {
    int binarySize = serialSize();
    if (binarySize <= 0xffff)
      return toString();
    int maxColumns = Math.min(1024, columns.size());
    StringBuilder string = new StringBuilder("[rn: ").append(rowNumber).append(", ");
    for (int index = 0; index < maxColumns; ++index) {
      var columnVal = columns.get(index);
      var type = columnVal.getType();
      string.append(", ").append(type.symbol());
      switch (type) {
      case BYTES:
        string.append('[').append(((BytesValue) columnVal).getBytes().remaining()).append(']');
        break;
      case STRING:
        string.append('[').append(((StringValue) columnVal).getString().length()).append(']');
        break;
      case HASH:
        ByteBuffer hash = ((BytesValue) columnVal).getBytes();
        string.append('[');
        IntegralStrings.appendHex(hash.limit(3), string).append("..]");
        break;
      case NUMBER:
        string.append('[').append(((NumberValue) columnVal).getNumber()).append(']');
        break;
      case NULL:
      }
    }
    return string.toString();
  }

}











