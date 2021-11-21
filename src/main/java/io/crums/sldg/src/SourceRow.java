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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SourceLedger;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * A simple model for a source row that makes up the <em>input-hash</em> in an
 * individual row in a skip ledger.
 * 
 * <h2>Data Model</h2>
 * <p>
 * A row is a non-empty, ordered list of {@linkplain ColumnValue}s. There is no other constraint
 * on the structure of rows across a {@linkplain SourceLedger} (the rows' columns may vary in number
 * or type).
 * </p>
 * 
 * <h2>Row Hash</h2>
 * <p>
 * A source row's SHA-256 hash is computed from the concatentation of the hashes
 * of its individual column-values. This supports redacting individual column-values
 * by substituting the column-value's hash for its actual value.
 * </p>
 * <h3>Salting</h3>
 * <p>
 * Column-values are salted before hashing. Moreover, each column-value is salted uniquely
 * across the ledger.
 * The purpose of this salting is to guard against reverse-engineering a
 * value from its hash using rainbow attacks. Therefore, whenever a column-value
 * is revealed, it's unique salt must be included so that its hash can be validated.
 * </p><p>
 * Fortunately, we don't have to store these individual column-value (table-cell) salts
 * anywhere: instead, they can each be derived deterministically from a single, secret,
 * user-generated key (see {@linkplain TableSalt}).
 * </p>
 */
public class SourceRow implements Serial {
  
  

  
  /**
   * Loads the given row from memory. The remaining bytes in the given buffer must not be
   * modified once this method is called.
   *  
   * @param in
   * @return
   */
  public static SourceRow load(ByteBuffer in) throws ByteFormatException {
    final long rowNumber = in.getLong();
    if (rowNumber <= 0)
      throw new ByteFormatException("read illegal row number " + rowNumber);
    final int count = 0xffff & in.getShort();
    if (count < 1)
      throw new ByteFormatException("empty column values");
    ColumnValue[] columns = new ColumnValue[count];
    for (int index = 0; index < count; ++index)
      columns[index] = ColumnValue.loadValue(in);
    return new SourceRow(rowNumber, Lists.asReadOnlyList(columns), false);
  }
  
  
  /**
   * Creates and returns a salted instance.
   * 
   * @param shaker non-null
   * 
   * @see ColumnValue#toInstance(Object, ByteBuffer)
   */
  public static SourceRow newSaltedInstance(long rowNumber, TableSalt shaker, Object... colValues) {
    SkipLedger.checkRealRowNumber(rowNumber);
    ColumnValue[] columns = new ColumnValue[colValues.length];
    for (int index = colValues.length; index-- > 0; )
      columns[index] = ColumnValue.toInstance(colValues[index], shaker.salt(rowNumber, index + 1));
    return new SourceRow(rowNumber, Lists.asReadOnlyList(columns), false);
  }
  
  
  
  private final long rowNumber;
  private final List<ColumnValue> columns;
  
  
  /**
   * Creates an unsalted instance. This constructor will likely be deprecated in a future
   * version. It's included here for now for completeness.
   * 
   * @deprecated
   */
  @SuppressWarnings("deprecation")
  public SourceRow(long rowNumber, Object... colValues) {
    this(
        rowNumber,
        Lists.map(Arrays.asList(colValues), o -> ColumnValue.toInstance(o)));
  }
  
  public SourceRow(long rowNumber, ColumnValue... colValues) {
    this(rowNumber, Arrays.asList(colValues));
  }
  
  
  /**
   * 
   */
  public SourceRow(long rowNumber, List<ColumnValue> colValues) {
    this.rowNumber = rowNumber;
    this.columns = Lists.readOnlyCopy(colValues);
    SkipLedger.checkRealRowNumber(rowNumber);
    if (columns.isEmpty())
      throw new IllegalArgumentException("empty column values");
    if (columns.size() > Short.MAX_VALUE)
      throw new IllegalArgumentException(
          "number of columns exceeds model capacity (" + Short.MAX_VALUE + "): " + columns.size());
  }
  
  
  /**
   * Used by pseudo-constructors.
   */
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
   * {@linkplain ColumnType#HASH}, then the column
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
    if (columns.get(index).getType().isHash())
      return this;
    
    ColumnValue[] values = columns.toArray(new ColumnValue[columns.size()]);
    values[index] = new HashValue(values[index].getHash());
    return new SourceRow(rowNumber, Lists.asReadOnlyList(values), false);
  }
  
  
  /**
   * Returns a redacted version of this instance with the given columns replaced
   * by theirs hashes. The only restriction on the input parameter is that it be
   * well-formed.
   * 
   * @param cols non-null, possibly empty bag of 1-based column numbers in
   *             striclty increasing order to redact.
   * 
   * @return a redacted version of this instance; {@code this} instance if
   *         {@code cols} is empty
   */
  public SourceRow redactColumns(Collection<Integer> cols) {
    if (cols.isEmpty())
      return this;
    
    final int ccount = columns.size();
    
    var redactIter = cols.iterator();
    int nextRedact = redactIter.next();
    
    if (ccount < nextRedact)
      return this;
    
    if (nextRedact < 1)
      throw new IllegalArgumentException("illegal redact column number: " + nextRedact);
    
    ColumnValue[] values = columns.toArray(new ColumnValue[ccount]);
    
    for (int index = 0; index < ccount; ++index) {
      var cv = columns.get(index);
      if (index + 1 < nextRedact)
        values[index] = cv;
      else {
        values[index] = cv.getType().isHash() ? cv : new HashValue(cv.getHash());
        int oldRedact = nextRedact;
        nextRedact = redactIter.hasNext() ? redactIter.next() : Integer.MAX_VALUE;
        if (oldRedact >= nextRedact)
          throw new IllegalArgumentException(
              "out-of-sequence redact column numbers: " + oldRedact + ", " + nextRedact);
      }
    }
    return new SourceRow(rowNumber, Lists.asReadOnlyList(values), false);
  }
  
  
  
  /**
   * Determines whether any of the column values is redacted. A column value is considered
   * redacted if instead of its value, it is represented by its hash.
   * 
   * @return {@code true} iff one or more column values is of type {@linkplain ColumnType#HASH}
   */
  public final boolean hasRedactions() {
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
  
  
  
  
  
  
  public void writeSource(WritableByteChannel ch, byte[] colSeparator) throws IOException {
    
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
  }
  
  
  private void writeColumn(WritableByteChannel ch, ColumnValue col) throws IOException {
    switch (col.getType()) {
    case NULL:
      break;
    case BYTES:
    case HASH:
      ChannelUtils.writeRemaining(ch, ((BytesValue) col).getBytes());
      break;
    case STRING:
      {
        byte[] utf8 = ((StringValue) col).getString().getBytes(Strings.UTF_8);
        ChannelUtils.writeRemaining(ch, ByteBuffer.wrap(utf8));
      }
      break;
    case LONG:
    case DATE:
      {
        ByteBuffer buf = ByteBuffer.wrap(new byte[8]).putLong(0, ((LongValue) col).getNumber());
        ChannelUtils.writeRemaining(ch, buf);
      }
      break;
    case DOUBLE:
      {
        ByteBuffer buf = ByteBuffer.wrap(new byte[8]).putDouble(0, ((DoubleValue) col).getNumber());
        ChannelUtils.writeRemaining(ch, buf);
      }
      break;
    }
  }
  
  
  public void writeSource(Writer writer, String colSeparator) throws IOException {

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
  }
  

  /**
   * Writes this source row to the given writer.
   * <p>
   * Note this is totally inadequate. A user should be able to format this as they
   * like. Keeping it simple for now.
   * </p>
   * 
   * @param colSeparator Optional column separator. Eg comma, or tab delimited
   */
  public void writeSource(Writer writer, String colSeparator, String redactSymbol) throws IOException {
    
    if (redactSymbol == null) {
      writeSource(writer, colSeparator);
      return;
    }
    final int colCount = columns.size();
    
    writeRedactableColumn(writer, columns.get(0), redactSymbol);
    
    
    if (colCount > 1) {
      if (colSeparator == null)
        colSeparator = "";
      
      for (int index = 1; index < colCount; ++index) {
        writer.write(colSeparator);
        writeRedactableColumn(writer, columns.get(index), redactSymbol);
      }
    }
  }
  
  
  private void writeRedactableColumn(
      Writer writer, ColumnValue col, String redactSymbol) throws IOException {
    
    if (col.getType().isHash())
      writer.append(redactSymbol);
    else
      writeColumn(writer, col);
  }
  
  
  private void writeColumn(Writer writer, ColumnValue col) throws IOException {
    switch (col.getType()) {
    case NULL:
      return;
    case BYTES:
    case HASH:
      writer.write(IntegralStrings.toHex(((BytesValue) col).getBytes()));
      break;
    case LONG:
      writer.write(String.valueOf(((LongValue) col).getNumber()));
      break;
    case STRING:
      writer.write(((StringValue) col).getString());
      break;
    case DOUBLE:
      writer.write(String.valueOf(((DoubleValue) col).getNumber()));
      break;
    case DATE:
      writer.write(new Date(((DateValue) col).getUtc()).toString());
      break;
    
    }
    
  }
  
  
  /**
   * 
   * Returns a string representation of the object. This can be quite big. (It doesn't
   * trim the columns if they're big.) Inspect the individual {@linkplain ColumnValue}s, if
   * not sure.
   * 
   * @return {@code toString(" ")}
   * 
   * @see #safeDebugString()
   */
  public String toString() {
    return toString(" ", null);
  }
  
  
  /**
   * Returns a string representation of the object. This can be quite big. (It doesn't
   * trim the columns if they're big.) Use sparingly.
   * 
   * @param sep the column separator
   */
  public String toString(String sep, String redactSymbol) {
    if (sep == null)
      sep = "";
    try {
      StringWriter sw = new StringWriter(64);
      writeSource(sw, sep, redactSymbol);
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
      case LONG:
        string.append('[').append(((LongValue) columnVal).getNumber()).append(']');
        break;
      case DATE:
        string.append('[').append(new Date(((DateValue) columnVal).getUtc())).append(']');
        break;
      case DOUBLE:
        string.append(((DoubleValue) columnVal).getNumber()).append(']');
      case NULL:
      }
    }
    return string.toString();
  }

}











