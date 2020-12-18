/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import io.crums.io.IoStateException;
import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.hash.Digest;

/**
 * A list of connected rows in a {@linkplain SkipLedger} with strictly ascending row numbers.
 * This describes a hash proof that specified rows are indeed from the same ledger with strong
 * constraints on the possible row numbers.
 * 
 * <h3>Validation Guarantee</h3>
 * <p>
 * Every instance's hash pointers are checked for validity at construction. Furthermore,
 * every public constructor makes a defensive copy of its row inputs. The upshot of this
 * guarantee is that a reference to instance at runtime, is a reference to an immutable proof.
 * </p>
 */
public class Path implements Digest, Serial {
  
  /**
   * The maximum number of rows in a path.
   */
  public final static int MAX_ROWS = 256 * 256;
  
  private final static int ROW_COUNT_OVERHEAD = Integer.BYTES;
  
  
  /**
   * Loads and returns a new instance from its serial form.  Since the data is self-delimiting,
   * any trailing data in the file is ignored.
   * 
   * {@link #serialize()}
   */
  public static Path load(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      return load(in.getChannel());
    }
  }
  
  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Path load(InputStream in) throws IOException {
    return load(ChannelUtils.asChannel(in));
  }
  

  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Path load(ReadableByteChannel in) throws IOException {
    // note the size below (4k) is a bit less than theoretical capacity
    ByteBuffer buffer = ByteBuffer.allocate(128 * SldgConstants.HASH_WIDTH);
    
    ChannelUtils.readRemaining(in, buffer.limit(ROW_COUNT_OVERHEAD));
    int count = buffer.flip().getInt();
    
    if (count < 1 || count > MAX_ROWS)
      throw new IoStateException("read illegal count " + count);
    
    Row[] rows = new Row[count];
    
    for (int index = 0; index < count; ++index) {
      buffer.clear().limit(8);
      
      long rowNumber = ChannelUtils.readRemaining(in, buffer).flip().getLong();
      int rowCells = 1 + Ledger.skipCount(rowNumber);
      
      buffer.clear().limit(rowCells * SldgConstants.HASH_WIDTH);
      ChannelUtils.readRemaining(in, buffer).flip();
      rows[index] = new SerialRow(rowNumber, buffer);
    }
    
    return new Path(Lists.asReadOnlyList(rows), false);
  }
  
  
  
  
  public static Path load(ByteBuffer in) throws BufferUnderflowException {
    int count = in.getInt();

    if (count < 1 || count > MAX_ROWS)
      throw new IllegalArgumentException("read illegal count " + count + " from " + in);
    
    Row[] rows = new Row[count];
    
    for (int index = 0; index < count; ++index) {
      
      long rowNumber = in.getLong();
      int rowCells = 1 + Ledger.skipCount(rowNumber);
      
      int savedLimit = in.limit();
      int newLimit = in.position() + rowCells * SldgConstants.HASH_WIDTH;
      
      in.limit(newLimit);
      rows[index] = new SerialRow(rowNumber, in);
      in.limit(savedLimit).position(newLimit);
    }
    
    return new Path(Lists.asReadOnlyList(rows), false);
  }
  
  
  
  
  
  
  //     I N S T A N C E    M E M B E R S
  
  
  
  protected final List<Row> path;

  /**
   * Constructs a new instance. The input <tt>path</tt> is defensively copied.
   * (An instance must guarantee immutable state.)
   * 
   * @param path non-empty list of rows with ascending row numbers, each row linked to
   *  from the next via one of that next row's hash pointers
   * 
   * @see #MAX_ROWS
   */
  public Path(List<Row> path) {
    
    if (Objects.requireNonNull(path, "null path").isEmpty())
      throw new IllegalArgumentException("empth path");
    else if (path.size() > MAX_ROWS)
      throw new IllegalArgumentException("too many rows: " + path.size());
    
    Row[] rows = new Row[path.size()];
    rows = path.toArray(rows);
    
    this.path = Lists.asReadOnlyList(rows);
    
    verify(this.path);
  }
  
  
  /**
   * Package-private. Makes no defensive copy.
   * 
   * @param path must be read-only (not copied)
   * @param ignore ignored; constructor disambiguator
   */
  Path(List<Row> path, boolean ignore) {
    this.path = path;
    verify(path);
  }
  
  
  /**
   * Copy constructor.
   */
  protected Path(Path copy) {
    this.path = Objects.requireNonNull(copy, "null copy").path;
  }


  // D I G E S T     M E T H O D S
  
  @Override
  public final int hashWidth() {
    return path.get(0).hashWidth();
  }

  @Override
  public final String hashAlgo() {
    return path.get(0).hashAlgo();
  }
  
  
  
  
  /**
   * Returns the list of rows specifying the unbroken path.
   * TODO: rename me rows()
   * 
   * @return non-empty list of rows with ascending row numbers, each successive row linked to
   *  the previous via one of the next row's hash pointers
   */
  public List<Row> path() {
    return path;
  }
  
  
  /**
   * Returns the lowest (first) row number in the list of {@linkplain #path()}.
   */
  public final long loRowNumber() {
    return first().rowNumber();
  }
  
  
  /**
   * Returns the first row.
   */
  public final Row first() {
    return path.get(0);
  }
  
  
  /**
   * Returns the last row.
   */
  public final Row last() {
    return path.get(path.size() - 1);
  }

  /**
   * Returns the highest (last) row number in the list of {@linkplain #path()}.
   */
  public final long hiRowNumber() {
    return last().rowNumber();
  }
  
  
  /**
   * Determines whether this linked path is a skip path. A skip path describes
   * the shortest possible path connecting the {@linkplain #loRowNumber()} from
   * the {@linkplain #hiRowNumber()}.
   * 
   * @see SkipPath
   */
  public boolean isSkipPath() {
    List<Long> vRowNumbers = Ledger.skipPathNumbers(
        path.get(0).rowNumber(),
        path.get(path.size() - 1).rowNumber());
    
    return Lists.map(path, r -> r.rowNumber()).equals(vRowNumbers);
  }
  
  
  
  public boolean intersects(Path other) {
    if (!Digest.equal(this, other))
      return false;
    
    
    SortedSet<Long> coverage = Ledger.coverage(rowNumbers());
    SortedSet<Long> otherCoverage = Ledger.coverage(other.rowNumbers());
    
    // TODO: compare the two rows, not just their numbers
    
    return Sets.intersect(coverage, otherCoverage);
  }
  
  
  /**
   * Returns the row numbers in the path.
   * 
   * @return non-empty, ascending list of row numbers
   */
  public final List<Long> rowNumbers() {
    return Lists.map(path, r -> r.rowNumber());
  }
  

  @Override
  public int serialSize() {
    int hashCells =
        path.stream().mapToInt( r -> 1 + Ledger.skipCount(r.rowNumber()) ).sum();
    int rowNumOverhead = 8 * path.size();
    return ROW_COUNT_OVERHEAD + rowNumOverhead + hashCells * hashWidth();
  }
  
  
  /**
   * Writes the {@linkplain #serialize() serial representation} of this instance
   * to the given <tt>out</tt> buffer. The position of the buffer is advanced.
   * 
   * @throws BufferUnderflowException if <tt>out</tt> doesn't have adequate remaining
   *          bytes
   *          
   * @return <tt>out</tt> (for invocation chaining)
   * 
   * @see #load(ByteBuffer)
   * @see #serialize()
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferUnderflowException {
    out.putInt(path.size());
    path.forEach(r -> out.putLong(r.rowNumber()).put(r.data()));
    return out;
  }
  
  
  
  /**
   * Verifies the hashes in the path.
   */
  private void verify(List<Row> path) {
    MessageDigest digest = path.get(0).newDigest();

    // artifact of deciding to reverse the order of the displayed rows
    path = Lists.reverse(path);
    
    for (int index = 0, nextToLast = path.size() - 2; index < nextToLast; ++index) {
      Row row = path.get(index);
      Row prev = path.get(index + 1);
      
      if (!Digest.equal(row, prev))
        throw new IllegalArgumentException(
            "digest conflict at index " + index + ": " +
                path.subList(index, index + 2));
      
      long rowNumber = row.rowNumber();
      long prevNumber = prev.rowNumber();
      long deltaNum = rowNumber - prevNumber;
      
      if (deltaNum < 1 || rowNumber % deltaNum != 0)
        throw new IllegalArgumentException(
            "row numbers at index " + index + ": " + path.subList(index, index + 2));
      if (Long.highestOneBit(deltaNum) != deltaNum)
        throw new IllegalArgumentException(
            "non-power-of-2 delta " + deltaNum + " at index " + index + ": " +
                path.subList(index, index + 2));
      
      digest.reset();
      digest.update(prev.data());
      ByteBuffer prevRowHash = ByteBuffer.wrap(digest.digest());
      
      int pointerIndex = Long.numberOfTrailingZeros(deltaNum);
      ByteBuffer hashPointer = row.prevHash(pointerIndex);
      if (!prevRowHash.equals(hashPointer))
        throw new IllegalArgumentException(
            "hash conflict at index " + index + ": " + path.subList(index, index + 2));
    }
  }

}
