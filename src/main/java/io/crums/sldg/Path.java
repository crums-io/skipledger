/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.Serial;
import io.crums.io.channels.ChannelUtils;
import io.crums.model.HashUtc;
import io.crums.util.Lists;
import io.crums.util.NaturalTuple;
import io.crums.util.Sets;
import io.crums.util.Tuple;
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
 * guarantee is that a reference to an instance at runtime, is a reference to an immutable proof.
 * </p>
 * <h3>Performance Note</h3>
 * <p>
 * Instances are <em>small</em> objects. Self reminder: there's little point in optimization.
 * Serialization footprint, maybe; clock-cycles NO(!).
 * </p>
 * <h3>Class Hierarchy Note</h3>
 * <p>
 * This base class knows about its subclasses via its {@linkplain Serial} implementation.
 * It's an anti-pattern, but one which centralizes factory methods for loading (which is why
 * it knows about everyone). It could be refactored, but at the cost of complexity. Ultimately,
 * the simpler wins--unless you find something simpler.
 * </p>
 */
public class Path implements Digest, Serial {
  
  
  /**
   * The maximum number of rows in a path.
   */
  public final static int MAX_ROWS = 256 * 256;
  
  
  private final static int MIXED_ROW_NUM_FLAG = 1;
  private final static int SKIP_ROW_NUM_FLAG = 2;
  private final static int CAMEL_ROW_NUM_FLAG = 4;
  
  private final static int TARGET_OPT_FLAG = Integer.MIN_VALUE;
  
  private final static int SANS_OPTS = ~TARGET_OPT_FLAG;
  
  
  
  private static boolean setsTarget(int type) {
    return (type & TARGET_OPT_FLAG) != 0;
  }
  
  private static int sansOpts(int type) {
    return type & SANS_OPTS;
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
   * Unchecked {@linkplain #load(InputStream) load} more suitable for functional idioms.
   */
  public static Path loadUnchecked(InputStream in) throws UncheckedIOException {
    try {
      return load(in);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  

  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Path load(ReadableByteChannel in) throws IOException {
    
    // TODO: implement with fewer reads.
    //       the only complexity being in that you mustn't read beyond
    //       the data
    
    // allocate 4kB
    
    ByteBuffer buffer = ByteBuffer.allocate(128 * SldgConstants.HASH_WIDTH);
    
    ChannelUtils.readRemaining(in, buffer.limit(4)).flip();
    
    final int type = buffer.getInt();
    
    List<Long> rowNumbers;
    
    switch (sansOpts(type)) {
    
    case MIXED_ROW_NUM_FLAG:
      {
        int pos = buffer.position();
        int limit = pos + 4;
        
        buffer.limit(limit);
        ChannelUtils.readRemaining(in, buffer).flip();
        int count = buffer.getInt(pos);
        
        if (count < 1 || count > MAX_ROWS)
          throw new IllegalArgumentException("read illegal count " + count);
        
        pos = limit;
        limit += count * 8;
        
        if (setsTarget(type))
          limit += 8;
        
        buffer = ensureCapacity(buffer, limit + 4);
        buffer.limit(limit).position(pos);
        
        ChannelUtils.readRemaining(in, buffer).flip();
        
        rowNumbers = new ArrayList<>(count);
        for (int index = 0; index < count; ++index)
          rowNumbers.add(buffer.getLong(pos + index * 8));
      }
      break;
      
    case SKIP_ROW_NUM_FLAG:
      {
        int pos = buffer.position();
        int limit = pos + 16;
        
        if (setsTarget(type))
          limit += 8;
        
        buffer.limit(limit);
        
        ChannelUtils.readRemaining(in, buffer).flip();
        long lo = buffer.getLong(pos);
        long hi = buffer.getLong(pos + 8);
        rowNumbers = Ledger.skipPathNumbers(lo, hi);
      }
      break;
      
    case CAMEL_ROW_NUM_FLAG:
      {
        int pos = buffer.position();
        int limit = pos + 24;
        
        buffer.limit(limit);
        ChannelUtils.readRemaining(in, buffer).flip();
        long lo = buffer.getLong(pos);
        long hi = buffer.getLong(pos + 8);
        long target = buffer.getLong(pos + 16);
        rowNumbers = CamelPath.rowNumbers(lo, target, hi);
      }
      break;
      
    default:
      throw new IllegalArgumentException(
          "unknown row-num header flag: " + Integer.toHexString(type));
    }
    

    buffer = readRest(in, buffer, rowNumbers);

    return load(buffer);
  }
  
  
  private static ByteBuffer readRest(
      ReadableByteChannel in, ByteBuffer buffer, Collection<Long> rowNumbers) throws IOException {

    // read the beacon count
    int pos = buffer.limit();
    int limit = pos + 4;
    buffer.limit(limit).position(pos);

    ChannelUtils.readRemaining(in, buffer).flip();
    
    int bc = buffer.getInt(pos);
    if (bc < 0 || bc > MAX_ROWS)
      throw new IllegalArgumentException("read illegal beacon count " + bc);

    // read the rest
    
    int cells = Ledger.coverage(rowNumbers).tailSet(1L).size();
    
    pos = limit;
    // ea beacon 16 bytes (2 longs); ea cell 32 bytes
    limit += bc * 16 + cells * SldgConstants.HASH_WIDTH;

    buffer = ensureCapacity(buffer, limit);
    buffer.limit(limit).position(pos);
    
    return ChannelUtils.readRemaining(in, buffer).flip();
  }
  
  
  private static ByteBuffer ensureCapacity(ByteBuffer buffer, int capacity) {
    if (buffer.capacity() < capacity) {
      int pos = buffer.position();
      int limit = buffer.limit();
      ByteBuffer expanded = ByteBuffer.allocate(capacity);
      expanded.put(buffer.clear());
      
      // unneeded nice behavior
      expanded.limit(limit).position(pos);
      buffer.limit(limit).position(pos);
      
      return expanded;
    }
    return buffer;
  }
  
  
  

  /**
   * Loads and returns a new instance from its serial form.
   * 
   * {@link #serialize()}
   */
  public static Path load(ByteBuffer in) throws BufferUnderflowException {
    
    // read the row numbers
    // often, the set can be defined by far fewer numbers
    // than explicitly enumerating them, so these are
    // classified by types
    List<Long> rowNumbers;
    final int type = in.getInt();
    long target = 0;
    
    switch (sansOpts(type)) {
    
      case MIXED_ROW_NUM_FLAG:
        {
          int count = in.getInt();
          if (count < 1 || count > MAX_ROWS)
            throw new IllegalArgumentException("read illegal count " + count + " from " + in);
          Long[] rowNums = new Long[count];
          long lastNum = -1;
          for (int index = 0; index < count; ++index) {
            rowNums[index] = in.getLong();
            if (lastNum >= rowNums[index])
              throw new IllegalArgumentException(
                  "out-of-sequence row numbers " + lastNum + " and " + rowNums[index]);
            lastNum = rowNums[index];
          }
          rowNumbers = Lists.asReadOnlyList(rowNums);
          if (setsTarget(type))
            target = in.getLong();
        }
        break;
      
      case SKIP_ROW_NUM_FLAG:
        {
          long lo = in.getLong();
          long hi = in.getLong();
          rowNumbers = Ledger.skipPathNumbers(lo, hi);
          if (setsTarget(type))
            target = in.getLong();
        }
        break;
        
      case CAMEL_ROW_NUM_FLAG:
        {
          long lo = in.getLong();
          long hi = in.getLong();
          target = in.getLong();
          rowNumbers = CamelPath.rowNumbers(lo, target, hi);
        }
        break;
        
      default:

        throw new IllegalArgumentException(
            "unknown row-num header flag: " + Integer.toHexString(type));
        
    }
    
    // read the beacons
    List<Tuple<Long,Long>> beacons;
    {
      int bc = in.getInt();
      if (bc < 0 || bc > rowNumbers.size())
        throw new IllegalArgumentException(
            "nonsensical beacon count " + bc + "; " + rowNumbers);
      if (bc == 0)
        beacons = Collections.emptyList();
      else {
        @SuppressWarnings("unchecked")
        Tuple<Long,Long>[] array = (Tuple<Long,Long>[]) new Tuple<?,?>[bc];
        for (int index = 0; index < bc; ++index) {
          long rn = in.getLong();
          long utc = in.getLong();
          array[index] = new Tuple<>(rn, utc);
        }
        beacons = Lists.asReadOnlyList(array);
      }
    }
    
    // read the row hashes and build a lookup table
    Map<Long, ByteBuffer> hashLookup;
    {
      SortedSet<Long> coveredNumbers = Ledger.coverage(rowNumbers).tailSet(1L);
      
      hashLookup = new HashMap<>(coveredNumbers.size() + 1);
      // we don't write zeroes (the sentinel hash), so first up add it
      hashLookup.put(0L, SldgConstants.DIGEST.sentinelHash());
      
      final int limitSnapshot = in.limit();
      
      for (Long rn : coveredNumbers) {
        int limit = in.position() + SldgConstants.HASH_WIDTH;
        ByteBuffer hash = in.limit(limit).slice();
        hashLookup.put(rn, hash);
        in.limit(limitSnapshot).position(limit);
      }
    }
    
    // done reading, now interpret it
    
    Row[] rows = new Row[rowNumbers.size()];
    
    for (int index = 0; index < rows.length; ++index) {
      
      final Long rowNumber = rowNumbers.get(index);
      final int skipCount = Ledger.skipCount(rowNumber);
      
      ByteBuffer data = ByteBuffer.allocate((1 + skipCount) * SldgConstants.HASH_WIDTH);
      
      // per our protocol, this is the row's input hash
      ByteBuffer entry = hashLookup.get(rowNumber).clear();
      data.put(entry);
      
      // add the skip pointers
      for (int exp = 0; exp < skipCount; ++exp) {
        long refRowNum = rowNumber - (1L << exp);
        ByteBuffer hashPtr = hashLookup.get(refRowNum).clear();
        data.put(hashPtr);
      }
      
      assert !data.hasRemaining();
      data.flip();
      Row row = new SerialRow(rowNumber, data);
      
      rows[index] = row;
      
      // update the lookup, so subsequent lookups for this row number
      // (we were first in this loop) will see the row hash,
      // instead of the input hash
      // 
      hashLookup.put(rowNumber, row.hash());
    }
    
    List<Row> path = Lists.asReadOnlyList(rows);
    return
        target == 0 ?
            new Path(path, beacons, false) :
              new TargetPath(path, beacons, target, false);
  }
  
  
  
  
  //     I N S T A N C E    M E M B E R S
  
  
  
  protected final List<Row> rows;
  
  /**
   * Possibly empyty, read-only list of beacon row numbers together with their
   * <em>minimum</em> UTCs.
   * <ul>
   * <li>{@code Tuple.a} is the beacon row number: the row's entry hash is the beacon hash.</li>
   * <li>{@code Tuple.b} is the beacon's timestamp: the next row's age must be older than this.</li>
   * </ul>
   */
  protected final List<Tuple<Long, Long>> beacons;
  
  
  
  
  
  public Path(List<Row> path) {
    this(path, Collections.emptyList());
  }

  /**
   * Constructs a new instance. The input arguments are defensively copied.
   * (An instance must guarantee immutable state.)
   * 
   * @param path non-empty list of rows with ascending row numbers, each row linked to
   *  from the next via one of that next row's hash pointers
   * 
   * @see #MAX_ROWS
   */
  public Path(List<Row> path, List<Tuple<Long, Long>> beacons) {
    
    if (Objects.requireNonNull(path, "null path").isEmpty())
      throw new IllegalArgumentException("empth path");
    else if (path.size() > MAX_ROWS)
      throw new IllegalArgumentException("too many rows: " + path.size());
    
    Row[] rows = new Row[path.size()];
    rows = path.toArray(rows);
    
    this.rows = Lists.asReadOnlyList(rows);
    this.beacons = copy(beacons);
    
    verify();
  }
  
  
  /**
   * Constructs a new copy of the path with the specified beacons.
   */
  public Path(Path path, List<Tuple<Long, Long>> beacons) {
    this.rows = Objects.requireNonNull(path, "null path").rows;
    this.beacons = copy(Objects.requireNonNull(beacons, "null beacons"));
    verifyBeacons();
  }
  


  private List<Tuple<Long, Long>> copy(List<Tuple<Long, Long>> beacons) {

    int count = Objects.requireNonNull(beacons, "null beacons").size();
    switch (count) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(beacons.get(0));
    default:
      @SuppressWarnings("unchecked")
      Tuple<Long, Long>[] copy = (Tuple<Long, Long>[]) new Tuple<?, ?>[count];
      for (int index = 0; index < count; ++index)
        copy[index] = beacons.get(index);
      return Lists.asReadOnlyList(copy);
    }
  }
  
  
  
  /**
   * Package-private. Makes no defensive copy.
   * 
   * @param path must be read-only (not copied)
   * @param beacons must be read-only (not copied)
   * @param ignore ignored; constructor disambiguator
   */
  Path(List<Row> path, List<Tuple<Long,Long>> beacons, boolean ignore) {
    this.rows = path;
    this.beacons = beacons;
    verify();
  }
  
  
  /**
   * Copy constructor.
   */
  protected Path(Path copy) {
    this.rows = Objects.requireNonNull(copy, "null copy").rows;
    this.beacons = copy.beacons;
  }


  // D I G E S T     M E T H O D S
  
  @Override
  public final int hashWidth() {
    return rows.get(0).hashWidth();
  }

  @Override
  public final String hashAlgo() {
    return rows.get(0).hashAlgo();
  }
  
  
  
  
  /**
   * Returns the list of rows specifying the unbroken path.
   * 
   * @return non-empty list of rows with ascending row numbers, each successive row linked to
   *  the previous via one of the next row's hash pointers
   */
  public List<Row> rows() {
    return rows;
  }
  
  
  
  /**
   * Returns the beacon rows as a list of tuples, the first element of the tuple
   * as the row, the second, as the UTC time of the beacon.
   */
  public List<Tuple<Row,Long>> beaconRows() {
    return Lists.map(
        beacons,
        bcn -> new Tuple<>(rows.get(Collections.binarySearch(rowNumbers(), bcn.a)), bcn.b));
  }
  
  
  /**
   * Returns the beacon row-number/UTC tuples.
   * 
   * @return non-null, but possibly emply, ordered list of tuples (<em>both</em> fields are ordered)
   */
  public final List<Tuple<Long,Long>> beacons() {
    return beacons;
  }
  
  
  
  /**
   * Returns <tt>true</tt> iff {@linkplain #beaconRows()} is not empty.
   */
  public boolean hasBeacon() {
    return !beacons.isEmpty();
  }
  
  
  
  
  
  /**
   * Returns the lowest (first) row number in the list of {@linkplain #rows()}.
   */
  public final long loRowNumber() {
    return first().rowNumber();
  }
  
  
  /**
   * Returns the first row.
   */
  public final Row first() {
    return rows.get(0);
  }
  
  
  /**
   * Returns the target row. This is usually the {@linkplain #first()}
   * row, but it may be overriden.
   * 
   * @see #isTargeted()
   */
  public Row target() {
    return first();
  }
  
  
  /**
   * Returns the last row.
   */
  public final Row last() {
    return rows.get(rows.size() - 1);
  }

  /**
   * Returns the highest (last) row number in the list of {@linkplain #rows()}.
   */
  public final long hiRowNumber() {
    return last().rowNumber();
  }
  
  
  /**
   * Determines whether this is a skip path. A skip path describes
   * the shortest possible list of rows connecting the {@linkplain #hiRowNumber()} to
   * the {@linkplain #loRowNumber()}.
   * 
   * @see SkipPath
   */
  public final boolean isSkipPath() {
    List<Long> vRowNumbers = Ledger.skipPathNumbers(loRowNumber(), hiRowNumber());
    
    // skip paths are *minimum length paths that are unique
    // therefore there is no need to check the row numbers individually
    // as in say Lists.map(rows, r -> r.rowNumber()).equals(vRowNumbers);
    // just their sizes
    //
    // TODO: Document proof of above statement: structural uniqueness of minimum path
    //       ..and not in code (which is how I know this)
    
    return vRowNumbers.size() == rows.size();
  }
  
  
  
  /**
   * Determines if the {@linkplain #target() target} row number is different
   * from the {@linkplain #first() first}.
   * 
   * @return {@code target().rowNumber() != loRowNumber()}
   */
  public final boolean isTargeted() {
    return target().rowNumber() != loRowNumber();
  }
  
  
  
  
  
  
  
  
  // Note: way more subtle than this
  // Need to classify intersections
  // Extensions, containments, and inconclusive overlaps
  
//  /**
//   * Determines whether a common ledger for this and the given path exists.
//   * 
//   * @see #intersects(Path, boolean)
//   */
//  public Maybe commonLedgerExists(Path other) {
//    if (intersectsConsistently(other))
//      return Maybe.YES;
//    else
//      return intersects(other) ? Maybe.NO : Maybe.YES;
//  }
  
  
  
  
  
  
  
  
  
  /**
   * Determines if this instance and the given path have rows <em>any</em> equal
   * rows. (2 rows are equal iff both their row numbers and row hashes match.)
   * 
   * @return {@code intersects(path, false)}
   * @see #intersects(Path, boolean)
   */
  public final boolean intersects(Path path) {
    return intersects(path, false);
  }
  
  /**
   * Determines whether this rows intersects the other consistently. 2 paths intersect
   * consistently if some of their row numbers intersect <em>and</em> if each pair of
   * row hashes at those intersections are equal.
   * 
   * @return {@code intersects(path, true)}
   * @see #intersects(Path, boolean)
   */
  public final boolean intersectsConsistently(Path path) {
    return intersects(path, true);
  }
  

  /**
   * Determines whether this path intersects the other. This is can have more
   * restrictive semantics than a simple set intersection (see below). Regardless, it
   * is always a <em>symmetric</em> relationship (the answer is the same if
   * the roles of instance and argument are reversed).
   * 
   * 
   * <h3>Terminology</h3>
   * 
   * <p>Let's introduce some terminology:</p><p>
   * <ol>
   * <li><em>Intersect</em>. 2 paths intersect each other if they share <em>any</em> 2
   * {@linkplain Row#equals(Object) equal} rows. (2 rows are equal iff
   * both their row numbers and row hashes match.) </li>
   * <li><em>Intersect Consistently</em>. 2 paths intersect consistently if (a) they
   * intersect (as defined above) <em>and</em> (b) if any 2 row numbers in their respective
   * paths match, then so too their row hashes.</li>
   * </ol></p><p>
   * So if 2 paths intersect consistently, then <em>there exists a ledger</em> that can
   * produce them both;
   * if 2 paths intersect, but don't intersect consistently, then <em>no ledger exists</em>
   * that can produce them both.
   * </p>
   * 
   * @param other non-null
   * @param consistent if <tt>true</tt>, the test determines a consistent intersection.
   */
  public final boolean intersects(Path other, boolean consistent) {
    Objects.requireNonNull(other, "null other");
    
    if (other == this)
      return true;
    
    if (!Digest.equal(this, other))
      return false;
    
    final List<Long> rowNums = rowNumbers();
    final List<Long> otherRowNums = other.rowNumbers();
    
    Iterator<Long> iter = Sets.intersectionIterator(rowNums, otherRowNums);
    
    boolean intersects = false;
    
    while (iter.hasNext()) {
      
      Long rowNum = iter.next();

      final ByteBuffer rowHash;
      {
        int index = Collections.binarySearch(rowNums, rowNum);
        assert index >= 0;
        rowHash = rows.get(index).hash();
      }
      
      final ByteBuffer otherRowHash;
      {
        int index = Collections.binarySearch(otherRowNums, rowNum);
        assert index >= 0;
        otherRowHash = other.rows.get(index).hash();
      }
      
      final boolean rowsIntersect = rowHash.equals(otherRowHash);
      
      if (consistent && !rowsIntersect || !consistent && rowsIntersect)
        return rowsIntersect;
      
      intersects = rowsIntersect;
    }
    
    return intersects;
    
  }
  
  
  /**
   * Returns the row numbers in the rows, in the order they occur.
   * 
   * @return non-empty, ascending list of row numbers &ge; 1
   */
  public final List<Long> rowNumbers() {
    return Lists.map(rows, r -> r.rowNumber());
  }
  
  
  /**
   * Returns all the row numbers referenced in this path. This collection includes
   * the path's {@linkplain #rowNumbers() row numbers} as well other rows referenced thru the rows'
   * skip pointers.
   * <p>The upshot of this accounting is that the hash of every row number returned
   * is known by the instance. 
   * </p>
   * 
   * @see #getRowHash(long)
   */
  public final SortedSet<Long> rowNumbersCovered() {
    return Ledger.coverage(rowNumbers());
  }
  
  /**
   * Returns the hash of the row with the given <tt>rowNumber</tt>. This is not
   * just a convenience for getting the {@linkplain Row#hash() hash} of one of the
   * instance's {@linkplain #rows() rows}; it also provides a more sophisticated
   * algorithm so that it can return the hash of <em>any</em> row returned by
   * {@linkplain #rowNumbersCovered()}.
   * 
   * @throws IllegalArgumentException if <tt>rowNumber</tt> is not a member of the
   * set returned by {@linkplain #rowNumbersCovered()}
   */
  public final ByteBuffer getRowHash(long rowNumber) throws IllegalArgumentException {
    
    if (rowNumber == 0)
      return sentinelHash();
    
    List<Long> rowNumbers = rowNumbers();
    {
      int index = Collections.binarySearch(rowNumbers, rowNumber);
      
      if (index >= 0)
        return rows().get(index).hash();
      
      else if (-1 - index == rowNumbers.size())
        throw new IllegalArgumentException("rowNumber " + rowNumber + " not covered");
    }
    
    final int pointerLevels = Ledger.skipCount(rowNumber);
    
    for (int exp = 0; exp < pointerLevels; ++exp) {
      
      long referrerNum = rowNumber + (1L << exp);
      int index = Collections.binarySearch(rowNumbers, referrerNum);
      
      if (index >= 0) {
        Row row = rows.get(index);
        assert row.prevLevels() > exp;
        
        return row.prevHash(exp);
        
      } else if (-1 - index == rowNumbers.size())
        throw new IllegalArgumentException("rowNumber " + rowNumber + " not covered");
    }
    
    throw new IllegalArgumentException("rowNumber " + rowNumber + " not covered");
  }
  
  
  
  
  public Path concat(Path path) {
    Objects.requireNonNull(path, "null path");
    
    long delta;
    Path head, tail;
    
    if (path.hiRowNumber() <= loRowNumber()) {
      
      head = path;
      tail = this;
      long referringNum = loRowNumber();
      delta = referringNum - path.hiRowNumber();
      checkDelta(delta, path, referringNum);
    
    } else if (hiRowNumber() <= path.loRowNumber()) {
      
      head = this;
      tail = path;
      long referringNum = path.loRowNumber();
      delta = referringNum - hiRowNumber();
      checkDelta(delta, path, referringNum);
      
    } else
      throw new IllegalArgumentException(path + " not adjacent to " + this);
    
    List<Row> headRows = head.rows;
    List<Row> tailRows = tail.rows;
    if (delta == 0) {
      // remove the extra row
      if (this.rows.size() == 1)
        return path;
      else if (path.rows.size() == 1)
        return this;
      
      // trim the head rows (there's a choice here; we choose the head)
      headRows = headRows.subList(0, headRows.size() - 1);
    }
    
    List<Row> concatRows = Lists.concat(headRows, tailRows);
    // now merge the beacons
    
    List<Tuple<Long,Long>> mergedBeacons;
    if (beacons.isEmpty())
      mergedBeacons = path.beacons;
    else if (path.beacons.isEmpty())
      mergedBeacons = beacons;
    else {
      TreeSet<NaturalTuple<Long, Long>> mergedSet = new TreeSet<>();
      for (Tuple<Long,Long> b : beacons)
        mergedSet.add(new NaturalTuple<>(b));
      for (Tuple<Long,Long> b : path.beacons)
        mergedSet.add(new NaturalTuple<>(b));
      
      mergedBeacons = new ArrayList<>(mergedSet.size());
      mergedBeacons.addAll(mergedSet);
    }
    
    return new Path(concatRows, mergedBeacons);
      
  }
  
  
  private void checkDelta(long delta, Path path, long referringNum) {
    boolean ok =
        delta >= 0 &&
        delta == Long.highestOneBit(delta) &&
        63 - Long.numberOfLeadingZeros(delta) < Ledger.skipCount(referringNum);
        
    if (!ok)
      throw new IllegalArgumentException(path + " not adjacent to " + this);
    
  }
  
  
  
  
  
  

  /**
   * {@inheritDoc}
   * 
   * <p>The base class knows a lot about it's subclasses. This is because of the static
   * {@linkplain #load(ByteBuffer) load} method. Which is why this method is marked
   * <b><tt>final</tt></b>. It's because the base class is also a factory.</p>
   */
  @Override
  public final int serialSize() {
    int headerBytes;
    
    if (isSkipPath())
      headerBytes = plusTargeted(20); // 4 + 8 + 8
    else if (isCamelPath())
      headerBytes = 28; // 4 + 8 + 8 + 8
    else
      headerBytes = plusTargeted(8 + rows.size() * 8); // 4 + 4 + size * 8
    
    int beaconBytes = 4 + beacons.size() * 16;
    int rowsCovered = rowNumbersCovered().tailSet(1L).size();
    return headerBytes + beaconBytes + rowsCovered * hashWidth();
  }
  
  
  private int plusTargeted(int bytes) {
    return isTargeted() ? bytes + 8 : bytes;
  }
  
  
  private boolean isCamelPath() {
    if (!isTargeted())
      return false;

    Long target = target().rowNumber();
    if (target.longValue() == hiRowNumber())
      return false;
    
    List<Long> rowNumbers = rowNumbers();
    int index = Collections.binarySearch(rowNumbers, target);
    assert index > 0; // not >= 0, cuz it's targeted
    return
        Ledger.skipPathNumbers(loRowNumber(), target).equals(rowNumbers.subList(0, index + 1)) &&
        Ledger.skipPathNumbers(target, hiRowNumber()).equals(rowNumbers.subList(index, rowNumbers.size()));
  }
  
  
  /**
   * 
   * @see #load(ByteBuffer)
   * @see #serialSize()
   */
  @Override
  public final ByteBuffer writeTo(ByteBuffer out) throws BufferUnderflowException {
    
    // write the row number info
    if (isSkipPath()) {
      int type = SKIP_ROW_NUM_FLAG;
      if (isTargeted())
        type |= TARGET_OPT_FLAG;
      out.putInt(type).putLong(loRowNumber()).putLong(hiRowNumber());
      if (isTargeted())
        out.putLong(target().rowNumber());
    } else if (isCamelPath()) {
      out.putInt(CAMEL_ROW_NUM_FLAG)
      .putLong(loRowNumber()).putLong(hiRowNumber()).putLong(target().rowNumber());
    } else {
      int type = MIXED_ROW_NUM_FLAG;
      if (isTargeted())
        type |= TARGET_OPT_FLAG;
      out.putInt(type).putInt(rows.size());
      rows.forEach(r -> out.putLong(r.rowNumber()));
      if (isTargeted())
        out.putLong(target().rowNumber());
    }
    
    // write the beacon info
    {
      int bc = beacons.size();
      out.putInt(bc);
      for (Tuple<Long,Long> beacon : beacons)
        out.putLong(beacon.a).putLong(beacon.b);
    }
    
    // write the row data in the order of coverage
    // there are 2 types of data:
    //
    // (1) input hash for row numbers in the path, and
    // (2) row hash for covered row numbers _not_ in (1)
    //
    List<Long> rowNumbers = rowNumbers();
    SortedSet<Long> rnCovered = rowNumbersCovered().tailSet(1L);  // exclude the sentinel 0
    
    for (Long rn : rnCovered) {
      int index = Collections.binarySearch(rowNumbers, rn);
      if (index >= 0)
        out.put(rows.get(index).inputHash());
      else
        out.put(getRowHash(rn));
    }
    
    return out;
  }
  
  
  
  @Override
  public String toString() {
    return Path.class.getSimpleName() +
        "[lo:" + loRowNumber() + ",trgt:" + target().rowNumber() + ",hi:" + hiRowNumber() + "]";
  }
  
  
  
  /**
   * Verifies the hashes and beacons in the path.
   */
  private void verify() {
    verifyHashes();
    verifyBeacons();
    
  }
  
  
  private void verifyHashes() {
    MessageDigest digest = rows.get(0).newDigest();

    // artifact of deciding to reverse the order of the displayed rows
    List<Row> rpath = Lists.reverse(this.rows);
    
    for (int index = 0, nextToLast = rpath.size() - 2; index < nextToLast; ++index) {
      Row row = rpath.get(index);
      Row prev = rpath.get(index + 1);
      
      if (!Digest.equal(row, prev))
        throw new IllegalArgumentException(
            "digest conflict at index " + index + ": " +
                rpath.subList(index, index + 2));
      
      long rowNumber = row.rowNumber();
      long prevNumber = prev.rowNumber();
      long deltaNum = rowNumber - prevNumber;
      
      if (deltaNum < 1 || rowNumber % deltaNum != 0)
        throw new IllegalArgumentException(
            "row numbers at index " + index + ": " + rpath.subList(index, index + 2));
      if (Long.highestOneBit(deltaNum) != deltaNum)
        throw new IllegalArgumentException(
            "non-power-of-2 delta " + deltaNum + " at index " + index + ": " +
                rpath.subList(index, index + 2));
      
      digest.reset();
      digest.update(prev.data());
      ByteBuffer prevRowHash = ByteBuffer.wrap(digest.digest());
      
      int pointerIndex = Long.numberOfTrailingZeros(deltaNum);
      ByteBuffer hashPointer = row.prevHash(pointerIndex);
      if (!prevRowHash.equals(hashPointer))
        throw new IllegalArgumentException(
            "hash conflict at index " + index + ": " + rpath.subList(index, index + 2));
    }
  }
  
  
  private void verifyBeacons() {
    if (beacons.isEmpty())
      return;
    
    List<Long> rowNumbers = rowNumbers();
    
    Tuple<Long, Long> prev = beacons.get(0);
    
    checkRowNumber(prev, rowNumbers);
    if (prev.b < HashUtc.INCEPTION_UTC)
      throw new IllegalArgumentException(
          "nonsensical beacon UTC: " + prev.b);
    
    for (int index = 1; index < beacons.size(); ++index) {
      Tuple<Long, Long> beacon = beacons.get(index);
      checkRowNumber(beacon, rowNumbers);
      if (prev.a.longValue() >= beacon.a.longValue())
        throw new IllegalArgumentException(
            "out-of-sequence row numbers in beacons " + prev + " and " + beacon);
      if (prev.b.longValue() > beacon.b.longValue())
        throw new IllegalArgumentException(
            "out-of-sequence UTCs in beacons " + prev + " and " + beacon);
      
      prev = beacon;
    }
  }
  
  private void checkRowNumber(Tuple<Long, Long> beacon, List<Long> rowNumbers) {
    int index = Collections.binarySearch(rowNumbers, beacon.a);
    if (index < 0)
      throw new IllegalArgumentException(
          "beacon row number " + beacon.a + " (" + beacon + ") not found in path " + rowNumbers);
  }

}













