/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.util.Lists;

/**
 * Base {@linkplain RowBag} implementation. Note this uses no caching/memo-ization.
 * Consequently the cost every full-row lookup is linear in the row numbers. Not good.
 * When used to serialize a path (via {@link Path#pack()}), the poor perfomance doesn't
 * matter. When actually reading the contents of the pack, use the
 * {@linkplain MemoPathPack} subclass.
 * 
 */
public class PathPack implements PathBag, Serial {
  
  
  
  /** int size */
  private final static int STITCH_COUNT_SIZE = 4;
  
  /**
   * Loads and returns a new instance by reading the given buffer.
   * The buffer's position is advanced exactly. The <em>content</em> of
   * the buffer argument should not be later modified: this is because to the degree
   * feasible, the data is not actually copied. On return, the given {@code in} buffer
   * is advanced by as many bytes as were read (err, sliced out).
   */
  public static PathPack load(ByteBuffer in)
      throws ByteFormatException, BufferUnderflowException {
    
    final int bytes = in.remaining();

    try {
      
      final int stitchRnCount = in.getInt();
      
      if (stitchRnCount <= 0)
        throw new ByteFormatException("stitch row count " + stitchRnCount);
      
      List<Long> stitchRns = readAscendingLongs(in, stitchRnCount);
      
      return load(stitchRns, in);
      
    } catch (BufferUnderflowException bux) {
      throw new ByteFormatException("eof after reading " + bytes + "bytes", bux);
    }
  }

  private static List<Long> readAscendingLongs(ByteBuffer in, int count) {
    Long[] rowNums = new Long[count];
    rowNums[0] = in.getLong();
    if (rowNums[0] <= 0)
      throw new ByteFormatException("non-positive rn[0] " + rowNums[0]);
    
    for (int index = 1; index < count; ++index) {
      rowNums[index] = in.getLong();
      if (rowNums[index] <= rowNums[index - 1])
        throw new ByteFormatException(
            "rn[" + index + "] " + rowNums[index] + " <= prev rn " +  rowNums[index - 1]);
    }
    return Lists.asReadOnlyList(rowNums);
  }
  
  
  
  /**
   * Pseudo-constructor, also used by the JSON parser.
   * Do not modify the contents of {@code hashBlock}; on
   * return its position is advanced to 1 beyond the last
   * byte read.
   * 
   * @param stitchRns   abbreviate path row no.s
   * @param hashBlock   block of hashes
   */
  public static PathPack load(List<Long> stitchRns, ByteBuffer hashBlock) {

    List<Long> inputRns = SkipLedger.stitch(stitchRns);
    
    int inputSize = HASH_WIDTH * inputRns.size();
    int hashSize = HASH_WIDTH * (
        SkipLedger.coverage(inputRns).tailSet(1L).size() -
        inputRns.size());
    {
      int reqSize = inputSize + hashSize;
      if (hashBlock.remaining() < reqSize)
        throw new ByteFormatException(
            "underflow: required " + reqSize + " bytes; actual is " +
            hashBlock.remaining());
    }
    
    
    ByteBuffer hashes = BufferUtils.slice(hashBlock, hashSize);
    ByteBuffer inputs = BufferUtils.slice(hashBlock, inputSize);

    return new PathPack(inputRns, hashes, inputs);
  }
  
  

  public static PathPack forPath(Path path) {
    
    // flatten the row no.s list
    // so the returned pack object doesn't internally
    // reference the given path argument
    
    List<Long> inputRns = Lists.readOnlyCopy(path.rowNumbers());
    
    var refOnlyRns = SkipLedger.refOnlyCoverage(inputRns).tailSet(1L);
    
    var refHashes = ByteBuffer.allocate(
        refOnlyRns.size() * SldgConstants.HASH_WIDTH);
    
    refOnlyRns.forEach(rn -> refHashes.put(path.getRowHash(rn)));
    assert !refHashes.hasRemaining();
    
    refHashes.flip();
    
    ByteBuffer inputHashes = ByteBuffer.allocate(
        inputRns.size() * SldgConstants.HASH_WIDTH);
    
    
    for (int index = 0, count = inputRns.size(); index < count; ++index) {
      inputHashes.put(path.rows().get(index).inputHash());
    }
    assert !inputHashes.hasRemaining();
    
    inputHashes.flip();
    
    return new PathPack(inputRns, refHashes, inputHashes);
  }
  
  
  
  
  
  
  
  private final List<Long> inputRns;
  private final List<Long> hashRns;
  
  private final ByteBuffer hashes;
  private final ByteBuffer inputs;

  private final boolean condensed;  // redundant, but good for clarity
  private final ByteBuffer funnels;
  private final List<Integer> funnelOffsets;
  
  
  

//  /**
//   * 
//   */
//  private PathPack() {
//    hashRns = inputRns = List.of();
//    hashes = inputs = BufferUtils.NULL_BUFFER;
//  }
  
  
  /**
   * Creates a new "un-condensed" instance. Caller agrees not to change the contents
   * of the inputs.
   * 
   * @param inputRns  ascending row no.s with input hashes (full rows)
   * @param hashes    ref-only hashes (row no.s inferred from {@code inputRns}
   * @param inputs    input hashes
   */
  PathPack(List<Long> inputRns, ByteBuffer hashes, ByteBuffer inputs) {
    
    this.inputRns = Objects.requireNonNull(inputRns, "null inputRns");
    SortedSet<Long> refs = SkipLedger.refOnlyCoverage(inputRns).tailSet(1L);
    Objects.requireNonNull(hashes, "null hashes");
    Objects.requireNonNull(inputs, "null inputs");
    this.hashRns = Lists.readOnlyCopy(refs);
    this.hashes = BufferUtils.readOnlySlice(hashes);
    this.inputs = BufferUtils.readOnlySlice(inputs);

    
    this.condensed = false;
    this.funnels = null;
    this.funnelOffsets = null;
    
    // if the following throw, there's a bug..
    
    if (inputRns.isEmpty())
      throw new IllegalArgumentException("inputRns is empty");
    
    if (hashRns.size() * HASH_WIDTH != this.hashes.remaining())
      throw new IllegalArgumentException(
          "hash row numbers / buffer size mismatch:\n" +
           hashRns + "\n  " + this.hashes);

    if (inputRns.size() * HASH_WIDTH != this.inputs.remaining())
      throw new IllegalArgumentException(
          "input row numbers / buffer size mismatch:\n" +
          inputRns + "\n  " + this.inputs);
    
  }



  
  
  /**
   * Promotion / copy constructor.
   */
  public PathPack(PathPack copy) {
    this.inputRns = copy.inputRns;
    this.hashRns = copy.hashRns;
    
    this.hashes = copy.hashes;
    this.inputs = copy.inputs;

    this.condensed = copy.condensed;
    this.funnels = copy.funnels;
    this.funnelOffsets = copy.funnelOffsets;
  }
  
  
  
  public final ByteBuffer inputsBlock() {
    return inputs.asReadOnlyBuffer();
  }
  

  
  public final ByteBuffer refsBlock() {
    return hashes.asReadOnlyBuffer();
  }


  public final ByteBuffer funnelsBlock() {
    return funnels == null ? BufferUtils.NULL_BUFFER : funnels.asReadOnlyBuffer();
  }

  
  
  public boolean isEmpty() {
    return inputRns.isEmpty();
  }
  

  @Override
  public ByteBuffer refOnlyHash(long rowNumber) {
    int index = Collections.binarySearch(hashRns, rowNumber);
    return index < 0 ? null : emit(hashes, index);
  }
  

  @Override
  public ByteBuffer inputHash(long rowNumber) {

    int index = Collections.binarySearch(inputRns, rowNumber);
    
    if (index < 0) {
      if (rowNumber <= 0)
        SkipLedger.checkRealRowNumber(rowNumber);
      throw new IllegalArgumentException("no data for rowNumber " + rowNumber);
    }
    
    return emit(inputs, index);
  }
  
  
  @Override
  public List<Long> getFullRowNumbers() {
    return inputRns;
  }
  
  
  private class PackedPath extends Path {

    PackedPath(List<Row> rows) {
      super(rows, null);
    }
    
    
    @Override
    public PathPack pack() {
      return PathPack.this;
    }
    
  }
  
  
  @Override
  public Path path() {
    return new PackedPath(Lists.map(inputRns, rn -> getRow(rn)));
  }
  
  
  
  /**
   * Slices out 32-byte, read-only buffer, at the given
   * index.
   * 
   * @param store  sliced i.e. {@code store.remaining() == store.capacity()}
   * @param index  index into {@code store} in 32-byte unites
   * @return  sliced buffer
   */
  protected final ByteBuffer emit(ByteBuffer store, int index) {
    int offset = index * HASH_WIDTH;
    return store.asReadOnlyBuffer()
        .position(offset)
        .limit(offset + HASH_WIDTH)
        .slice();
  }

  @Override
  public int serialSize() {
    final int inputsCount = inputRns.size();
    final int dataBytes = inputs.capacity() + hashes.capacity();
    
    if (inputsCount <= 2)
      return STITCH_COUNT_SIZE + inputsCount*8 + dataBytes;
    
    final int stitchRns;
    {
      var candidate = List.of(lo(), hi());
      
      List<Long> skipRns = SkipLedger.stitch(candidate);
      
      if (skipRns.equals(inputRns))
        stitchRns = 2;
      
      else {
        var stitchSet = new TreeSet<Long>(candidate);
        for (int index = inputsCount - 1; index-- > 1; ) {
          Long rn = inputRns.get(index);
          if (Collections.binarySearch(skipRns, rn) >= 0)
            continue;
          stitchSet.add(rn);
          skipRns = SkipLedger.stitchSet(stitchSet);
        }
        stitchRns = stitchSet.size();
      }
    }
    
    return STITCH_COUNT_SIZE + stitchRns*8 + dataBytes;
  }

  
  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    final int inputsCount = inputRns.size();
    
    // handle the corner cases first..
    if (inputsCount <= 2) {
      out.putInt(inputsCount);
      return inputsCount == 0 ? out :
          writeLongs(inputRns, out)
            .put(hashes.slice())
            .put(inputs.slice());
    }
    
    // calculate the stitch row no.s..
    
    List<Long> stitchRns = compressedRowNos();

    // haven't written anything yet.. write it all out
    out.putInt(stitchRns.size());
    writeLongs(stitchRns, out).put(hashes.slice()).put(inputs.slice());
    
    return out;
  }
  
  
  
  /**
   * Returns the full row numbers in pre-stitched form.
   */
  public List<Long> compressedRowNos() {
    
    return SkipLedger.stitchCompress(inputRns);
  }
  
  
  
  
  
  
  
  
  private ByteBuffer writeLongs(List<Long> rns, ByteBuffer out) {
    final int size = rns.size();
    for (int index = 0; index < size; ++index)
      out.putLong(rns.get(index));
    return out;
  }

}
