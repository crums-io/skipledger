/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
  
  
  
  /** Size of int. */
  public final static int STITCH_COUNT_SIZE = 4;

  /** Size of byte. */
  public final static int TYPE_SIZE = 1;
  
  public final static byte FULL_TYPE = 0;
  
  public final static byte CNDN_TYPE = 1;
  
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
    
    final boolean condensed;
    {
      byte type = hashBlock.get();
      condensed = switch (type) {
        case FULL_TYPE  -> false;
        case CNDN_TYPE  -> true;
        default -> throw new ByteFormatException("illegal type byte: " + type);
      };
    }
    int inputSize = HASH_WIDTH * inputRns.size();
    int hashSize;
    {
      int refCount = (condensed ?
          SkipLedger.refOnlyCondensedCoverage(inputRns) :
          SkipLedger.refOnlyCoverage(inputRns))
          .tailSet(1L).size();

      hashSize = refCount * HASH_WIDTH;
    }
    int funnelSize = condensed ?  SkipLedger.countFunnelBytes(inputRns) : 0;
    
    {
      int reqSize = inputSize + hashSize + funnelSize;
      if (hashBlock.remaining() < reqSize)
        throw new ByteFormatException(
            "underflow: required " + reqSize + " bytes; actual is " +
            hashBlock.remaining());
    }
    
    
    var inputs = BufferUtils.slice(hashBlock, inputSize);
    
    if (condensed) {
      var funnels = BufferUtils.slice(hashBlock, funnelSize);
      var hashes = BufferUtils.slice(hashBlock, hashSize);
      return new PathPack(inputRns, hashes, inputs, funnels);
    }

    var hashes = BufferUtils.slice(hashBlock, hashSize);
    return new PathPack(inputRns, hashes, inputs);
  }
  
  

  public static PathPack forPath(Path path) {
    
    // flatten the row no.s list
    // so the returned pack object doesn't internally
    // reference the given path argument
    
    List<Long> inputRns = List.copyOf(path.rowNumbers());

    final boolean condensed = path.isCondensed();

    var refOnlyRns = ( condensed ?
            SkipLedger.refOnlyCondensedCoverage(inputRns) :
            SkipLedger.refOnlyCoverage(inputRns) )
            .tailSet(1L);
      
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

    if (!condensed)
      return new PathPack(inputRns, refHashes, inputHashes);



    
    var funnelList = new ArrayList<List<ByteBuffer>>();


    long lastRn = path.loRowNumber() - 1L;
    int hashCount = 0;

    try {
      for (Row row : path.rows().subList(1, path.length())) {
        var levelsPtr = row.levelsPointer().compressToLevelRowNo(lastRn);
        lastRn = levelsPtr.rowNo();
        if (!levelsPtr.isCondensed())
          continue;
        var funnel = levelsPtr.funnel();
        hashCount += funnel.size();
        funnelList.add(funnel);
      }
    } catch (IllegalArgumentException iax) {
      if (lastRn == path.loRowNumber() - 1L)
        throw new IllegalArgumentException(
            "not enuf info to pack first row [" + lastRn +
            "]: " + iax.getMessage());

      throw iax;
    }

    assert hashCount > 0;

    var funnels = ByteBuffer.allocate(hashCount * HASH_WIDTH);
    for (var funnel : funnelList)
      for (var hash : funnel)
        funnels.put(hash);

    assert !funnels.hasRemaining();
    funnels.flip();

    return new PathPack(inputRns, refHashes, inputHashes, funnels);
  }
  
  
  
  
  
  
  
  private final List<Long> inputRns;
  private final List<Long> hashRns;
  
  private final ByteBuffer hashes;
  private final ByteBuffer inputs;

  private final ByteBuffer funnels;
  private final List<Long> funnelRns;
  private final List<Integer> funnelOffsets;
  
  
  
  
  
  /**
   * Creates a new "un-condensed" instance. Caller agrees not to change the contents
   * of the input arguments.
   * 
   * @param inputRns  ascending row no.s with input hashes (full rows)
   * @param hashes    ref-only hashes (row no.s inferred from {@code inputRns}
   * @param inputs    input hashes
   */
  PathPack(List<Long> inputRns, ByteBuffer hashes, ByteBuffer inputs) {
    
    this.inputRns = Objects.requireNonNull(inputRns, "null inputRns");
    this.hashRns = List.copyOf(
        SkipLedger.refOnlyCoverage(inputRns).tailSet(1L));  // sans 0L
    
    this.hashes = BufferUtils.readOnlySlice(hashes);
    this.inputs = BufferUtils.readOnlySlice(inputs);


    this.funnels = BufferUtils.NULL_BUFFER;
    this.funnelRns = List.of();
    this.funnelOffsets = List.of();
    
    
    checkCommonArgs(hashes, inputs);
    
  }


  private void checkCommonArgs(ByteBuffer hashes, ByteBuffer inputs) {

    if (inputRns.isEmpty())
      throw new IllegalArgumentException("inputRns is empty");
    
    checkRnsBuffer(
        inputRns, inputs, "input row no.s / buffer size mismatch");
    checkRnsBuffer(
        hashRns, hashes, "hash row no.s / buffer size mismatch");
  }


  private void checkRnsBuffer(
    List<Long> rns, ByteBuffer buffer, String preamble) {
    if (rns.size() * HASH_WIDTH != buffer.remaining())
        throw new IllegalArgumentException(
            preamble + ":\n " + rns + "\n " + buffer);
  }



  PathPack(
      List<Long> inputRns, ByteBuffer hashes, ByteBuffer inputs,
      ByteBuffer funnels) {
    
    this.inputRns = inputRns;
    this.hashRns = List.copyOf(
        SkipLedger.refOnlyCondensedCoverage(inputRns).tailSet(1L)); // sans 0L

    this.hashes = BufferUtils.readOnlySlice(hashes);
    this.inputs = BufferUtils.readOnlySlice(inputs);
    this.funnels = BufferUtils.readOnlySlice(funnels);

    checkCommonArgs(hashes, inputs);

    var condensedRns = new ArrayList<Long>(inputRns.size());
    var funOffs = new ArrayList<Integer>(inputRns.size());
    
    long prevRn = inputRns.get(0) - 1L;
    assert prevRn >= 0;
    int offset = 0;
    
    for (Long rn : inputRns) {
      assert prevRn < rn;
      if (SkipLedger.isCondensable(rn)) {
        condensedRns.add(rn);
        funOffs.add(offset);
        offset += SkipLedger.funnelLength(prevRn, rn) * HASH_WIDTH;
      }
      prevRn = rn;
    }

    if (condensedRns.isEmpty())
      throw new IllegalArgumentException("no condensable rows: " + inputRns);
    
    if (offset != this.funnels.remaining())
      throw new IllegalArgumentException(
          "expected " + offset + " funnel bytes; actual was " +
          this.funnels.remaining() + "; funnels " + funnels + ": " + inputRns);

    this.funnelRns = Lists.asReadOnlyList(condensedRns);
    this.funnelOffsets = Lists.asReadOnlyList(funOffs);
  }



  
  
  /**
   * Promotion / copy constructor.
   */
  public PathPack(PathPack copy) {
    this.inputRns = copy.inputRns;
    this.hashRns = copy.hashRns;
    
    this.hashes = copy.hashes;
    this.inputs = copy.inputs;

    this.funnels = copy.funnels;
    this.funnelRns = copy.funnelRns;
    this.funnelOffsets = copy.funnelOffsets;
  }
  
  
  @Override
  public final boolean isCondensed() {
    return !funnelRns.isEmpty();
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



  @Override
  public Optional<List<ByteBuffer>> getFunnel(long rn, int level) {
    Objects.checkIndex(level, SkipLedger.skipCount(rn));
    int findex = Collections.binarySearch(funnelRns, rn);
    if (findex < 0)
      return Optional.empty();
    int offset = funnelOffsets.get(findex);
    int end =
        findex + 1 == funnelRns.size() ?
            funnels.capacity() :
            funnelOffsets.get(findex + 1);

    var sub =
        funnels.asReadOnlyBuffer().position(offset).limit(end).slice();

    long refRn = rn - (1L << level);
    

    if (end - offset != SkipLedger.funnelLength(refRn, rn) * HASH_WIDTH)
      return Optional.empty();

    List<ByteBuffer> fList = 
        Lists.functorList(
            sub.remaining() / HASH_WIDTH,
            i -> sub.slice()
                .position(i * HASH_WIDTH).limit((i+1) * HASH_WIDTH));

    return Optional.of(fList);
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
    return
        STITCH_COUNT_SIZE + TYPE_SIZE +
        preStitchRowNos().size() * 8 +
        hashes.remaining() +
        inputs.remaining() + 
        funnels.remaining();
    
  }

  
  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    
    // calculate the stitch row no.s..
    List<Long> stitchRns = preStitchRowNos();
    byte type = isCondensed() ? CNDN_TYPE : FULL_TYPE;

    // haven't written anything yet.. write it all out
    out.putInt(stitchRns.size());
    return writeLongs(stitchRns, out)
        .put(type)
        .put(inputs.slice())
        .put(funnels.slice())
        .put(hashes.slice());
  }
  
  
  
  /**
   * Returns the full row numbers in pre-stitched form.
   */
  public List<Long> preStitchRowNos() {
    
    return SkipLedger.stitchCompress(inputRns);
  }
  
  
  
  
  
  
  
  
  private ByteBuffer writeLongs(List<Long> rns, ByteBuffer out) {
    final int size = rns.size();
    for (int index = 0; index < size; ++index)
      out.putLong(rns.get(index));
    return out;
  }

}
