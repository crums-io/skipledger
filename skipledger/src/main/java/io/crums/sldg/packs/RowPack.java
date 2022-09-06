/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;

import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.bags.RowBag;
import io.crums.util.Lists;

/**
 * Base {@linkplain RowBag} implementation. Note this uses no caching/memo-ization.
 * Consequently the cost every full-row lookup is linear in the row number. Not good:
 * validating the pack then is <b>0</b>(n<sup><small>2</small></sup>). See {@linkplain CachingRowPack}.
 * 
 * <h2>Serial Format</h2>
 * <p>
 * Below, the data types are first defined, then the row pack data structure itself.
 * </p>
 * <h3>Definitions</h3>
 * <pre>
 * 
 *    HASH        := BYTE ^32
 *    RN          := LONG
 *    RHASH       := HASH   // hash of entire row (used as the value of a hash pointer)
 *    IHASH       := HASH   // input hash
 *                          // the hash of row is computed from IHASH RHASH^p with the RHASH's
 *                          // being the row's hash [skip] pointers
 *    
 * </pre>
 * 
 * <h3>Structure</h3>
 * <pre>
 *    
 *    I_COUNT     := INT                // number of rows with full info (have input hash)
 *    FULL_RNS    := RN ^ICOUNT         // full info row numbers in ascending order
 *    
 *    // The following is inferred from FULL_RNS
 *    // see {@linkplain SkipLedger#refOnlyCoverage(java.util.Collection)}
 *    
 *    R_COUNT     := INT                // (INFERRED)
 *     
 *                              
 *    R_TBL       := RHASH ^R_COUNT     // hash pointer table
 *    I_TBL       := IHASH ^I_COUNT     // input hash table
 *    
 *    HASH_TBL    := R_TBL I_TBL
 *    
 *    ROW_BAG     := ICOUNT FULL_RNS HASH_TBL
 * </pre>
 * 
 */
public class RowPack extends RecurseHashRowPack {
  

  
  /**
   * Loads and returns a new instance by reading the given buffer. The <em>content</em> of
   * the buffer argument should not be later modified: this is because to the degree
   * feasible, the data is not actually copied. On return, the given {@code in} buffer
   * is advanced by as many bytes as were read (err, sliced out).
   * 
   * @throws ByteFormatException if an error or inconsistency in the data is discovered
   */
  public static RowPack load(ByteBuffer in) throws ByteFormatException {
    final int bytes = in.remaining();
    try {
      
      final int fullRows = in.getInt();
      
      if (fullRows <= 0) {
        if (fullRows == 0)
          return EMPTY;
        throw new ByteFormatException("full row count " + fullRows);
      }
      
      List<Long> inputRns = readAscendingRows(in, fullRows);
      
      int inputSize = HASH_WIDTH * fullRows;
      int hashSize =
          HASH_WIDTH * (SkipLedger.coverage(inputRns).tailSet(1L).size() - inputRns.size());
      {
        int reqSize = inputSize + hashSize;
        if (in.remaining() < reqSize)
          throw new ByteFormatException(
              "required " + reqSize + " more bytes; actual is " + in.remaining());
      }
      
      
      
      ByteBuffer hashes = BufferUtils.slice(in, hashSize);
      ByteBuffer inputs = BufferUtils.slice(in, inputSize);
      
      return new RowPack(inputRns, hashes, inputs);
      
    } catch (BufferUnderflowException bux) {
      throw new ByteFormatException("eof after reading " + bytes + "bytes", bux);
    }
  }
  
  
  private static List<Long> readAscendingRows(ByteBuffer in, int count) {
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
  
  
  public final static RowPack EMPTY = new RowPack();
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  private final List<Long> inputRns;
  private final List<Long> hashRns;
  
  private final ByteBuffer hashes;
  private final ByteBuffer inputs;
  
  
  // NULL instance constructor
  private RowPack() {
    hashRns = inputRns = Collections.emptyList();
    hashes = inputs = BufferUtils.NULL_BUFFER;
  }
  
  
  RowPack(List<Long> inputRns, ByteBuffer hashes, ByteBuffer inputs) {
    
    this.inputRns = Objects.requireNonNull(inputRns, "null inputRns");
    SortedSet<Long> refs = SkipLedger.refOnlyCoverage(inputRns).tailSet(1L);
    
    this.hashRns = Lists.readOnlyCopy(refs);
    this.hashes = BufferUtils.readOnlySlice(Objects.requireNonNull(hashes, "null hashes"));
    this.inputs = BufferUtils.readOnlySlice(Objects.requireNonNull(inputs, "null inputs"));
    
    if (inputRns.isEmpty())
      throw new IllegalArgumentException("inputRns is empty");
    
    if (hashRns.size() * HASH_WIDTH != hashes.remaining())
      throw new IllegalArgumentException(
          "hash row number / buffer mismatch:\n" + hashRns + "\n" + hashes);

    if (inputRns.size() * HASH_WIDTH != inputs.remaining())
      throw new IllegalArgumentException(
          "input row number / buffer mismatch:\n" + inputRns + "\n" + inputs);
    
  }
  
  
  /**
   * Copy constructor for subclasses. Not public cuz no reason to: instances are stateless.
   * 
   * @param copy non-null
   */
  protected RowPack(RowPack copy) {
    this.hashRns = Objects.requireNonNull(copy, "null copy").hashRns;
    this.inputRns = copy.inputRns;
    this.hashes = copy.hashes;
    this.inputs = copy.inputs;
  }
  



  @Override
  protected ByteBuffer refOnlyHash(long rowNumber) {
    int index = Collections.binarySearch(hashRns, rowNumber);
    return index < 0 ? null : emit(hashes, index);
  }

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    if (rowNumber <= 0)
      throw new IllegalArgumentException("rowNumber: " + rowNumber);

    int index = Collections.binarySearch(inputRns, rowNumber);
    if (index < 0)
      throw new IllegalArgumentException("no data for rowNumber " + rowNumber);
    
    return emit(inputs, index);
  }
  
  
  @Override
  public List<Long> getFullRowNumbers() {
    return inputRns;
  }
  
  
  
  private ByteBuffer emit(ByteBuffer store, int index) {
    int offset = index * HASH_WIDTH;
    return store.asReadOnlyBuffer().position(offset).limit(offset + HASH_WIDTH).slice();
  }
}
