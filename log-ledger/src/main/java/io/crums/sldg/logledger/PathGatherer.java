/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.crums.sldg.Path;
import io.crums.sldg.RowBag;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.logledger.Hasher.RowHashListener;


/**
 * {@linkplain RowHashListener} for gathering information for
 * constructing arbitrary skipledger {@linkplain Path}s from a log.
 * 
 * <h2>TODO: Building State Paths on the Fly</h2>
 * <p>
 * Presently, in order to construct a <em>state</em> path using this
 * class (a path threading the first row to the last), you must
 * already know how many rows the log has. With a bit of work (thought),
 * it should be possible to efficiently assemble the state path on
 * the fly. (It must involve adding and pruning the internal maps in
 * order to keep memory manageable.) Punting, for now.
 * </p>
 */
public class PathGatherer implements RowHashListener {

  // we don't need an ordered map.. HashMap is fastest
  private final HashMap<Long, ByteBuffer> inputs = new HashMap<>();
  private final HashMap<Long, ByteBuffer> rowHashes = new HashMap<>();
  
  private final RowBag bag = new RowBag() {
    
    @Override
    public ByteBuffer rowHash(long rowNo) {
      if (rowNo == 0L)
        return SldgConstants.DIGEST.sentinelHash();
      var hash = rowHashes.get(rowNo);
      if (hash == null)
        throw new IllegalArgumentException(
            "row [%d] not covered".formatted(rowNo));
      return hash.asReadOnlyBuffer().clear();
    }
    
    @Override
    public ByteBuffer inputHash(long rowNo) {
      var input = inputs.get(rowNo);
      if (input == null)
        throw new IllegalArgumentException(
            "row [%d] is not a full row".formatted(rowNo));
      return input.asReadOnlyBuffer().clear();
    }
    
    @Override
    public List<Long> getFullRowNumbers() {
      return pathNos;
    }
  };
  
  
  private final List<Long> pathNos;
  
  private final Set<Long> fullRowNos;
  private final Set<Long> refOnlyNos;
  
  

  /**
   * 
   */
  public PathGatherer(Collection<Long> stitchNos) {
    this.pathNos = SkipLedger.stitchCollection(stitchNos);
    this.fullRowNos = new HashSet<>(pathNos);
    
    assert fullRowNos.size() == pathNos.size();
    
    this.refOnlyNos = new HashSet<>(SkipLedger.refOnlyCoverage(pathNos));
  }

  @Override
  public void rowHashParsed(
      ByteBuffer inputHash, HashFrontier frontier, HashFrontier prevFrontier) {
    
    final Long rowNo = frontier.rowNo();
    
    assert prevFrontier.nextFrontier(inputHash.duplicate()).equals(frontier);
    
    if (fullRowNos.contains(rowNo)) {
      if (inputs.isEmpty()) {
        for (int level = SkipLedger.skipCount(rowNo); level-- > 0; )
          rowHashes.put(rowNo - (1L << level), prevFrontier.levelHash(level));
      }
      inputs.put(rowNo, inputHash);
      rowHashes.put(rowNo, frontier.frontierHash());
    } else if (refOnlyNos.contains(rowNo))
      rowHashes.put(rowNo, frontier.frontierHash());
    
    
  }
  
  
  public Path getPath() throws IllegalStateException {
    if (!inputs.containsKey(pathNos.getFirst()))
      throw new IllegalStateException("not parsed");
    if (!inputs.containsKey(pathNos.getLast()))
      throw new IllegalStateException("parse not completed");
    
    return bag.getPath(pathNos);
  }
  

}
