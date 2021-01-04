/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.util.Collections;
import java.util.List;

import io.crums.util.Tuple;

/**
 * A path whose target row is not necessarily the first row.
 * 
 * @see #target()
 */
public class TargetPath extends Path {
  
  
  protected final int targetIndex;

  public TargetPath(List<Row> path, long targetRowNumber) {
    super(path);
    this.targetIndex = targetIndex(targetRowNumber);
  }

  public TargetPath(List<Row> path, List<Tuple<Long, Long>> beacons, long targetRowNumber) {
    super(path, beacons);
    this.targetIndex = targetIndex(targetRowNumber);
  }

  public TargetPath(Path path, List<Tuple<Long, Long>> beacons) {
    this(path, beacons, path.target().rowNumber());
  }

  public TargetPath(Path path, List<Tuple<Long, Long>> beacons, long targetRowNumber) {
    super(path, beacons);
    this.targetIndex = targetIndex(targetRowNumber);
  }

  public TargetPath(Path path, long targetRowNumber) {
    super(path);
    this.targetIndex = targetIndex(targetRowNumber);
  }

  /**
   * Package-private constructor does not make copies of argument. Verifies still. Used by
   * {@linkplain Path#load(java.nio.ByteBuffer)}.
   * 
   * @param path
   * @param beacons
   * @param ignore
   */
  TargetPath(List<Row> path, List<Tuple<Long, Long>> beacons, long targetRowNumber, boolean ignore) {
    super(path, beacons, ignore);
    this.targetIndex = targetIndex(targetRowNumber);
  }

  /**
   * Copy constructor.
   */
  protected TargetPath(TargetPath copy) {
    super(copy);
    this.targetIndex = copy.targetIndex;
  }
  

  
  
  private int targetIndex(long targetRowNumber) {
    int index = Collections.binarySearch(rowNumbers(), targetRowNumber);
    if (index < 0)
      throw new IllegalArgumentException(
          "targetRowNumber " + targetRowNumber + " not a member of " + rowNumbers());
    return index;
  }
  
  
  
  
  /**
   * <p>The target row in this class is usually <em>not</em> the {@linkplain #first() first} row.</p>
   * {@inheritDoc}
   */
  @Override
  public Row target() {
    return rows().get(targetIndex);
  }

  

}














