/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;

import io.crums.util.Tuple;


/**
 * A {@linkplain Path path} in V-form (the shape the list of paths looks like if
 * printing a line per row). This is the shortest-path layout.
 * 
 * @see #isSkipPath()
 */
public class SkipPath extends Path {

  
  
  /**
   * Creates a new instance with no beacons.
   */
  public SkipPath(List<Row> path) {
    super(path);
    verify();
  }
  
  /**
   * Creates a new instance. The arguments are defensively copied.
   * 
   * @param path the shortest possible list of {@linkplain Row row}s in ascending
   *             row numbers,  each row linked to from the next via one of
   *             that next row's hash pointers
   * 
   * @see #MAX_ROWS
   */
  public SkipPath(List<Row> path, List<Tuple<Long,Long>> beacons) {
    super(path, beacons);
    verify();
  }

  /**
   * Package-private.
   */
  SkipPath(List<Row> path, List<Tuple<Long,Long>> beacons, boolean ignored) {
    super(path, beacons, ignored);
    verify();
  }
  
  /**
   * Constructor promotes a {@linkplain Path path} to an instance of
   * this class.
   * 
   * @param skipPath  skipPath.{@linkplain Path#isSkipPath() isSkipPath()} returns <tt>true</tt>
   */
  public SkipPath(Path skipPath) {
    super(skipPath);
    verify();
  }
  
  /**
   * Copy constructor.
   */
  protected SkipPath(SkipPath copy) {
    super(copy);
  }
  
  
  
  
  
  
  
  /**
   * Creates a new instance with the given copy, and sets its beacon rows.
   */
  public SkipPath(SkipPath vanilla, List<Tuple<Long, Long>> beacons) {
    super(vanilla, beacons);
  }
  
  

  private void verify() {
    if (!super.isSkipPath())
      throw new IllegalArgumentException("not a skip path: " + rows);
  }

}











