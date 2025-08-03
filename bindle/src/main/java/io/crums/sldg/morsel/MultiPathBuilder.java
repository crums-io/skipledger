/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.Row;

/**
 * {@linkplain MultiPath} builder.
 * 
 * @see #addPath(Path)
 */
public class MultiPathBuilder {
  
  private final List<Path> paths = new ArrayList<>();
  
  
  

  /**
   * Creates a new intance with the given starting path.
   */
  public MultiPathBuilder(Path path) {
    Objects.requireNonNull(path, "path");
    paths.add(path);
  }
  
  
  public MultiPathBuilder(MultiPath init) {
    paths.addAll(init.paths());
  }
  
  
  
  public final Path[] paths() {
    return paths.toArray(new Path[count()]);
  }
  
  
  /** Returns the number of paths in this instance. */
  public final int count() {
    return paths.size();
  }
  
  
  
  /**
   * Adds the given <em>intersecting</em> {@code path} and returns
   * the highest row number at which the added path intersects with
   * the other paths in the collection.
   * <p>
   * Note this class does not do anything fancy. A subclass, for
   * example, may check for duplicate information and crop the path
   * before adding.
   * </p>
   * 
   * @param path from the same ledger as the other paths in this instance
   * @return {@code highestCommonNo(path)} (not 0)
   * @throws HashConflictException
   *            if {@code path} is from a different ledger than the one
   *            represented in this collection
   * @throws IllegalArgumentException
   *            if {@code highestCommonNo(path)} returns zero, or if the
   *            path is already contained in the collection
   * 
   * @see #highestCommonNo(Path)
   */
  public long addPath(Path path)
      throws HashConflictException, IllegalArgumentException {
    
    long hcn = highestCommonNo(path);
    if (hcn == 0L)
      throw new IllegalArgumentException(
          path + " does not intersect with collection");
    
    if (hcn == path.hi() && paths.contains(path))
      throw new IllegalArgumentException(
          path + " already contained in collection");
    
    paths.add(path);
    return hcn;
  }
  
  
  /**
   * Returns the highest row number in the given {@code path}
   * and this collection whose row-hash is known. The given path
   * <em>must be</em> from the same ledger, otherwise, the method will
   * throw. If the path does not intersect with the collection,
   * then zero is returned.
   * 
   * @param path  from the same ledger
   *
   * @throws HashConflictException
   *            if {@code path} is from a different ledger than the one
   *            represented in this collection
   */
  public final long highestCommonNo(Path path) throws HashConflictException {
    
    return MultiPath.highestCommonNo(paths, path);
  }
  
  
  /**
   * Returns the highest row number known to the collection.
   * 
   * @return &ge; {@code lo()}
   * @see #lo()
   */
  public final long hi() {
    return MultiPath.hi(paths);
  }
  

  /**
   * Returns the lowest (full) row number known to the collection.
   * 
   * @return &ge; 1
   * @see #hi()
   */
  public final long lo() {
    return MultiPath.lo(paths);
  }
  
  
  /**
   * Returns {@code true} iff the <em>input</em>-hash of the row at the
   * given number is known by this collection.
   */
  public final boolean hasRow(long rowNo) {
    int index = paths.size();
    while (index-- > 0 && !paths.get(index).hasRow(rowNo));
    return index != -1;
  }
  
  
  public final Optional<Row> findRow(long rowNo) {
    return MultiPath.findRow(paths, rowNo);
  }
  
  public final ByteBuffer rowHash(long rowNo) {
    return MultiPath.rowHash(paths, rowNo);
  }
  
  public final MultiPath toMultiPath() {
    return new MultiPath(this);
  }
  
  

}

















