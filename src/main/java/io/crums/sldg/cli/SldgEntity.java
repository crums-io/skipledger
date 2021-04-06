/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;

import java.util.Objects;

import io.crums.sldg.Path;
import io.crums.sldg.scraps.Nugget;

/**
 * 
 */
final class SldgEntity {
  
  private final Path path;
  private final Nugget nugget;
  
  
  public SldgEntity(Path path) {
    this(Objects.requireNonNull(path, "null path"), null);
  }
  
  
  public SldgEntity(Nugget nugget) {
    this(null, Objects.requireNonNull(nugget, "null nugget"));
  }

  /**
   * 
   */
  private SldgEntity(Path path, Nugget nugget) {
    this.path = path;
    this.nugget = nugget;
  }
  
  
  
  public boolean hasPath() {
    return path != null;
  }
  
  
  public boolean hasNugget() {
    return nugget != null;
  }
  
  
  public Path getPath() {
    return path;
  }
  
  
  public Nugget getNugget() {
    return nugget;
  }
  
  
  public Object getObject() {
    return path == null ? nugget : path;
  }

}
