/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import java.util.Objects;

import io.crums.model.CrumTrail;

/**
 * 
 */
public class CrumedPath {
  
  
  private final Path path;
  private final CrumTrail trail;

  /**
   * 
   */
  public CrumedPath(Path path, CrumTrail trail) {
    this.path = Objects.requireNonNull(path, "null path");
    this.trail = Objects.requireNonNull(trail, "null trail");
//    if (trail.crum().hash().equals(path.first().))
  }
  
  
  
  public Path path() {
    return path;
  }
  
  
  public CrumTrail trail() {
    return trail;
  }

}
