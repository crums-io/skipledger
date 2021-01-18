/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.NoSuchElementException;

/**
 * 
 */
public class UniquePairIntersector extends PathIntersector {
  
  
  private RowIntersection lookAhead;

  /**
   * Creates a new instance.
   * 
   * @param a
   * @param b
   */
  public UniquePairIntersector(Path a, Path b) {
    super(a, b);
    if (super.hasNext())
      lookAhead = super.next();
  }

  /**
   * Copy constructor restarts the iteration at the beginning.
   */
  public UniquePairIntersector(PathIntersector copy) {
    super(copy);
    if (super.hasNext())
      lookAhead = super.next();
  }
  

  @Override
  public boolean hasNext() {
    return lookAhead != null;
  }

  @Override
  public RowIntersection next() {
    if (lookAhead == null)
      throw new NoSuchElementException();
    
    RowIntersection out = lookAhead;
    lookAhead = null;
    while (super.hasNext()) {
      lookAhead = super.next();
      if (out.sameSourceNumbers(lookAhead)) {
        out = lookAhead;
        lookAhead = null;
        continue;
      }
      break;
    }
    return out;
  }
  
  

  @Override
  public UniquePairIntersector iterator() {
    return new UniquePairIntersector(this);
  }

}
