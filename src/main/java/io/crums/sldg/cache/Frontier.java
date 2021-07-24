/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import java.util.List;
import java.util.Objects;

import io.crums.sldg.SkipLedger;
import io.crums.util.Lists;

/**
 * Prototype class documenting the frontier rows in a {@linkplain SkipLedger}.
 * This class deals only with the row numbers involved. It's here only to reduce
 * cognitive load: it may be merged into its subclass at a future date.
 * <p>
 * An instance models the <em>minimum</em> information necessary to construct the next row
 * in a {@linkplain SkipLedger} <em>and every row after</em>.
 * </p>
 * 
 * @see HashFrontier
 */
public abstract class Frontier {
  
  
  /**
   * Returns the number of levels for the given frontier row number.
   * <br/><em>Arguments are not checked. Behavior is undefined outside expected bounds.</em>
   * 
   * @param frontierRn  the frontier row number
   * 
   * @return {@code 64 - Long.numberOfLeadingZeros(frontierRn)}
   */
  public static int levelCount(long frontierRn) {
    return 64 - Long.numberOfLeadingZeros(frontierRn);
  }
  
  
  /**
   * Returns the row number at the given level and frontier row number.
   * <br/><em>Arguments are not checked. Behavior is undefined outside expected bounds.</em>
   * 
   * @param frontierRn  the frontier row number
   * @param level       &ge; 0 and &lt; {@code levelCount(frontierRn)}
   * 
   * @return {@code (frontierRn >>> level) << level}
   * 
   * @see #levelCount(long)
   */
  public static long levelRowNumber(long frontierRn, int level) {
    return (frontierRn >>> level) << level;
  }
  
  
  /**
   * Returns the frontier's level row numbers.
   * 
   * @return non-empty, descending (buy not necessarily monotonic) list of positive row numbers
   */
  public abstract List<Long> levelRowNumbers();
  
  
  /**
   * Returns the number of existing levels for this instance.
   * 
   * @return {@code frontierRowNumbers().size()}
   */
  public int levelCount() {
    return levelRowNumbers().size();
  }
  

  /**
   * Returns the highest row number in the frontier. This just the row number at level zero.
   * 
   * @return {@code levelRowNumbers().get(0)}
   */
  public long lead() {
    return levelRowNumbers().get(0);
  }
  

  /**
   * Returns the lowest row number in the frontier.
   * 
   * @return {@code levelRowNumbers().get(levelCount() - 1)}
   */
  public long tail() {
    return levelRowNumbers().get(levelCount() - 1);
  }
  

  
  
  public static Frontier newFrontier(long frontier) {
    return new Numbers(frontier);
  }
  
  
  /**
   * This base-class implementation is lazy but memory efficient.
   */
  public static class Numbers extends Frontier {
    
    private final long frontierRn;
    
    /**
     * 
     * @param frontierRn the last row number
     */
    public Numbers(long frontierRn) {
      this.frontierRn = frontierRn;
      
      if (frontierRn < 1)
        throw new IllegalArgumentException("frontier row number: " + frontierRn);
    }

    @Override
    public List<Long> levelRowNumbers() {
      return new Lists.RandomAccessList<Long>() {

        @Override
        public Long get(int index) {
          Objects.checkIndex(index, levelCount());
          return levelRowNumber(index);
        }

        @Override
        public int size() {
          return levelCount();
        }
      };
    }
    
    
    @Override
    public final long lead() {
      return frontierRn;
    }
    
    
    @Override
    public final long tail() {
      return levelRowNumber(levelCount() - 1);
    }
    
    
    private long levelRowNumber(int level) {
      return levelRowNumber(frontierRn, level);
    }
    
    
    @Override
    public final int levelCount() {
      return levelCount(frontierRn);
    }
    
    
    public String toString() {
      return levelRowNumbers().toString();
    }
    
  }

}
