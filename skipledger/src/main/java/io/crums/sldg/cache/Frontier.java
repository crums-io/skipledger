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
 * <h2>Model</h2>
 * <p>
 * Information in a frontier is broken into levels.
 * Instead of a normative spec, let's describe this by example.
 * Suppose the frontier is at row number {@code 101101} (in binary digits). An instance
 * of this class for this row number would have as many levels as there are significant
 * digits in the row number (i.e. as written here, or more concretely, 1 + high-bit-position
 * of its 64-bit reprsentation). In our example our frontier has 6 levels.
 * </p><p><pre><b>
 *    Level  Row-number</b><code>
 *    
 *      0       101101
 *      1       101100
 *      2       101100
 *      3       101000
 *      4       101000
 *      5       100000
 * 
 * </code></pre></p><p>
 * Each of the row numbers at a these levels represents the row with which a next row
 * will connect to, if that next row has a [hash] pointer connecting at that level to a
 * previous row.
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
   * @return the equivalent of {@code frontierRowNumbers().size()}
   */
  public int levelCount() {
    return levelRowNumbers().size();
  }
  

  /**
   * Returns the highest row number in the frontier. This just the row number at level zero.
   * 
   * @return the equivalent of {@code levelRowNumbers().get(0)}
   */
  public long rowNumber() {
    return levelRowNumbers().get(0);
  }
  

  /**
   * Returns the lowest row number in the frontier.
   * 
   * @return the equivalent of {@code levelRowNumbers().get(levelCount() - 1)}
   */
  public long tail() {
    return levelRowNumbers().get(levelCount() - 1);
  }
  

  
  /**
   * Returns an instance of this class for the given row number.
   * @param frontier
   * @return
   */
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
    public final long rowNumber() {
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
