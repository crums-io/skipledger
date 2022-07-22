/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.pred;


import java.util.Optional;
import java.util.function.Predicate;

import io.crums.sldg.src.ColumnType;
import io.crums.sldg.src.ColumnValue;

/**
 * Implementation interface for cell value (column value in SQL parlance) predicates.
 * Used as a constraint to define a selection of
 * {@linkplain io.crums.sldg.src.SourceRow SourceRow}s.
 * 
 * <h3>About Numbers</h3>
 * <p>
 * {@linkplain Number} arguments here work with any of the autoboxed <em>primitives</em>,
 * even when mixed. <em>They do not work arbitrary precision types such as
 * {@linkplain java.math.BigInteger BigInteger}.</em>
 * See {@linkplain io.crums.util.PrimitiveComparator} for details.
 * </p>
 */
public sealed interface ColumnValuePredicate extends Predicate<ColumnValue>
    permits
    NumberPredicate,
//    CellPredicate.Equals,
    NotCellPredicate {

  /** Returns a &gt; {@code rhs} predicate. */
  public static ColumnValuePredicate greaterThan(Number rhs) {
    return new Greater(rhs);
  }


  /** Returns a &ge; {@code rhs} predicate. */
  public static ColumnValuePredicate greaterThanOrEqualTo(Number rhs) {
    return new GreaterOrEqual(rhs);
  }

  /** Returns a &lt; {@code rhs} predicate. */
  public static ColumnValuePredicate lessThan(Number rhs) {
    return new Lesser(rhs);
  }

  /** Returns a &le; {@code rhs} predicate. */
  public static ColumnValuePredicate lessThanOrEqualTo(Number rhs) {
    return new LesserOrEqual(rhs);
  }

  /** Returns an = {@code rhs} predicate. */
  public static ColumnValuePredicate equalTo(Number rhs) {
    return new NumberEquals(rhs);
  }

  /** Returns an = {@code rhs} predicate. */
  public static ColumnValuePredicate notEqualTo(Number rhs) {
    return new NotNumberEquals(rhs);
  }
  
  
  
  
  
  
  /**
   * @param cell not {@code null}
   * 
   * @return {@code acceptType(cell.getType()) && acceptValue(cell.getValue())}
   * 
   * @see #acceptType(ColumnType)
   * @see #acceptValue(Object)
   */
  @Override
  default boolean test(ColumnValue cell) {
    return acceptType(cell.getType()) && acceptValue(cell.getValue());
  }
  
  
  /**
   * Does the predicate <em>accept</em> this {@code type}?
   * This is the first post to pass.
   * 
   * @param type not null
   */
  boolean acceptType(ColumnType type);
  
  /**
   * Does the predicate accept this {@code value}?
   * This is the second (and by default final) post to pass.
   * 
   * @param value may be {@code null} (!)
   */
  boolean acceptValue(Object value);
  
  
  

  /**
   *  Returns the RHS (the value the predicate compares values with), if present.
   *  Only leaf predicates have a RHS.
   *  
   *  @return empty, by default
   */
  default Optional<?> rhs()  {
    return Optional.empty();
  }
  
  
  // Not used. Presently
  
//  /**
//   * Strict value equality predicate. To be equal, the objects
//   * must be of the same type.
//   * 
//   * @param expected the expected value (may be {@code null})
//   */
//  public record Equals(Object expected) implements CellPredicate {
//    /** @return {@code true} */
//    @Override public boolean acceptType(ColumnType type) { return true; }
//    /** @return {@code Objects.equals(expected, value)} */
//    @Override public boolean acceptValue(Object value) {
//      return Objects.equals(expected, value);
//    }
//  }
  
  
  public final class Greater extends NumberPredicate {
    /** @param rhs non-null boxed primitive */
    public Greater(Number rhs) {
      super(rhs, 1);
    }
  }
  
  
  public final class Lesser extends NumberPredicate {
    /** @param rhs non-null boxed primitive */
    public Lesser(Number rhs) {
      super(rhs, -1);
    }
  }
  
  
  
  
  public final class GreaterOrEqual extends NotCellPredicate {

    /** @param rhs non-null boxed primitive */
    public GreaterOrEqual(Number rhs) {
      super(new Lesser(rhs));
    }
    
  }
  
  
  public final class LesserOrEqual extends NotCellPredicate {

    /** @param rhs non-null boxed primitive */
    public LesserOrEqual(Number rhs) {
      super(new Greater(rhs));
    }
  }
  
  
  public final class NumberEquals extends NumberPredicate {
    /** @param rhs non-null boxed primitive */
    public NumberEquals(Number rhs) {
      super(rhs, 0);
    }
  }
  

  /** @param rhs non-null boxed primitive */
  public final class NotNumberEquals extends NotCellPredicate {
    public NotNumberEquals(Number rhs) {
      super(new NumberEquals(rhs));
    }
  }

}








