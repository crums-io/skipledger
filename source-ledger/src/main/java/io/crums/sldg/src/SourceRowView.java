/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.crums.util.collections.Lists;


/**
 * A view of a {@code SourceRow}. 
 * 
 * <h2>Motivation (what problem it solves)</h2>
 * <p>
 * This class exists in order to support external tools (such as Qute) where
 * some functions (Qute's value-handlers being the motivating case) need to
 * name concrete implementation classes in order to work.
 * </p><p>
 * By design, {@linkplain SourceRow} is an abstract type. So is {@linkplain Cell}.
 * This class uses a lazy decorator pattern to provide fix the problem.
 * </p>
 * 
 * 
 */
public final class SourceRowView extends SourceRow {

  /**
   * Returns a view of the given {@code row}. If the given argument is
   * already a view, then the argument is returned as-is.
   * 
   * @see #mapUnchecked(SourceRow)
   */
  public static SourceRowView map(SourceRow row) {
    return row instanceof SourceRowView view ? view : new SourceRowView(row);
  }


  /**
   * Returns a view of the given {@code row}.
   * This method <em>does not guard against inadvertent nesting.</em>
   * 
   * @param row not an instance of this class
   * @see #map(SourceRow)
   */
  public static SourceRowView mapUnchecked(SourceRow row) {
    return new SourceRowView(row);
  }



  /**
   * Decorator view of a cell. Note, cell-instances should be considered
   * <em>value objects</em>; owing to how instances are lazily constructed,
   * they <em>lack identity semantics</em>. 
   */
  public static class CellView extends Cell {

    private final Cell base;

    private CellView(Cell base) {
      this.base = base;
    }

    @Override
    public ByteBuffer hash(MessageDigest digest) {
      return base.hash(digest);
    }

    @Override
    public DataType dataType() {
      return base.dataType();
    }

    @Override
    public boolean hasData() {
      return base.hasData();
    }

    @Override
    public int dataSize() {
      return base.dataSize();
    }

    @Override
    public ByteBuffer data() throws NoSuchElementException {
      return base.data();
    }

    @Override
    public boolean hasSalt() {
      return base.hasSalt();
    }

    @Override
    public ByteBuffer salt() {
      return base.salt();
    }

  }


  private final SourceRow baseRow;


  private SourceRowView(SourceRow baseRow) {
    this.baseRow = Objects.requireNonNull(baseRow);
  }

  @Override
  public long no() {
    return baseRow.no();
  }


  /**
   * 
   * @return a list of {@linkplain CellView}s.
   */
  @Override
  public List<Cell> cells() {
    return Lists.map(baseRow.cells(), CellView::new);
  }

    

}
