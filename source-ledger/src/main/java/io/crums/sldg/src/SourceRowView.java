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
   * An optional callback, invoked once per {@code Cell} actually
   * constructed from {@linkplain #cells()}'s lazily-mapped list (i.e. once
   * per element access, not once per {@code cells()} call) -- a generic
   * access hook with no opinion on what a caller does with it. See
   * {@link #map(SourceRow, AccessListener)}.
   */
  @FunctionalInterface
  public interface AccessListener {
    void onAccess();
  }

  /**
   * Returns a view of the given {@code row}. If the given argument is
   * already a view, then the argument is returned as-is.
   *
   * @see #mapUnchecked(SourceRow)
   */
  public static SourceRowView map(SourceRow row) {
    return row instanceof SourceRowView view ? view : new SourceRowView(row, null);
  }

  /**
   * Returns an access-monitored view of the given {@code row}: {@code
   * listener} is invoked once per {@code Cell} element actually accessed
   * off {@link #cells()}'s returned list. Unlike {@link #map(SourceRow)},
   * always wraps fresh (no identity shortcut for an already-wrapped {@code
   * row}), so the listener is never silently dropped.
   */
  public static SourceRowView map(SourceRow row, AccessListener listener) {
    return new SourceRowView(row, Objects.requireNonNull(listener, "listener"));
  }


  /**
   * Returns a view of the given {@code row}.
   * This method <em>does not guard against inadvertent nesting.</em>
   * 
   * @param row not an instance of this class
   * @see #map(SourceRow)
   */
  public static SourceRowView mapUnchecked(SourceRow row) {
    return new SourceRowView(row, null);
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
  private final AccessListener listener;


  private SourceRowView(SourceRow baseRow, AccessListener listener) {
    this.baseRow = Objects.requireNonNull(baseRow);
    this.listener = listener;
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
    return listener == null ?
        Lists.map(baseRow.cells(), CellView::new) :
          Lists.map(baseRow.cells(), this::accessedView);
  }

  private CellView accessedView(Cell cell) {
    listener.onAccess();
    return new CellView(cell);
  }

    

}
