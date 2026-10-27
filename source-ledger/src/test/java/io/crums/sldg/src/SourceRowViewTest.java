/*
 * Copyright 2026 Babak Farhang
 */
package io.crums.sldg.src;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SourceRowView.AccessListener}: it must fire once per
 * {@code Cell} element actually accessed off {@link SourceRowView#cells()},
 * not once per {@code cells()} call.
 */
public class SourceRowViewTest {

  @Test
  public void testAccessListener_firesOncePerCellAccessed() {
    SourceRowBuilder builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(1L, "a", "b", "c");

    AtomicInteger accesses = new AtomicInteger();
    SourceRowView view = SourceRowView.map(row, accesses::incrementAndGet);

    var cells = view.cells(); // obtaining the list itself is not an access
    assertEquals(0, accesses.get());

    cells.get(0);
    assertEquals(1, accesses.get());
    cells.get(1);
    cells.get(2);
    assertEquals(3, accesses.get());

    // re-fetching the same index re-triggers the listener (lazy, not cached)
    cells.get(0);
    assertEquals(4, accesses.get());
  }

  @Test
  public void testNoListener_behavesExactlyAsPlainMap() {
    SourceRowBuilder builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(1L, "x");

    SourceRowView view = SourceRowView.map(row);
    assertEquals("x", view.cells().get(0).value());
  }

  @Test
  public void testListenerException_propagatesFromCellAccess() {
    SourceRowBuilder builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(1L, "x");

    SourceRowView view = SourceRowView.map(row, () -> {
      throw new IllegalStateException("stop");
    });

    assertThrows(IllegalStateException.class, () -> view.cells().get(0));
  }

  @Test
  public void testNullListenerRejected() {
    SourceRowBuilder builder = new SourceRowBuilder();
    SourceRow row = builder.buildRow(1L, "x");
    assertThrows(NullPointerException.class, () -> SourceRowView.map(row, null));
  }
}
