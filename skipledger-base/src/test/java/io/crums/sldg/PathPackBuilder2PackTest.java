/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

/**
 * Since the builder accesses the package-private, no-check constructor
 * {@linkplain PathPack#PathPack(List, java.nio.ByteBuffer, java.nio.ByteBuffer)}
 * we must test it.
 */
public class PathPackBuilder2PackTest extends RowBagTest {
  

  @Override
  protected PathPack newBag(SkipLedger ledger, List<Long> rowNumbers) {
    var builder = new PathPackBuilder();
    var path = ledger.getPath(rowNumbers);
    assertEquals(rowNumbers, path.rowNumbers());
    builder.addPath(path);
    return builder.toPack();
  }

}
