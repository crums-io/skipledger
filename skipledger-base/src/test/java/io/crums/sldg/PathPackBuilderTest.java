/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * 
 */
public class PathPackBuilderTest extends RowBagTest {

  @Override
  protected PathPackBuilder newBag(SkipLedger ledger, List<Long> rowNumbers) {
    var builder = new PathPackBuilder();
    var path = ledger.getPath(rowNumbers);
    assertEquals(rowNumbers, path.rowNumbers());
    builder.addPath(path);
    return builder;
  }

}
