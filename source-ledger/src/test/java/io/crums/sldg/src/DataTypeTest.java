/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class DataTypeTest {
  
  /** Honestly, this is a test of my understanding of enums. */
  @Test
  public void testForOrdinal() {
    for (var type : DataType.values())
      assertEquals(type, DataType.forOrdinal(type.ordinal()));
  }

}





