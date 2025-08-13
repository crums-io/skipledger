/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;


import static io.crums.sldg.src.SharedConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.TableSalt;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.Strings;

/**
 * This actually tests both the builder and some base class methods of
 * {@code SourceBuilder}.
 */
public class SourceRowBuilderTest extends SelfAwareTestCase {
 
  
  @Test
  public void testUnsaltedSingleCellNull() {
    
    var builder = new SourceRowBuilder();
    
    assertFalse(builder.saltScheme().hasSalt());
    
    final long no = 1L;
    
    var row = builder.buildRow(no, (Object) null);
    // verify instance Object.equals
    assertEquals(row, builder.buildRow(no, (Object) null));
    assertEquals(row, builder.buildRow(
        no, List.of(DataType.NULL), Arrays.asList(new Object[1]))); // other api method
    
    assertEquals(no, row.no());
    
    assertSingleNullRow(row, false);
  }
  
  
  @Test
  public void testSaltedSingleCellNull() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);

    
    assertTrue(builder.saltScheme().hasSalt());
    
    final long no = 2L;
    
    var row = builder.buildRow(no, (Object) null);
    assertEquals(row, builder.buildRow(no, (Object) null));
    assertEquals(row, builder.buildRow(
        no, List.of(DataType.NULL), Arrays.asList(new Object[1])));
    assertEquals(no, row.no());
    
    assertSingleNullRow(row, true);
  }
  
  
  @Test
  public void testUnsaltedSingleCellString() {
    var builder = new SourceRowBuilder();
    final long no = 3L;
    final String value = "there's no value";
    var row = builder.buildRow(no, value);
    assertEquals(row, builder.buildRow(no, value));
    assertEquals(row, builder.buildRow(no, List.of(DataType.STRING), List.of(value)));
    assertEquals(no, row.no());
    assertUnsaltedSingleCellRow(row, DataType.STRING);
    assertFalse(row.cells().get(0).hasSalt());
  }
  
  
  @Test
  public void testSaltedSingleCellString() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    final long no = 4L;
    final String value = "untokenized string value";
    var row = builder.buildRow(no, value);
    assertEquals(row, builder.buildRow(no, value));
    assertEquals(row, builder.buildRow(no, List.of(DataType.STRING), List.of(value)));
    assertEquals(no, row.no());
    
    assertSaltedSingleCellRow(row, DataType.STRING);
  }
  
  
  @Test
  public void testUnsaltedSingleLong() {
    var builder = new SourceRowBuilder();
    final long no = 5L;
    final long value = 42L;
    var row = builder.buildRow(no, value);
    assertEquals(row, builder.buildRow(no, value));
    assertEquals(no, row.no());
    
    assertSingleLong(row, value, DataType.LONG, false);
    
  }
  
  
  @Test
  public void testSaltedSingleLong() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 6L;
    final long value = 42L;
    var row = builder.buildRow(no, value);
    assertEquals(row, builder.buildRow(no, value));
    assertEquals(no, row.no());
    
    assertSingleLong(row, value, DataType.LONG, true);
    
  }
  
  
  @Test
  public void testSaltedSingleInt() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 7L;
    final int value = 42;
    var row = builder.buildRow(no, value);
    assertEquals(row, builder.buildRow(no, value));
    assertEquals(no, row.no());
    
    assertSingleLong(row, value, DataType.LONG, true);
    
  }
  

  
  
  @Test
  public void testSaltedSingleDate() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 8L;
    
    final Date date = new Date();
    var row = builder.buildRow(no, date);
    assertEquals(row, builder.buildRow(no, date));
    assertEquals(no, row.no());
    
    assertSingleLong(row, date.getTime(), DataType.DATE, true);
    
  }
  
  

  

  
  
  @Test
  public void testSaltedSingleDateViaListApi() {
    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 9L;
    
    final Date date = new Date();
    var row = builder.buildRow(no, List.of(DataType.DATE), List.of(date));
    assertEquals(row, builder.buildRow(no, date));
    assertEquals(row, builder.buildRow(no, List.of(DataType.DATE), List.of(date.getTime())));
    assertEquals(no, row.no());
    
    assertSingleLong(row, date.getTime(), DataType.DATE, true);
    
  }
  
  
  
  
  @Test
  public void testIllegalFloatingPoint() {

    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 10L;
    final double value = 42.3;
    try {
      builder.buildRow(no, value);
      fail();
    } catch (IllegalArgumentException expected) {
      System.out.println(method(label) + " [EXPECTED]: " + expected);
    }
  }
  
  
  
  @Test
  public void testSaltedNumString() {

    final Object label = new Object() {  };
    
    var tableSalt = testShaker(label);
    var builder = new SourceRowBuilder(SaltScheme.SALT_ALL, tableSalt);
    
    final long no = 11L;
    
    final int id = 99;
    final String alias = "smarter than max";
    
    var row = builder.buildRow(no, id, alias);
    assertEquals(
        row,
        builder.buildRow(no, List.of(DataType.LONG, DataType.STRING), List.of(id, alias)));
    
    assertValues(row, List.of(DataType.LONG, DataType.STRING), List.of(id, alias), true);
    
  }
  
  
  
  
  private void assertValues(
      SourceRow row, List<DataType> expTypes, List<?> expValues, boolean salted) {
    
    assertEquals(expTypes.size(), row.cells().size());
    assertEquals(expTypes, row.cellTypes());
    
    var digest = DIGEST.newDigest();
    
    for (int index = 0; index < expTypes.size(); ++index) {
      Cell cell = row.cells().get(index);
      Object expected = expValues.get(index);
      final var type = expTypes.get(index);
      switch (type) {
      case STRING:
        assertEquals(expected.toString(), Strings.utf8String(cell.data()));
        break;
      case LONG:
        assertEquals(((Number) expected).longValue(), cell.data().getLong());
        assertEquals(8, cell.data().remaining());
        break;
      case DATE:
        assertEquals(8, cell.data().remaining());
        {
          long expValue =
              expected instanceof Date d ? d.getTime() :
                ((Number) expected).longValue();
          assertEquals(expValue, cell.data().getLong());
        }
        break;
      case NULL:
        assertEquals(1, cell.data().remaining());
        assertEquals(0, cell.data().get());
        break;
      case HASH:
        assertEquals(HASH_WIDTH, cell.data().remaining());
        // fall thru..
      case BYTES:
        assertEquals(expected, cell.data());
        break;
      default:
        fail();
      }
      
      assertEquals(salted, cell.hasSalt());
      
      if (salted)
        assertSaltedHash(cell, row.rowSalt().get(), index, digest);
      
    }
  }
  
  
  
  private void assertSaltedHash(Cell cell, ByteBuffer rowSalt, int index, MessageDigest digest) {
    var salt = cell.salt();
    assertEquals(HASH_WIDTH, salt.remaining());
    digest.reset();
    digest.update(salt);
    digest.update(cell.data());
    assertEquals(ByteBuffer.wrap(digest.digest()), cell.hash());

    assertEquals(
        ByteBuffer.wrap(TableSalt.cellSalt(rowSalt, index, digest)),
        cell.salt());
  }
  
  
  
  
  private void assertSingleLong(
      SourceRow row, long expectedValue, DataType subType, boolean salted) {
    
    assertSingleCellRow(row, subType, salted);
    assertEquals(expectedValue, row.cells().get(0).data().getLong());
    
  }
  
  
  private void assertSingleNullRow(SourceRow row, boolean salted) {
    assertSingleCellRow(row, DataType.NULL, salted);
    assertEquals(0, row.cells().get(0).data().get());
  }
  
  
  private void assertSaltedSingleCellRow(SourceRow row, DataType type) {
    assertSingleCellRow(row, type, true);
  }
  
  
  private void assertUnsaltedSingleCellRow(SourceRow row, DataType type) {
    assertSingleCellRow(row, type, false);
  }
  
  
  private void assertSingleCellRow(SourceRow row, DataType type, boolean salted) {
    assertEquals(1, row.cells().size());
    assertEquals(1, row.cellTypes().size());
    assertEquals(type, row.cellTypes().get(0));
    Cell cell = row.cells().get(0);
    assertTrue(cell.hasData());
    if (type.isFixedSize())
      assertEquals(type.size(), cell.dataSize());
    assertEquals(cell.dataSize(), cell.data().remaining());
    
    // unlike with multi-cell rows, the cell's hash is not rehashed
    assertEquals(cell.hash(), row.hash());
    
    assertEquals(salted, cell.hasSalt());

    
    if (salted)
      assertSaltedHash(cell, row.rowSalt().get(), 0, DIGEST.newDigest());
    
  }
  
  
  
  private TableSalt testShaker(Object label) {
    byte[] salt = DIGEST.newDigest().digest(method(label).getBytes());
    return new TableSalt(salt);
  }
  
  
  

}
