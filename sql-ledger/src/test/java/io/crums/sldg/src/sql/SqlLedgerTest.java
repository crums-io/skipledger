/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;


import static org.junit.jupiter.api.Assertions.*;


import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.Cell;
import io.crums.sldg.src.DataType;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SharedConstants;
import io.crums.sldg.src.SourceLedger;
import io.crums.sldg.src.SourceRow;
import io.crums.testing.IoTestCase;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * 
 */
public class SqlLedgerTest extends IoTestCase {
  
  


  final static String MOCK_DB_NAME = "mock_db";
  
  
  static Connection newDatabase(File dir)
      throws ClassNotFoundException, SQLException {
    dir.mkdirs();
    String dbPath = new File(dir, MOCK_DB_NAME).getPath();
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection("jdbc:h2:" + dbPath);
  }
  
  
  private Connection newDatabase(Object label)
      throws ClassNotFoundException, SQLException {
    
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection("jdbc:h2:mem:" + method(label));
    
  }
  
  
  
  static Date date(int year, int month, int day, int hour, int minute) {
    var calendar = Calendar.getInstance();
    calendar.clear();
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.set(year, month, day, hour, minute);
    return new Date(calendar.getTimeInMillis());
  }
  static Date date(int year, int month, int day, int hour, int minute, int seconds) {
    var date = date(year, month, day, hour, minute);
    return new Date(date.getTime() + seconds*1000L);
  }
  

  
  final static String INVENTORY_LOG_CREATE =
      """
      CREATE TABLE inventory_log (
        entry_no BIGINT NOT NULL AUTO_INCREMENT,
        entry_date DATE NOT NULL,
        entry_type INT NOT NULL,
        entry_ref INT,
        item_id VARCHAR(100) NOT NULL,
        item_type INT NOT NULL,
        note VARCHAR(4096),
        PRIMARY KEY (entry_no)
      )""";
  
  final static void createInventoryLog(Connection con) throws SQLException {
    try (var stmt = con.createStatement()) {
      stmt.execute(INVENTORY_LOG_CREATE);
    }
  }
  
  
  final static String INVENTORY_LOG_SIZE_QUERY =
      "SELECT MAX(entry_no) FROM inventory_log";
  
  static PreparedStatement prepareInventoryLogSizeQuery(Connection con)
      throws SQLException {
    return con.prepareStatement(INVENTORY_LOG_SIZE_QUERY);
  }
  
  
  final static String INVENTORY_LOG_ROW_QUERY =
      """
      SELECT entry_date,
        entry_type,
        entry_ref,
        item_id,
        item_type,
        note
      FROM inventory_log WHERE entry_no = ?""";
  
  static PreparedStatement prepareInventoryLogRowQuery(Connection con)
      throws SQLException {
    return con.prepareStatement(INVENTORY_LOG_ROW_QUERY);
  }
  
  final static String INVENTORY_LOG_INSERT =
      """
      INSERT INTO inventory_log (
        entry_date, entry_type, entry_ref, item_id, item_type, note)
      VALUES (?, ?, ?, ?, ?, ?)""";
  
  static PreparedStatement prepareInventoryLogInsert(Connection con)
      throws SQLException {
    
    return con.prepareStatement(INVENTORY_LOG_INSERT);
  }
  
  
  record InventoryLogRow(
      long entryNo,
      Date entryDate,
      int entryType,
      Optional<Integer> entryRef,
      String itemId,
      int itemType,
      Optional<String> note) {
    
    InventoryLogRow {
      Objects.requireNonNull(entryDate);
      if (entryRef == null)
        entryRef = Optional.empty();
      if (note == null)
        note = Optional.empty();
    }
    
    
    
    InventoryLogRow(
        long entryNo,
        Date entryDate,
        int entryType,
        String itemId,
        int itemType) {
      this(entryNo, entryDate, entryType, null, itemId, itemType, null);
    }
    

    InventoryLogRow(
        long entryNo,
        Date entryDate,
        int entryType,
        String itemId,
        int itemType,
        String note) {
      this(entryNo, entryDate, entryType, null, itemId, itemType, Optional.ofNullable(note));
    }
    
  }
  
  
  
  static void insert(PreparedStatement insertStmt, InventoryLogRow row)
      throws SQLException {
    
    insertStmt.setDate(1, row.entryDate());
    insertStmt.setInt(2, row.entryType());
    if (row.entryRef().isEmpty())
      insertStmt.setNull(3, Types.INTEGER);
    else
      insertStmt.setInt(3, row.entryRef().get());
    insertStmt.setString(4, row.itemId());
    insertStmt.setInt(5, row.itemType());
    if (row.note().isEmpty())
      insertStmt.setNull(6, Types.VARCHAR);
    else
      insertStmt.setString(6, row.note().get());
    insertStmt.execute();
  }
  
  
  
  static SqlLedger inventoryLogSource(
      Connection con, SaltScheme saltScheme, TableSalt shaker)
      throws SQLException {
    PreparedStatement sizeQuery = prepareInventoryLogSizeQuery(con);
    assertNotNull(sizeQuery);
    PreparedStatement rowQuery = prepareInventoryLogRowQuery(con);
    assertNotNull(rowQuery);
    var config = new SqlLedger.Config(sizeQuery, rowQuery, saltScheme, shaker);
    return new SqlLedger(config);
  }
  
  
  @SuppressWarnings("deprecation")
  static void assertExpected(InventoryLogRow expected, SqlLedger source) {
    SourceRow srcRow = source.getSourceRow(expected.entryNo());
    assertEquals(expected.entryNo(), srcRow.no());
    var saltScheme = source.saltScheme();
    {
      // The java.sql.Date type, as implemented by h2 is a mess
      // .. the Date type should be avoided, generally: use UTC millis/seconds
      // instead.
      final int index = 0;
      assertEquals(DataType.DATE, srcRow.cellTypes().get(index));
      Cell dateCell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), dateCell.hasSalt());
      assertCellSize(DataType.DATE, dateCell);
      Date expectedDate = expected.entryDate();
      Date actual = new Date(dateCell.data().getLong());
      assertEquals(expectedDate.getYear(), actual.getYear());
      assertEquals(expectedDate.getMonth(), actual.getMonth());
      assertEquals(expectedDate.getDate(), actual.getDate());
      assertCellSize(DataType.DATE, dateCell);
    }
    
    {
      final int index = 1;
      assertEquals(DataType.LONG, srcRow.cellTypes().get(index));
      Cell entryType = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), entryType.hasSalt());
      assertCellSize(DataType.LONG, entryType);
      assertEquals(expected.entryType(), entryType.data().getLong());
      assertCellSize(DataType.LONG, entryType);
    }
    
    {
      DataType expectedType =
          expected.entryRef().isEmpty() ? DataType.NULL : DataType.LONG;
      assertEquals(expectedType, srcRow.cellTypes().get(2));    
      Cell entryRef = srcRow.cells().get(2);
      assertCellSize(expectedType, entryRef);
      if (!expectedType.isNull())
        assertEquals(expected.entryRef().get().longValue(), entryRef.data().getLong());
      assertCellSize(expectedType, entryRef);
    }
    
    {
      assertEquals(DataType.STRING, srcRow.cellTypes().get(3));
      Cell itemId = srcRow.cells().get(3);
      String actualItemId = Strings.utf8String(itemId.data());
      assertEquals(expected.itemId(), actualItemId);
    }
    
    {
      assertEquals(DataType.LONG, srcRow.cellTypes().get(4));
      Cell itemType = srcRow.cells().get(4);
      assertCellSize(DataType.LONG, itemType);
      assertEquals(expected.itemType(), itemType.data().getLong());
    }
    
    {
      DataType expectedType =
          expected.note().isEmpty() ? DataType.NULL : DataType.STRING;
      assertEquals(expectedType, srcRow.cellTypes().get(5));
      Cell note = srcRow.cells().get(5);
      if (!expectedType.isNull())
        assertEquals(
            expected.note().get(),
            Strings.utf8String(note.data()) );
    }
    
  }
  
  
  

  private static void assertCellSize(DataType type, Cell cell) {
    if (cell.hasData())
    assertEquals(type.size(), cell.dataSize());
    assertEquals(type.size(), cell.data().remaining());
  }
  
  
  
  @Test
  public void testOneNoSalt() throws Exception {
    final Object label = new Object() {  };
    testOneImpl(label, SaltScheme.of());
  }

  @Test
  public void testOneSaltAll() throws Exception {
    final Object label = new Object() {  };
    testOneImpl(label, SaltScheme.ofAll());
  }
  
  
  private void testOneImpl(Object label, SaltScheme saltScheme)
      throws ClassNotFoundException, SQLException {
    
  
    File dir = getMethodOutputFilepath(label);
    try (var closer = new TaskStack()) {
      
      var con = newDatabase(dir);
      closer.pushClose(con);
      
      createInventoryLog(con);
      
      TableSalt shaker = saltScheme.hasSalt() ? makeTableSalt(label) : null;
      
      SqlLedger source = inventoryLogSource(con, saltScheme, shaker);
      closer.pushClose(source);
      
      assertEquals(0L, source.size());
      
      PreparedStatement insertStmt = prepareInventoryLogInsert(con);
      closer.pushClose(insertStmt);
      
      var row = new InventoryLogRow(
          1L,
          date(2025, 8, 14, 0, 0),
          99,
          "thingaming-H70",
          33);
      insert(insertStmt, row);

      assertEquals(0L, source.size());
      try {
        source.getSourceRow(1L);
        fail();
      } catch (IndexOutOfBoundsException expected) {  }
      
      assertEquals(1L, source.updateSize());
      assertEquals(1L, source.size());
      
      assertExpected(row, source);
    }
  }
  
  
  @Test
  public void testTwo() throws Exception {
    Object label = new Object() { };
    testTwoImpl(label, SaltScheme.ofAllExcept(0, 1));
  }
  
  private void testTwoImpl(Object label, SaltScheme saltScheme)
      throws ClassNotFoundException, SQLException {
    
  
//    File dir = getMethodOutputFilepath(label);
    try (var closer = new TaskStack()) {
      
      var con = newDatabase(label);
      closer.pushClose(con);
      
      createInventoryLog(con);
      
      PreparedStatement insertStmt = prepareInventoryLogInsert(con);
      closer.pushClose(insertStmt);
      
      var row1 = new InventoryLogRow(
          1L,
          date(2025, 8, 14, 0, 0),
          99,
          "thingaming-H70",
          33);
      insert(insertStmt, row1);
      
      var row2 = new InventoryLogRow(
          2L,
          date(2025, 8, 16, 0, 0),
          11,
          "thingaming-H70",
          33,
          "Reason: fails inspection.");

      insert(insertStmt, row2);

      TableSalt shaker = saltScheme.hasSalt() ? makeTableSalt(label) : null;
      SqlLedger source = inventoryLogSource(con, saltScheme, shaker);
      closer.pushClose(source);

      assertEquals(2L, source.size());
      
      assertExpected(row1, source);
      assertExpected(row2, source);
    }
  }
  
  @Test
  public void testThreeWithGap() throws Exception {
    Object label = new Object() { };
    testThreeWithGapImpl(label, SaltScheme.ofAll());
  }
  
  private void testThreeWithGapImpl(Object label, SaltScheme saltScheme)
      throws ClassNotFoundException, SQLException {
    
  
    File dir = getMethodOutputFilepath(label);
    try (var closer = new TaskStack()) {
      
      var con = newDatabase(dir);
      closer.pushClose(con);
      
      createInventoryLog(con);
      
      PreparedStatement insertStmt = prepareInventoryLogInsert(con);
      closer.pushClose(insertStmt);
      
      var row1 = new InventoryLogRow(
          1L,
          date(2025, 8, 14, 0, 0),
          99,
          "thingaming-H70",
          33);
      insert(insertStmt, row1);
      
      var row3 = new InventoryLogRow(
          3L,
          date(2025, 8, 16, 0, 0),
          11,
          "thingaming-H70",
          33,
          "Reason: fails inspection.");

      insert(insertStmt, row3);
      insert(insertStmt, row3);
      
      var drop = con.createStatement();
      drop.execute("DELETE FROM inventory_log WHERE entry_no = 2");
      

      TableSalt shaker = saltScheme.hasSalt() ? makeTableSalt(label) : null;
      SqlLedger source = inventoryLogSource(con, saltScheme, shaker);
      closer.pushClose(source);

      assertEquals(3L, source.size());
      
      assertExpected(row1, source);
      assertEquals(SharedConstants.DIGEST.sentinelHash(), source.getSourceRow(2L).hash());
      assertExpected(row3, source);
    }
  }
  
  
  
  private TableSalt makeTableSalt(Object label) {
    int seed = method(label).hashCode();
    byte[] salt = new byte[SharedConstants.HASH_WIDTH];
    new Random(seed).nextBytes(salt);
    return new TableSalt(salt);
  }
  
  
  
  
  

}




















