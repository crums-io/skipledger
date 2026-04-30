/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;


import static org.junit.jupiter.api.Assertions.*;


import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
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


  // ---- Financial Ledger: DECIMAL + BOOLEAN coverage ----

  final static String FINANCIAL_LEDGER_CREATE =
      """
      CREATE TABLE financial_ledger (
        txn_no     BIGINT        NOT NULL AUTO_INCREMENT,
        txn_date   DATE          NOT NULL,
        amount     DECIMAL(18,4) NOT NULL,
        fee        DECIMAL(10,4),
        balance    DECIMAL(20,4) NOT NULL,
        memo       VARCHAR(256),
        is_credit  BOOLEAN       NOT NULL,
        PRIMARY KEY (txn_no)
      )""";

  final static void createFinancialLedger(Connection con) throws SQLException {
    try (var stmt = con.createStatement()) {
      stmt.execute(FINANCIAL_LEDGER_CREATE);
    }
  }

  final static String FINANCIAL_LEDGER_SIZE_QUERY =
      "SELECT MAX(txn_no) FROM financial_ledger";

  static PreparedStatement prepareFinancialLedgerSizeQuery(Connection con)
      throws SQLException {
    return con.prepareStatement(FINANCIAL_LEDGER_SIZE_QUERY);
  }

  final static String FINANCIAL_LEDGER_ROW_QUERY =
      """
      SELECT txn_date, amount, fee, balance, memo, is_credit
      FROM financial_ledger WHERE txn_no = ?""";

  static PreparedStatement prepareFinancialLedgerRowQuery(Connection con)
      throws SQLException {
    return con.prepareStatement(FINANCIAL_LEDGER_ROW_QUERY);
  }

  final static String FINANCIAL_LEDGER_INSERT =
      """
      INSERT INTO financial_ledger (txn_date, amount, fee, balance, memo, is_credit)
      VALUES (?, ?, ?, ?, ?, ?)""";

  static PreparedStatement prepareFinancialLedgerInsert(Connection con)
      throws SQLException {
    return con.prepareStatement(FINANCIAL_LEDGER_INSERT);
  }


  record FinancialRow(
      long txnNo, Date txnDate,
      BigDecimal amount, Optional<BigDecimal> fee,
      BigDecimal balance, Optional<String> memo,
      boolean isCredit) {

    FinancialRow {
      Objects.requireNonNull(txnDate, "txnDate");
      Objects.requireNonNull(amount, "amount");
      Objects.requireNonNull(balance, "balance");
      if (fee == null)
        fee = Optional.empty();
      if (memo == null)
        memo = Optional.empty();
    }

    /** Convenience constructor: fee and memo both absent. */
    FinancialRow(
        long txnNo, Date txnDate,
        BigDecimal amount, BigDecimal balance,
        boolean isCredit) {
      this(txnNo, txnDate, amount, null, balance, null, isCredit);
    }
  }


  static void insert(PreparedStatement insertStmt, FinancialRow row)
      throws SQLException {
    insertStmt.setDate(1, row.txnDate());
    insertStmt.setBigDecimal(2, row.amount());
    if (row.fee().isEmpty())
      insertStmt.setNull(3, Types.DECIMAL);
    else
      insertStmt.setBigDecimal(3, row.fee().get());
    insertStmt.setBigDecimal(4, row.balance());
    if (row.memo().isEmpty())
      insertStmt.setNull(5, Types.VARCHAR);
    else
      insertStmt.setString(5, row.memo().get());
    insertStmt.setBoolean(6, row.isCredit());
    insertStmt.execute();
  }


  static SqlLedger financialLedgerSource(
      Connection con, SaltScheme saltScheme, TableSalt shaker)
      throws SQLException {
    PreparedStatement sizeQuery = prepareFinancialLedgerSizeQuery(con);
    assertNotNull(sizeQuery);
    PreparedStatement rowQuery = prepareFinancialLedgerRowQuery(con);
    assertNotNull(rowQuery);
    var config = new SqlLedger.Config(sizeQuery, rowQuery, saltScheme, shaker);
    return new SqlLedger(config);
  }


  @SuppressWarnings("deprecation")
  static void assertFinancialExpected(FinancialRow expected, SqlLedger source) {
    SourceRow srcRow = source.getSourceRow(expected.txnNo());
    assertEquals(expected.txnNo(), srcRow.no());
    var saltScheme = source.saltScheme();

    // col 0: txn_date — DATE
    {
      final int index = 0;
      assertEquals(DataType.DATE, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      Date expectedDate = expected.txnDate();
      Date actual = new Date(cell.data().getLong());
      assertEquals(expectedDate.getYear(), actual.getYear());
      assertEquals(expectedDate.getMonth(), actual.getMonth());
      assertEquals(expectedDate.getDate(), actual.getDate());
    }

    // col 1: amount — BIG_DEC (non-null)
    {
      final int index = 1;
      assertEquals(DataType.BIG_DEC, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      assertEquals(0, expected.amount().compareTo((BigDecimal) cell.value()));
    }

    // col 2: fee — BIG_DEC or NULL
    {
      final int index = 2;
      DataType expectedType =
          expected.fee().isEmpty() ? DataType.NULL : DataType.BIG_DEC;
      assertEquals(expectedType, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      if (!expectedType.isNull())
        assertEquals(0, expected.fee().get().compareTo((BigDecimal) cell.value()));
    }

    // col 3: balance — BIG_DEC (non-null)
    {
      final int index = 3;
      assertEquals(DataType.BIG_DEC, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      assertEquals(0, expected.balance().compareTo((BigDecimal) cell.value()));
    }

    // col 4: memo — STRING or NULL
    {
      final int index = 4;
      DataType expectedType =
          expected.memo().isEmpty() ? DataType.NULL : DataType.STRING;
      assertEquals(expectedType, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      if (!expectedType.isNull())
        assertEquals(expected.memo().get(), Strings.utf8String(cell.data()));
    }

    // col 5: is_credit — BOOL
    {
      final int index = 5;
      assertEquals(DataType.BOOL, srcRow.cellTypes().get(index));
      Cell cell = srcRow.cells().get(index);
      assertEquals(saltScheme.isSalted(index), cell.hasSalt());
      assertEquals(expected.isCredit(), cell.value());
    }
  }


  @Test
  public void testDecimalNoSalt() throws Exception {
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      createFinancialLedger(con);

      SqlLedger source = financialLedgerSource(con, SaltScheme.of(), null);
      closer.pushClose(source);

      assertEquals(0L, source.size());

      PreparedStatement insertStmt = prepareFinancialLedgerInsert(con);
      closer.pushClose(insertStmt);

      // debit, no fee, no memo
      var row = new FinancialRow(
          1L,
          date(2025, 0, 15, 0, 0),
          new BigDecimal("1000.0000"),
          new BigDecimal("5000.0000"),
          false);
      insert(insertStmt, row);

      assertEquals(1L, source.updateSize());
      assertFinancialExpected(row, source);
    }
  }


  @Test
  public void testDecimalSaltAll() throws Exception {
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      createFinancialLedger(con);

      TableSalt shaker = makeTableSalt(label);
      SqlLedger source = financialLedgerSource(con, SaltScheme.ofAll(), shaker);
      closer.pushClose(source);

      PreparedStatement insertStmt = prepareFinancialLedgerInsert(con);
      closer.pushClose(insertStmt);

      // credit, all fields populated
      var row = new FinancialRow(
          1L,
          date(2025, 0, 20, 0, 0),
          new BigDecimal("2500.7500"),
          Optional.of(new BigDecimal("25.0000")),
          new BigDecimal("7500.7500"),
          Optional.of("Payment received"),
          true);
      insert(insertStmt, row);

      assertEquals(1L, source.updateSize());
      assertFinancialExpected(row, source);
    }
  }


  @Test
  public void testDecimalTwoRowsMixedSalt() throws Exception {
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      createFinancialLedger(con);

      // salt all except txn_date (0) and memo (4)
      SaltScheme saltScheme = SaltScheme.ofAllExcept(0, 4);
      TableSalt shaker = makeTableSalt(label);
      SqlLedger source = financialLedgerSource(con, saltScheme, shaker);
      closer.pushClose(source);

      PreparedStatement insertStmt = prepareFinancialLedgerInsert(con);
      closer.pushClose(insertStmt);

      // row 1: debit, fee absent
      var row1 = new FinancialRow(
          1L,
          date(2025, 1, 1, 0, 0),
          new BigDecimal("500.0000"),
          new BigDecimal("4500.0000"),
          false);
      insert(insertStmt, row1);

      // row 2: credit, fee present
      var row2 = new FinancialRow(
          2L,
          date(2025, 1, 5, 0, 0),
          new BigDecimal("1000.0000"),
          Optional.of(new BigDecimal("10.0000")),
          new BigDecimal("5500.0000"),
          Optional.of("Wire transfer"),
          true);
      insert(insertStmt, row2);

      assertEquals(2L, source.updateSize());
      assertFinancialExpected(row1, source);
      assertFinancialExpected(row2, source);
    }
  }


  @Test
  public void testDecimalBeyondLongRange() throws Exception {
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      try (var stmt = con.createStatement()) {
        stmt.execute("""
            CREATE TABLE big_amount_test (
              row_no BIGINT NOT NULL AUTO_INCREMENT,
              amount DECIMAL(24,0) NOT NULL,
              PRIMARY KEY (row_no)
            )""");
      }

      BigDecimal bigAmount = new BigDecimal(
          BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));

      try (var ps = con.prepareStatement(
          "INSERT INTO big_amount_test (amount) VALUES (?)")) {
        ps.setBigDecimal(1, bigAmount);
        ps.execute();
      }

      PreparedStatement sizeQuery = con.prepareStatement(
          "SELECT MAX(row_no) FROM big_amount_test");
      closer.pushClose(sizeQuery);
      PreparedStatement rowQuery = con.prepareStatement(
          "SELECT amount FROM big_amount_test WHERE row_no = ?");
      closer.pushClose(rowQuery);

      var config = new SqlLedger.Config(sizeQuery, rowQuery, SaltScheme.of(), null);
      SqlLedger source = new SqlLedger(config);
      closer.pushClose(source);

      assertEquals(1L, source.size());

      SourceRow srcRow = source.getSourceRow(1L);
      assertEquals(DataType.BIG_DEC, srcRow.cellTypes().get(0));
      BigDecimal actual = (BigDecimal) srcRow.cells().get(0).value();
      assertEquals(0, bigAmount.compareTo(actual));
    }
  }


  @Test
  public void testVarbinaryColumn() throws Exception {
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      try (var stmt = con.createStatement()) {
        stmt.execute("""
            CREATE TABLE binary_test (
              row_no  BIGINT       NOT NULL AUTO_INCREMENT,
              payload VARBINARY(256) NOT NULL,
              PRIMARY KEY (row_no)
            )""");
      }

      byte[] expected = { 0x01, 0x02, 0x03, (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };

      try (var ps = con.prepareStatement(
          "INSERT INTO binary_test (payload) VALUES (?)")) {
        ps.setBytes(1, expected);
        ps.execute();
      }

      PreparedStatement sizeQuery = con.prepareStatement(
          "SELECT MAX(row_no) FROM binary_test");
      closer.pushClose(sizeQuery);
      PreparedStatement rowQuery = con.prepareStatement(
          "SELECT payload FROM binary_test WHERE row_no = ?");
      closer.pushClose(rowQuery);

      var config = new SqlLedger.Config(sizeQuery, rowQuery, SaltScheme.of(), null);
      SqlLedger source = new SqlLedger(config);
      closer.pushClose(source);

      assertEquals(1L, source.size());

      SourceRow srcRow = source.getSourceRow(1L);
      assertEquals(DataType.BYTES, srcRow.cellTypes().get(0));
      java.nio.ByteBuffer buf = srcRow.cells().get(0).data();
      byte[] actual = new byte[buf.remaining()];
      buf.get(actual);
      assertArrayEquals(expected, actual);
    }
  }


  @Test
  public void testUnsupportedDefaultTypeRejected() throws Exception {
    // H2's ARRAY type falls to the already-present Types.ARRAY case (throws), so we use
    // it as a proxy to confirm that unsupported types surface as SqlLedgerException.
    // The new default branch covers other driver-specific types (JAVA_OBJECT, STRUCT,
    // REF, etc.) that cannot be easily created in H2 but are rejected by the same path.
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      try (var stmt = con.createStatement()) {
        // H2 requires a base type for ARRAY columns (e.g. VARCHAR ARRAY)
        stmt.execute("""
            CREATE TABLE array_test (
              row_no BIGINT        NOT NULL AUTO_INCREMENT,
              tags   VARCHAR ARRAY NOT NULL,
              PRIMARY KEY (row_no)
            )""");
        stmt.execute("INSERT INTO array_test (tags) VALUES (ARRAY['a','b'])");
      }

      PreparedStatement sizeQuery = con.prepareStatement(
          "SELECT MAX(row_no) FROM array_test");
      closer.pushClose(sizeQuery);
      PreparedStatement rowQuery = con.prepareStatement(
          "SELECT tags FROM array_test WHERE row_no = ?");
      closer.pushClose(rowQuery);

      var config = new SqlLedger.Config(sizeQuery, rowQuery, SaltScheme.of(), null);
      SqlLedger source = new SqlLedger(config);
      closer.pushClose(source);

      assertEquals(1L, source.size());
      assertThrows(SqlLedgerException.class, () -> source.getSourceRow(1L));
    }
  }


  @Test
  public void testNumericColumn() throws Exception {
    // Types.NUMERIC is semantically identical to Types.DECIMAL — both map to BIG_DEC.
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      BigDecimal expected = new BigDecimal("1.23");

      try (var stmt = con.createStatement()) {
        stmt.execute("""
            CREATE TABLE numeric_test (
              row_no BIGINT NOT NULL AUTO_INCREMENT,
              val NUMERIC(10,2) NOT NULL,
              PRIMARY KEY (row_no)
            )""");
        stmt.execute("INSERT INTO numeric_test (val) VALUES (1.23)");
      }

      PreparedStatement sizeQuery = con.prepareStatement(
          "SELECT MAX(row_no) FROM numeric_test");
      closer.pushClose(sizeQuery);
      PreparedStatement rowQuery = con.prepareStatement(
          "SELECT val FROM numeric_test WHERE row_no = ?");
      closer.pushClose(rowQuery);

      var config = new SqlLedger.Config(sizeQuery, rowQuery, SaltScheme.of(), null);
      SqlLedger source = new SqlLedger(config);
      closer.pushClose(source);

      assertEquals(1L, source.size());

      SourceRow srcRow = source.getSourceRow(1L);
      assertEquals(DataType.BIG_DEC, srcRow.cellTypes().get(0));
      assertEquals(0, expected.compareTo((BigDecimal) srcRow.cells().get(0).value()));
    }
  }


  @Test
  public void testFloatColumnRejected() throws Exception {
    // Floating-point types (FLOAT, DOUBLE, REAL) are deliberately unsupported:
    // their imprecise representation would make ledger hashes non-reproducible.
    Object label = new Object() { };
    try (var closer = new TaskStack()) {
      var con = newDatabase(label);
      closer.pushClose(con);

      try (var stmt = con.createStatement()) {
        stmt.execute("""
            CREATE TABLE float_test (
              row_no BIGINT NOT NULL AUTO_INCREMENT,
              val    DOUBLE NOT NULL,
              PRIMARY KEY (row_no)
            )""");
        stmt.execute("INSERT INTO float_test (val) VALUES (1.23)");
      }

      PreparedStatement sizeQuery = con.prepareStatement(
          "SELECT MAX(row_no) FROM float_test");
      closer.pushClose(sizeQuery);
      PreparedStatement rowQuery = con.prepareStatement(
          "SELECT val FROM float_test WHERE row_no = ?");
      closer.pushClose(rowQuery);

      var config = new SqlLedger.Config(sizeQuery, rowQuery, SaltScheme.of(), null);
      SqlLedger source = new SqlLedger(config);
      closer.pushClose(source);

      assertEquals(1L, source.size());
      assertThrows(SqlLedgerException.class, () -> source.getSourceRow(1L));
    }
  }


}




















