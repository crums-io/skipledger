/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

import javax.sql.DataSource;

import io.crums.util.Lists;

/**
 * A simple graph of tables from a relational database. Each node (vertex) is a
 * table (identified simply by name) with directed edges to other tables defined
 * via foreign-key/primary-key relationships.
 * 
 * <h2>Design and Purpose</h2>
 * <p>
 * The ability to explore table relationships is useful when setting up a
 * microchain's ledger-definition query. This class models those relationships.
 * The following infuence the design:
 * </p>
 * <ol>
 * <li><em>Lazy.</em> A database may host hundreds of tables. We don't want
 * to hit it with hundreds of queries on app start up.</li>
 * <li><em>Cache.</em> Since queries are expensive, and the data is relatively
 * static, results are cached.</li>
 * <li><em>Avoid concurrent duplicate queries.</em> The source of the queries
 * are from the UI (executed on worker threads) and a user may cause multiple
 * queries to run concurrently (e.g. shifty mouse gestures, or a cat making its
 * way across the keyboard).
 * </li>
 * </ol>
 * <h2>API</h2>
 * <p>
 * The following methods are expensive and should never be invoked directly
 * from the UI thread:
 * </p>
 * <ul>
 * <li>{@linkplain #softLoadTable(String)} </li>
 * <li>{@linkplain #loadTable(String)} </li>
 * </ul>
 * <p>
 * The following methods return immediately and may be invoked from the UI thread:
 * </p>
 * <ul>
 * <li>{@linkplain #loadedTable(String)} </li>
 * <li>{@linkplain #referredTables(String)} </li>
 * <li>{@linkplain #dependentTables(String)} </li>
 * </ul>
 */
class TableGraph {
  
  /**
   * A 3-tuple representing a foreign key in a {@linkplain Table}.
   * 
   * @param columnName          column name (in "this" table)
   * @param foreignTableName    foreign table name
   * @param foreignColumnName   column name in foreign table
   */
  record ForeignKey(
      String columnName,
      String foreignTableName,
      String foreignColumnName) {
  }
  
  
  /**
   * A node in the relational graph of tables.
   */
  final static class Table {
    
    private final String name;
    private final List<String> pkColumns;
    private final List<ForeignKey> foreignKeys;

    /**
     * 
     * @param name              table name (unique across a graph instance)
     * @param pkColumns         primary key column names (in order and unique)
     * @param foreignKeys       foreign keys (with unique column names)
     */
    Table(String name, List<String> pkColumns, List<ForeignKey> foreignKeys) {
      this.name = name;
      this.pkColumns = List.copyOf(pkColumns);
      this.foreignKeys = List.copyOf(foreignKeys);
      if (name.isBlank())
        throw new IllegalArgumentException("blank name");
    }
    
    
    
    public String name() {
      return name;
    }
    
    
    public List<String> primaryKeyColumns() {
      return pkColumns;
    }
    
    
    public boolean hasPrimaryKey() {
      return !pkColumns.isEmpty();
    }
    
    
    public boolean hasForeignKeys() {
      return !foreignKeys.isEmpty();
    }
    
    
    public List<String> foreignKeyColumns() {
      return Lists.map(foreignKeys, ForeignKey::columnName);
    }
    
    
    public List<String> foreignKeyTables() {
      return
          foreignKeys.stream().map(ForeignKey::foreignTableName).distinct()
          .toList();
    }
    
    
    public List<ForeignKey> foreignKeys() {
      return foreignKeys;
    }
    
    
    
  }
  
  
  protected final Object lock = new Object();
  
  
  private final Map<String, Optional<Table>> tablesByName = new TreeMap<>();
  
  private final Map<String, List<String>> tableDependents = new TreeMap<>();
  
  
  private final DataSource dataSource;
  
  private final List<String> tableNames;
  
  
  
  /**
   * Constructs an instance using the given database {@code dataSource}
   * and retrieves its list of tables. Do not invoke from the
   * UI thread.
   * 
   * @see #tableNames()
   */
  TableGraph(DataSource dataSource, Predicate<String> tableFilter)
      throws SQLException {
    this.dataSource = dataSource;
    
    try (Connection con = dataSource.getConnection()) {
      var tables = listTables(con.getMetaData(), tableFilter);
      String[] array = tables.toArray(new String[tables.size()]);
      Arrays.sort(array);
      this.tableNames = Lists.asReadOnlyList(array);
    }
  }
  
  
  
  public List<String> tableNames() {
    return tableNames;
  }
  
  
  
  private void checkTableName(String name) {
    if (Collections.binarySearch(tableNames, name) < 0)
      throw new IllegalArgumentException(
          "unknown table (quoted): '%s'".formatted(name));
    
  }
  
  
  /**
   * Returns a list of tables with foreign keys referencing
   * the given table's primary keys. I.e. a list of tables whose
   * schema depends on the given table.
   * 
   * @param table     the name of the table
   * 
   * @return read-only list of table names, possibly empty 
   */
  public List<String> dependentTables(String table) {
    synchronized (lock) {
      return tableDependents.getOrDefault(table, List.of());
    }
  }
  
  
  /**
   * Returns a list of tables referenced thru the foreign-key columns of the
   * given {@code table}.
   * 
   * @param table     the name of the table (one of {@linkplain #tableNames()})
   * 
   * @return read-only list of table names, possibly empty 
   */
  public List<String> referredTables(String table) {
    synchronized (lock) {
      return
          loadedTable(table).map(Table::foreignKeyTables).orElse(List.of());
    }
  }
  
  
  
  
  
  
  public Optional<Table> loadedTable(String name) {
    synchronized (lock) {
      var entry = tablesByName.get(name);
      if (entry != null)
        return entry;
    }
    checkTableName(name);
    return Optional.empty();
  }
  
  
  
  public Table loadTable(String name) throws SQLException {
    return loadTable(name, false).get();    
  }
  
  
  
  
  public Optional<Table> softLoadTable(String name) throws SQLException {
    return loadTable(name, true);
  }
  
  
  
  private Optional<Table> loadTable(String name, boolean soft) throws SQLException {

    synchronized (lock) {
      var entry = tablesByName.get(name);
      
      if (entry != null) {
        if (soft || entry.isPresent())
          return entry;
      
      } else {
        
        checkTableName(name);
        tablesByName.put(name, Optional.empty());
      }
    }
    
    Table table = null;
    
    try (Connection con = dataSource.getConnection()) {
      
      table = loadTable(name, con.getMetaData());
    
      synchronized (lock) {
        
        var entry = tablesByName.get(name);
        if (entry.isPresent())
          return entry;
        
        for (var fkTable : table.foreignKeyTables())
          addDependentTable(fkTable, name);

        entry = Optional.of(table);
        tablesByName.put(name, entry);
        return entry;
      }
    
    } finally {
      
      if (table == null) {
        synchronized (lock) {
          
          var entry = tablesByName.get(name);
          if (entry != null && entry.isEmpty())
            tablesByName.remove(name);
        }
      }
    }
  }
  
  
  private void addDependentTable(String table, String dependent) {
    List<String> deps = tableDependents.get(table);
    
    if (deps == null) {
      tableDependents.put(table, List.of(dependent));
      return;
    }
    
    if (deps.contains(dependent))
      return;
    
    deps = List.copyOf(Lists.concat(deps, dependent));
    tableDependents.put(table, deps);
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  private final static String[] TABLE_TYPES = { "TABLE" };
  
  private List<String> listTables(
      DatabaseMetaData meta, Predicate<String> filter) throws SQLException {

    var tables = new ArrayList<String>();
    try (ResultSet rs = meta.getTables(null, null, null, TABLE_TYPES)) {
      
//      var debug = System.out;
//      var rsMeta = rs.getMetaData();
//      final int cc = rsMeta.getColumnCount();
//      debug.print(this + ": cc " + cc);
//      for (int col = 1; col <= cc; ++col)
//        debug.print(" || " + rsMeta.getColumnName(col));
//      debug.println();
      
      while (rs.next()) {
        var name = rs.getString("TABLE_NAME");
        if (filter == null || filter.test(name))
          tables.add(name);
      }
      
      return tables;
    }
    
  }
  
  
  
  private Table loadTable(String table, DatabaseMetaData meta) throws SQLException {
    ResultSet rs = meta.getPrimaryKeys(null, null, table);
    
    List<String> pkColumns = new ArrayList<>();
    while (rs.next())
      pkColumns.add(rs.getString("COLUMN_NAME"));
    
    rs.close();
    
    rs = meta.getImportedKeys(null, null, table);
    
    List<ForeignKey> foreignKeys = new ArrayList<>();
    while (rs.next()) {
      var columnName = rs.getString("FKCOLUMN_NAME");
      var foreignTable = rs.getString("PKTABLE_NAME");
      var foreignColumn = rs.getString("PKCOLUMN_NAME");
      foreignKeys.add(new ForeignKey(columnName, foreignTable, foreignColumn));
    }
    
    rs.close();
    
    return new Table(table, pkColumns, foreignKeys);
  }
  

}






















