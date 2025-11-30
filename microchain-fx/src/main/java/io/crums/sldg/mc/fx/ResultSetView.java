/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableColumn.CellDataFeatures;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.util.Callback;

/**
 * A read-only view of an adhoc {@linkplain ResultSet} composed
 * using a {@linkplain TableView}.
 * 
 * @see #ResultSetView(ResultSet)
 * @see #tableView()
 */
class ResultSetView {
  
  record Row(Object[] cells) {
    
    Row(int cc) {
      this(new Object[cc]);
    }
  }
  
  
  private static class ColumnInfo {
    
    final int col;
    final String name;
    final int sqlType;
    final String sqlTypeName;
    final SqlGenType generalType;
    final boolean autoIncrement;
    final boolean caseSensitive;
    final int precision;
    final int scale;
    
    ColumnInfo(int col, ResultSetMetaData meta) throws SQLException {
      this.col = col;
      if (col < 1)
        throw new IllegalArgumentException("col " + col);
      this.name = meta.getColumnName(col);
      this.sqlType = meta.getColumnType(col);
      this.generalType = SqlGenType.forSqlType(sqlType);
      this.sqlTypeName = meta.getColumnTypeName(col);
      this.autoIncrement =
          generalType.isIntegral() && meta.isAutoIncrement(col);
      this.caseSensitive =
          generalType.isString() && meta.isCaseSensitive(col);
      this.precision = meta.getPrecision(col);
      this.scale = meta.getScale(col);
    }
    
    boolean displayable() {
      
      switch (generalType) {
      
      case null:
      case BYTES:
      case LOB:
      case OTHER:
        return false;
        
      default:
        return true;
      }
    }
    
    
    Object readValue(ResultSet rs) throws SQLException {
      if (generalType.isString())
        return rs.getString(col);
      if (generalType.isNumber()) {
        Object value;
        if (generalType.isIntegral())
          value = rs.getLong(col);
        else
          value = rs.getDouble(col);
        
        return rs.wasNull() ? null : value;
      }
      if (generalType.isDate())
        return rs.getDate(col);
      if (generalType.isBool()) {
        var value = rs.getBoolean(col);
        return rs.wasNull() ? null : value;
      }
      throw new RuntimeException("huh.. a bug");
    }
    
    public String displayValue(Object value) {
      return value == null ? "" : value.toString();  // TODO
    }
  }
  
  
  
  
  
  
  private final ObservableList<Row> rows = FXCollections.observableArrayList();
  
  private final List<ColumnInfo> columnInfos = new ArrayList<>();
  
  private final TableView<Row> tableView;
  
  private List<ColumnInfo> displayableColumns;


  /**
   * Constructs a view using the given {@code ResultSet}. BLOBS and binary
   * columns are not displayed (this class has no apriori knowledge about
   * their formats/structure).
   * <p>
   * <em>Do not construct from the UI thead!</em> Depending on database,
   * driver, network, etc., this may be slow.
   * </p>
   * 
   * @param rs  open result set (caller must close; not closed here)
   * 
   * @throws AppException wraps any {@linkplain SQLException}
   */
  public ResultSetView(ResultSet rs) throws AppException {
    this.tableView = new TableView<>();
    tableView.setMaxHeight(Double.MAX_VALUE);
    tableView.setItems(rows); 
    try {
      
      configureColumns(rs);
      
      rows.setAll(toRows(rs));
      
    } catch (SQLException sx) {
      throw new AppException(sx);
    }
  }
  
  
  private void configureColumns(ResultSet rs) throws SQLException {
    var meta = rs.getMetaData();
    
    final int cc = meta.getColumnCount();

    columnInfos.clear();
    
    for (int col = 1; col <= cc; ++col)
      columnInfos.add( new ColumnInfo(col, meta) );
      
    
    this.displayableColumns =
        columnInfos.stream().filter(ColumnInfo::displayable).toList();
    
    final int dc = displayableColumns.size();
    List<TableColumn<Row, String>> tableColumns = new ArrayList<>(dc);
    
    for (int index = 0; index < dc; ++index) {
      ColumnInfo colInfo = displayableColumns.get(index);
      TableColumn<Row, String> column = new TableColumn<>(colInfo.name);
      column.setCellValueFactory(cellValueFactory(colInfo, index));
      column.setCellFactory(cellFactory(colInfo));
      tableColumns.add(column);
    }
    tableView.getColumns().setAll(tableColumns);
  }
  
  
  
  
  
  
  
  List<Row> toRows(ResultSet rs) throws SQLException {
    final int dc = displayableColumns.size();
    List<Row> rows = new ArrayList<>();
    
    while (rs.next()) {
      Row row = new Row(dc);
      for (int index = 0; index < dc; ++index) {
        ColumnInfo  colInfo = displayableColumns.get(index);
        row.cells[index] = colInfo.readValue(rs);
      }
      rows.add(row);
    }
    return rows;
  }
  
  
  private
  Callback<TableColumn.CellDataFeatures<Row,String>, ObservableValue<String>>
  cellValueFactory(ColumnInfo colInfo, final int index) {
    
    return new Callback<>() {
      
      @Override
      public ObservableValue<String> call(CellDataFeatures<Row, String> p) {
        Object value = p.getValue().cells[index];
        
        return new ReadOnlyObjectWrapper<>(colInfo.displayValue(value));
      }
    };
  }
  
  
  
  private
  Callback<TableColumn<Row, String>, TableCell<Row, String>>
  cellFactory(ColumnInfo colInfo) {
    
    return new Callback<>() {
      
      @Override
      public TableCell<Row, String> call(TableColumn<Row, String> p) {
        return new TableCell<>() {
          @Override
          protected void updateItem(String item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
              setText(null);
              setGraphic(null);
            } else {
              setText(item);
            }            
            StringBuilder tip =
                new StringBuilder("Type: ")
                .append(colInfo.sqlTypeName);
            if (colInfo.autoIncrement)
              tip.append(" (AUTO-INCREMENT)");
            tip.append("\nType Code: ").append(colInfo.sqlType);
            
            setTooltip(new Tooltip(tip.toString()));
          }
        };
      }
    };
  }
  
  /**
   * Returns the read-only table view populated with result-set data, set on
   * construction.
   */
  public TableView<Row> tableView() {
    return tableView;
  }

}




