/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import io.crums.util.TaskStack;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.RowConstraints;
import javafx.scene.text.Text;
import javafx.scene.control.Alert.AlertType;

/**
 * SQL query and results view.
 * 
 * <h2>Grid Pane</h2>
 * <p>
 * 5 columns wide, 3 rows.
 * </p>
 */
class QueryView {
  
  final static int PREF_LINES_FOR_QUERY = 5;
  private final static int GRID_ROWS = 3;
  private final static int GRID_COLS = 5;


  private final GridPane gridPane = new GridPane();

  private final Map<String, ResultSetView> queryCache = new HashMap<>();
  
  private final AppState appState;
  
  private final TextArea queryInput = new TextArea("SELECT ");
  
  private final Button clearButton;
  private final Button submitButton;
  
  
  /** Last successful query (shown by {@linkplain #queryResultView}. */
  private String lastQuery;
  
  private String inFlightQuery;
  
  private Node result;
  
  /**
   * 
   */
  public QueryView(AppState appState) {
    
    this.appState = appState;
    
    for (int count = GRID_ROWS - 1; count-- > 0; )
      gridPane.getRowConstraints().add(new RowConstraints());
    
    var lastRowConstraints = new RowConstraints();
    lastRowConstraints.setVgrow(Priority.ALWAYS);

    clearButton = new Button("Clear");
    clearButton.setOnAction(_ -> clearQuery());
    submitButton = new Button("Execute");
    submitButton.setOnAction(_ -> execute());
    
    
    

    queryInput.prefColumnCountProperty().set(80);
    queryInput.prefRowCountProperty().set(PREF_LINES_FOR_QUERY);
    
    GridPane.setRowIndex(queryInput, 0);
    GridPane.setColumnIndex(queryInput, 0);
    GridPane.setColumnSpan(queryInput, GRID_COLS);
    
    gridPane.add(queryInput, 0, 0, GRID_COLS, 1);
    gridPane.add(clearButton, GRID_COLS - 2, 1);
    gridPane.add(submitButton, GRID_COLS - 1, 1);
    
    {
      Text noResults = new Text("No Results");
      noResults.getStyleClass().add("text-empty");
      setResult(noResults);
    }
    
  }
  
  
  
  private void setResult(Node view) {
    if (result != null)
      gridPane.getChildren().remove(result);
    
    result = view;
    gridPane.add(result, 0, GRID_ROWS - 1, GRID_COLS, 1);
  }
  
  
  private void clearQuery() {
    queryInput.setText("SELECT ");
    unlockControls();
  }
  
  
  private void execute() {
    String sql = normalizeQueryInput();
    
    if (sql.equals(lastQuery))
      return;
    
    ResultSetView cached = queryCache.get(sql);
    
    if (cached != null) {
      setQueryResult(sql, cached, false);
      return;
    }
    
    if (sql.length() < 8) {
      Alert alert = new Alert(AlertType.ERROR);
      alert.setTitle("SQL Error");
      alert.setHeaderText("Invalid SQL Query");
      alert.setContentText("SQL query is incomplete.");
      alert.showAndWait();
      queryInput.requestFocus();
      return;
    }
    
    lockControls(sql);
    
    Thread.ofVirtual().start(() -> executeQuery(sql));
  }
  
  
  private void executeQuery(String sql) { 
    
    try (var closer = new TaskStack()) {
      Connection con = closer.push(appState.openConnection());
      Statement stmt = closer.push(con.createStatement());
      ResultSet rs = closer.push(stmt.executeQuery(sql));
      ResultSetView result = new ResultSetView(rs);
      Platform.runLater(() -> setQueryResult(sql, result, true));
    } catch (SQLException sx) {
      Platform.runLater(() -> alertSqlError(sql, sx));
    }
  }
  
  
  private void alertSqlError(String sql, SQLException sx) {
    if (!normalizeQueryInput().equals(sql))
      return;
    
    

    Alert alert = new Alert(AlertType.ERROR);
    alert.setTitle("SQL Error");
    alert.setHeaderText("SQL Query Failed");
    var content = new StringBuilder("Failed to execute query.");
    String msg = sx.getMessage();
    if (msg != null && !msg.isBlank())
      content.append("\nError Message: ").append(msg);
    var cause = sx.getCause();
    if (cause != null) {
      content.append("\nCaused By: ").append(cause);
    }
    content.append("\nDB Error Code: ").append(sx.getErrorCode());
    
    var debug = System.out;
    debug.println(this + ".alertSqlError: SQLState(?) is " + sx.getSQLState());
    
    alert.setContentText(content.toString());
    alert.showAndWait();
    unlockControls();
    queryInput.requestFocus();
    return;
  }
  
  
  private void lockControls(String sql) {
    queryInput.setEditable(false);
    submitButton.setDisable(true);
    inFlightQuery = sql;
  }
  
  
  private void unlockControls() {
    queryInput.setEditable(true);
    submitButton.setDisable(false);
    inFlightQuery = null;
  }
  
  
  
  private String normalizeQueryInput() {
    String sql = queryInput.getText().trim();
    if (sql.endsWith(";"))
      sql = sql.substring(0, sql.length() - 1);
    if (sql.isEmpty())
      return "";
    StringTokenizer tokenizer = new StringTokenizer(sql);
    StringBuilder n = new StringBuilder(100);
    while (tokenizer.hasMoreTokens())
      n.append(tokenizer.nextToken()).append(' ');
    n.setLength(n.length() - 1);
    return n.toString();
  }
  
  
  private void setQueryResult(String sql, ResultSetView view, boolean cache) {
    if (cache)
      this.queryCache.put(sql, view);
    
    if (!sql.equals(inFlightQuery))
      return;
    
    var tableView = view.tableView();
    gridPane.getChildren().remove(result);
    
    result = view.tableView();
    
    GridPane.setConstraints(
        result, 0, GRID_ROWS - 1, GRID_COLS, 1);
    
    gridPane.getChildren().add(result);
    lastQuery = sql;
    unlockControls();
  }

}


















