/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import static io.crums.sldg.mc.fx.AppConstants.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import io.crums.util.TaskStack;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;

/**
 * 
 */
class QueryViewV1 {
  
  final static int PREF_LINES_FOR_QUERY = 5;
  
  private final SplitPane splitPane = new SplitPane();
  
  private final VBox results = new VBox(RIBBON_SPACING);
  
  private final TextArea queryInput = new TextArea("SELECT ");
  
  private final Button clearButton;
  private final Button submitButton;
  
  private final AppState appState;
  
  private final Map<String, ResultSetView> queryCache = new HashMap<>();
  
  /** Last successful query (shown by {@linkplain #queryResultView}. */
  private String lastQuery;
  
  /** Query result view for {@linkplain #lastQuery}. TODO: remove */
  private ResultSetView queryResultView;
  
  private String inFlightQuery;

  /**
   * 
   */
  QueryViewV1(AppState appState) {
    this.appState = appState;
//    this.queryBus = appState.newQueryBus(ResultSetView::new);
    
    
    clearButton = new Button("Clear");
    clearButton.setOnAction(_ -> clearQuery());
    submitButton = new Button("Execute");
    submitButton.setOnAction(_ -> execute());
    
    splitPane.setOrientation(Orientation.VERTICAL);
    queryInput.prefColumnCountProperty().set(80);
    queryInput.prefRowCountProperty().set(PREF_LINES_FOR_QUERY);
    
    results.setAlignment(Pos.CENTER);
    results.prefHeight(VIEW_HEIGHT);
    results.setMaxHeight(Double.MAX_VALUE);
    {
      Text noResults = new Text("No Results");
      noResults.getStyleClass().add("text-empty");
      results.getChildren().setAll(noResults);
    }
    VBox queryBox = new VBox();
    {
      Label label = new Label("SQL Query");
      HBox.setMargin(label, new Insets(2));
      HBox labelBox = new HBox(label);
      labelBox.setAlignment(Pos.CENTER);
      labelBox.setMaxHeight(Double.MAX_VALUE);
      queryBox.getChildren().add(labelBox);
    }
    queryBox.getChildren().add(queryInput);
    
    {
      
      HBox ribbon = new HBox(RIBBON_SPACING);
      ribbon.setAlignment(Pos.BASELINE_RIGHT);
      ribbon.getChildren().setAll(clearButton, submitButton);
      queryBox.getChildren().add(ribbon);
    }
    splitPane.getItems().setAll(queryBox, results);
    splitPane.prefHeight(VIEW_HEIGHT);
  }
  
  
  public SplitPane view() {
    return splitPane;
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
    
    
    results.getChildren().setAll(view.tableView());
    this.queryResultView = view;
    lastQuery = sql;
    unlockControls();
  }

}












