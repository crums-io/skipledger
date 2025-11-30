/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import static io.crums.sldg.mc.fx.AppConstants.RIBBON_SPACING;
import static io.crums.sldg.mc.fx.AppConstants.VIEW_HEIGHT;
import static io.crums.sldg.mc.fx.AppConstants.VIEW_WIDTH;

import java.util.List;

import org.kordamp.ikonli.fontawesome6.FontAwesomeSolid;
import org.kordamp.ikonli.javafx.FontIcon;

import io.crums.sldg.mc.mgmt.ChainInfo;
import io.crums.sldg.mc.mgmt.MicrochainManager;
import io.crums.util.Lists;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Control;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

/**
 * 
 */
public class WorkspaceView extends SceneBuilder {
  
  //  private final Stage primaryStage;
  private final AppState appState;
  
  private final Workspace workspace;
  private final QueryBus<ResultSetView> tableViewBus;
  
  private final ListView<String> microsListView;
  private final ListView<String> tablesListView;
  
  private final Control queryView;
  
  
  private final VBox toc;
  private final VBox rightView;
  

  WorkspaceView(Stage primaryStage, AppState appState) {
//    this.primaryStage = primaryStage;
    this.appState = appState;
    
    this.workspace = appState.currentWorkspace().orElseThrow(
        () -> new IllegalStateException("workspace not set"));
    
    this.tableViewBus = appState.newQueryBus(ResultSetView::new);
    
    this.microsListView = new ListView<>();
    microsListView.getSelectionModel()
      .selectedItemProperty()
      .addListener(
          (_, _, chain) -> chainSelected(chain));
    
    this.tablesListView = new ListView<>();
    tablesListView.getSelectionModel()
      .selectedItemProperty()
      .addListener(
          (_, _, tableName) -> tableSelected(tableName));
    tablesListView.getStyleClass().add("toc-list");
    
    this.queryView = new QueryViewV1(appState).view();
    queryView.prefHeight(VIEW_HEIGHT);
    queryView.setMaxHeight(Double.MAX_VALUE);
    
    this.toc = new VBox();
//    toc.setMaxHeight(Double.MAX_VALUE);
    this.rightView = new VBox();
//    rightView.setMaxHeight(Double.MAX_VALUE);
//    var debug = System.out;
    VBox rootBox = new VBox();
    {
      HBox ribbon = new HBox(RIBBON_SPACING);
      ribbon.setAlignment(Pos.CENTER_RIGHT);
      ribbon.getStyleClass().add("bevel-raised");
      
//      debug.println(this + ": workspace " + workspace.name());
      
      Label wsLabel = new Label(workspace.name());
      wsLabel.getStyleClass().add("bevel-lowered");
      
      FontIcon dbIcon = new FontIcon(FontAwesomeSolid.DATABASE);
      dbIcon.setIconSize(18);
      
      dbIcon.setIconColor(Color.valueOf("7290b0"));
      
      ribbon.getChildren().addAll(dbIcon, wsLabel);
      rootBox.getChildren().add(ribbon);
    }
    
    {
      TitledPane micros = new TitledPane("Microchains", microsListView);
      micros.setExpanded(false);
      
      FontIcon searchIcon = new FontIcon(FontAwesomeSolid.SEARCH);
      Hyperlink search = new Hyperlink("Query Tables");
      search.setOnAction(_-> showQueryView());
      HBox searchBox = new HBox(RIBBON_SPACING);
      searchBox.setAlignment(Pos.CENTER_LEFT);
      Region space = new Region();
      space.setPrefWidth(2);
      searchBox.getChildren().setAll(space, searchIcon, search);
      TitledPane tables = new TitledPane("Tables", tablesListView);
      tables.setExpanded(false);
      toc.getChildren().addAll(micros, searchBox, tables);
      SplitPane splitPane = new SplitPane(toc, rightView);
      splitPane.setDividerPositions(0.3);
      splitPane.setPrefHeight(VIEW_HEIGHT);
      rootBox.getChildren().add(splitPane);
    }
    
//    rootBox.backgroundProperty().set(logoBackground());
    
    
    
    
    Scene scene = new Scene(rootBox, VIEW_WIDTH, VIEW_HEIGHT);
    applyStyles(scene);
    
    primaryStage.setScene(scene);
    primaryStage.show();
    
    Thread.ofVirtual().start(this::initTocViews);
  }
  
  
  
  private void initTocViews() {
    fillMicrosView();
    fillTablesView();
  }
  
  private void fillMicrosView() {
    var chains =
        appState.microManager().map(MicrochainManager::list).orElse(List.of());
    if (chains.isEmpty())
      return;
    
    var aliases = Lists.map(chains, ChainInfo::name);
    Platform.runLater(() -> microsListView.getItems().setAll(aliases));
  }
  
  
  
  private void showQueryView() {;
    clearTableSelection();
    rightView.getChildren().setAll(queryView);
  }
  
  
  private String chainSelected;
  
  private void chainSelected(String chainAlias) {
    System.out.println("TODO " + this + ".chainSelected: " + chainAlias);
    this.chainSelected = chainAlias;
  }
  
  private void fillTablesView() {
    var tables = appState.listTables();
    Platform.runLater(() -> tablesListView.getItems().setAll(tables));
  }
  
  
  
  private void clearTableSelection() {
    tableSelected = null;
    tableView = null;
  }
  
  
  
  private String tableSelected;
  private ResultSetView tableView;
  
  private int limit = 10;
  
  private void tableSelected(String table) {
    tableSelected = table;
    String sql = "SELECT * FROM %s LIMIT %d".formatted(table, limit);
    tableViewBus.get(
        sql,
        view -> explore(table, view));
  }
  
  
  
  
  
  private void explore(String table, ResultSetView view) {

    if (!table.equals(tableSelected) || view == tableView)
      return;
    
    var t = view.tableView();
    t.prefHeight(VIEW_HEIGHT);
    rightView.getChildren().setAll(t);
    this.tableView = view;
  }
  
  
//  @Override
//  public String toString() {
//    return getClass().getSimpleName();
//  }
  

}





