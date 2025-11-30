/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import static io.crums.sldg.mc.fx.AppConstants.LOGO_PATH;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import io.crums.sldg.mc.mgmt.DbEnv;
import io.crums.sldg.mc.mgmt.MicrochainManager;
import io.crums.util.Lists;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.geometry.Side;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.BackgroundImage;
import javafx.scene.layout.BackgroundPosition;
import javafx.scene.layout.BackgroundRepeat;
import javafx.scene.layout.BackgroundSize;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Paint;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

/**
 * Workspace configuration view.
 * 
 */
public class WorkspaceConfig extends SceneBuilder {


  record WorkspaceBuilder(
      String name, File hikariConfig, File driver, String tablePrefix) {
    
    
    WorkspaceBuilder {
      name = name == null ? "" : name.trim();
      
      if (tablePrefix != null) {
        tablePrefix = tablePrefix.trim();
        if (tablePrefix.isEmpty())
          tablePrefix = null;
      }
    }
    
    WorkspaceBuilder() {
      this("", null, null, null);
    }
    
    WorkspaceBuilder name(String newName) {
      return new WorkspaceBuilder(newName, hikariConfig, driver, tablePrefix);
    }
    
    WorkspaceBuilder hikariConfig(File newConfig) {
      return new WorkspaceBuilder(name, newConfig, driver, tablePrefix);
    }
    
    WorkspaceBuilder driver(File newDriver) {
      return new WorkspaceBuilder(name, hikariConfig, newDriver, tablePrefix);
    }
    
    WorkspaceBuilder tablePrefix(String newPrefix) {
      return new WorkspaceBuilder(name, hikariConfig, driver, newPrefix);
    }
    
    
    boolean complete() {
      return !name.isEmpty() && hikariConfig != null && driver != null;
    }
    
    
    Workspace build() {
      return new Workspace(name, hikariConfig, driver, tablePrefix);
    }
    
  }
  
  private final static int SPLASH_WIDTH = 800;
  private final static int SPLASH_HEIGHT = 500;
  
  private final static int SPLASH_INSET = 15;
  private final static int SPLASH_RECT_INSET = 30;
  private final static int CONFIG_CONTROLS_TOP_INSET = SPLASH_RECT_INSET * 3;
  private final static int CONFIG_CONTROLS_SEP = 10;
  
  private final static String DRIVER_BUTTON_TXT = "Driver";
  private final static String HIKARI_BUTTON_TXT = "Settings";
  private final static String SAVE_BUTTON_TXT = "Save";
  
  
  private final static int NAME_PREF_WIDTH = 300;
  private final static int BUTTON_MIN_WIDTH = 75;
  private final static int BUTTON_SETTING_SEP = 5;
  
  
  

  private final TextField name = new TextField();
  private final TextField driverSetting = new TextField();
  private final TextField hikariProperties = new TextField();
  private final Button saveButton = new Button(SAVE_BUTTON_TXT);
  
  private final AppState appState;
  
  private final Set<String> wsNames;
  
//  private final Stage primaryStage;
  
  private WorkspaceBuilder builder;
  
  
  

  /**
   * 
   */
  WorkspaceConfig(Stage primaryStage, AppState appState) {
//    this.primaryStage = primaryStage;
    this.appState = appState;
    this.wsNames = new HashSet<>(Lists.map(appState.listWorkspaces(), Workspace::name));
    this.builder = new WorkspaceBuilder();
    
    
    StackPane stackPane = new StackPane();
    
    Rectangle frame = new Rectangle();
    frame.getStyleClass().add("round-rect");
    
    StackPane.setMargin(frame, new Insets(SPLASH_INSET));
    StackPane.setAlignment(frame, Pos.TOP_CENTER);
    
    frame.widthProperty().bind(stackPane.widthProperty().subtract(SPLASH_INSET * 2));
    frame.heightProperty().bind(
        stackPane.heightProperty().subtract(SPLASH_INSET * 2));
    
    stackPane.getChildren().add(frame);
    
    Text heading = new Text("Configure Workspace");
    
    heading.getStyleClass().add("text-announce");
    StackPane.setMargin(heading, new Insets(SPLASH_RECT_INSET));
    StackPane.setAlignment(heading, Pos.TOP_CENTER);
    
    stackPane.getChildren().add(heading);
    
    VBox controls = new VBox(CONFIG_CONTROLS_SEP);
    
    StackPane.setMargin(
        controls,
        new Insets(
            CONFIG_CONTROLS_TOP_INSET,
            SPLASH_RECT_INSET,
            SPLASH_RECT_INSET,
            SPLASH_RECT_INSET));
    StackPane.setAlignment(controls, Pos.TOP_CENTER);
    

    controls.setAlignment(Pos.TOP_LEFT);
    

    addSpacer(controls, 10);
    
    {
      Text desc = new Text("Pick a name for the new workspace");
      Label label = new Label("Name:");
      label.setStyle("-fx-font-weight: bold");
      label.setMinWidth(BUTTON_MIN_WIDTH);
      name.setPrefWidth(NAME_PREF_WIDTH);
      HBox hbox = new HBox(BUTTON_SETTING_SEP);
      hbox.getChildren().addAll(label, name);
      controls.getChildren().addAll(desc, hbox);
      name.textProperty().addListener(
          (_, _, newVal) -> setName(newVal));
    }

    addSpacer(controls, 10);
    
    addFileChooserSetting(
        primaryStage,
        controls,
        "Set the JDBC driver (a .jar file) for the database",
        DRIVER_BUTTON_TXT,
        "JAR Files", "*.jar",
        driverSetting,
        this::driverSelected);

    addSpacer(controls, 10);
    
    addFileChooserSetting(
        primaryStage,
        controls,
        new Text("Set the connection properties (a .properties file)"),
        HIKARI_BUTTON_TXT,
        hikariExtFilters(),
        hikariProperties,
        this::hikariSelected);
    
    addFlexibleSpacer(controls);
    
    
    saveButton.setOnAction(
        _ -> save());
    saveButton.setDisable(true);
    saveButton.setMinWidth(BUTTON_MIN_WIDTH);
    controls.getChildren().add(saveButton);
    
    
    stackPane.getChildren().add(controls);
    
    
    
    stackPane.backgroundProperty().set(logoBackground());
    
    Scene scene = new Scene(stackPane, SPLASH_WIDTH, SPLASH_HEIGHT);
    applyStyles(scene);
    
    primaryStage.setScene(scene);
    primaryStage.show();
  }
  
  
  private void setName(String wsName) {
    builder = builder.name(wsName);
    
    
    enableSave();
  }
  
  
  private void save() {
    if (!builder.complete()) 
      throw new IllegalStateException("builder is not complete");
    
    Workspace workspace = builder.build();
    try {
      
      workspace.loadDataSource();
      
    } catch (Workspace.ConfigException wcx) {
      
      Alert alert = new Alert(AlertType.ERROR);
      alert.setTitle("Configuration Error");
      String header, msg;
      TextField inError;
      if (workspace.drivers().isEmpty()) {
        inError = driverSetting;
        header = "JDBC Driver Not Found";
        msg = "No JDBC driver found in " + builder.driver().getName();
      } else {
        inError = hikariProperties;
        header = "Bad Connection Properties";
        msg =
            "Failed to load Hikari data source using "
            + builder.hikariConfig().getName() + ": " + wcx.getMessage();
      }
      setError(inError);
      alert.setHeaderText(header);
      alert.setContentText(msg);
      alert.showAndWait();
      return;
    }
    
    appState.newWorkspace(workspace);
    appState.fireChange();
  }
  
  
  
  private List<FileChooser.ExtensionFilter> hikariExtFilters() {
    
    List<String> exts = Arrays.asList(AppConstants.HikariExtension.values())
        .stream().map(e -> "*" + e.ext).toList();
    return 
        List.of(new FileChooser.ExtensionFilter("Hikari Properties File", exts));
  }
  
  
  private void addSpacer(VBox controls, int height) {
    Region spacer = new Region();
    spacer.setPrefHeight(height);
    controls.getChildren().add(spacer);
  }
  
  
  private void addFlexibleSpacer(VBox controls) {
    Region spacer = new Region();
    VBox.setVgrow(spacer, Priority.ALWAYS);
    controls.getChildren().add(spacer);
  }
  
  
  private void addFileChooserSetting(
      Stage primaryStage,
      VBox controls,
      String sectionText,
      String buttonText, String extDesc, String extFilter,
      TextField currentSetting,
      Consumer<File> selectionCallback) {
    
    addFileChooserSetting(
        primaryStage,
        controls,
        new Text(sectionText),
        buttonText,
        List.of( new FileChooser.ExtensionFilter(extDesc, extFilter) ),
        currentSetting,
        selectionCallback);
  }
  
  
  private void addFileChooserSetting(
      Stage primaryStage,
      VBox controls,
      Node desc,
      String buttonText, List<FileChooser.ExtensionFilter> filters,
      TextField currentSetting,
      Consumer<File> selectionCallback) {
    
    System.out.println(this + ": " + filters);
    Button button = new Button(buttonText);
    FileChooser chooser = new FileChooser();
    chooser.getExtensionFilters().addAll(filters);
    button.setOnAction(
        _ -> selectionCallback.accept(chooser.showOpenDialog(primaryStage)));
    currentSetting.setEditable(false);
    currentSetting.setPrefWidth(SPLASH_WIDTH);
    button.setMinWidth(BUTTON_MIN_WIDTH);
    
    HBox hbox = new HBox(BUTTON_SETTING_SEP);
    hbox.getChildren().addAll(button, currentSetting);
    
    controls.getChildren().addAll(desc, hbox);
  }
  
  
  
  
  
  
  private void driverSelected(File jar) {
    if (jar == null)
      return;
    builder = builder.driver(jar);
    setFile(driverSetting, jar);
    enableSave();
  }
  
  private void setFile(TextField setting, File file) {
    setting.setText(file.getName());
    setting.setStyle("-fx-text-fill: black;");
  }
  
  
  private void setError(TextField setting) {
    setting.setStyle("-fx-text-fill: red;");    
  }
  
  
  
  private void enableSave() {
    if (builder.complete() && !wsNames.contains(builder.name()))
      saveButton.setDisable(false);
  }
 

  private void hikariSelected(File properties) {
    if (properties == null)
      return;
    
    builder = builder.hikariConfig(properties);
    setFile(hikariProperties, properties);
    enableSave();
  }
  
  
  
  
  
  
  
  
//  private Background logoBackground() {
//    BackgroundFill bgFill = new BackgroundFill(
//        Paint.valueOf(AppConstants.BACKGROUND_GRADIENT),
//        null, null);
//    return new Background(List.of(bgFill), List.of(logoBackgroundImage()));
//  }
//  
//  private BackgroundImage logoBackgroundImage() {
//
//    Image logo = new Image(App.class.getResourceAsStream(LOGO_PATH));
//    double aspectRatio = logo.getWidth() / logo.getHeight();
//    
//    return new BackgroundImage(
//        logo, BackgroundRepeat.NO_REPEAT, BackgroundRepeat.NO_REPEAT,
//        new BackgroundPosition(Side.RIGHT, 0.68, true, Side.BOTTOM, 0.025, true),
//        new BackgroundSize((aspectRatio - 1), 1, true, true, false, false));
//        
//  }

}





























