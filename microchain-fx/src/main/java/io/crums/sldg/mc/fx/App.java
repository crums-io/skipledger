package io.crums.sldg.mc.fx;


import static io.crums.sldg.mc.fx.AppConstants.ICON_PATH;

import javafx.application.Application;
import javafx.scene.image.Image;
import javafx.stage.Stage;



/**
 * JavaFX App
 * <p>
 * Color drop from website: {@code #98bfeb}.
 * </p>
 */
public class App extends Application {

  public static void main(String[] args) {
    launch(args);
  }
  
  
  enum ViewState {
    GREET,
    CONFIG_WORKSPACE,
    SHOW_WORKSPACE;
  }
  
  
  private final Image icon;
  
  private final AppState appState;
  
  private Stage primaryStage;
  private ViewState view;
  
  
  
  public App() {
    this.icon = new Image(App.class.getResourceAsStream(ICON_PATH));
    this.appState = new AppState();
    appState.addListener(_ -> this.stateChanged());
  }
  
  
  @Override
  public void start(Stage primaryStage) {
    
    this.primaryStage = primaryStage;
    primaryStage.getIcons().add(icon);
    primaryStage.setTitle("Microchains");
    
    final int wsCount = appState.listWorkspaces().size();
    switch (wsCount) {
    case 0:
      view = ViewState.CONFIG_WORKSPACE;
      new WorkspaceConfig(primaryStage, appState);
      break;
    case 1:
      // TODO: handle case when misconfigured (outside app)
      appState.openWorkspace(appState.listWorkspaces().get(0).name());
      view = ViewState.SHOW_WORKSPACE;
      new WorkspaceView(primaryStage, appState);
      break;
    default:
      System.out.println("TODO: pick one of " + appState.listWorkspaces());
    }
  }


  void stateChanged() {
    if (appState.currentWorkspace().isPresent()) {
      if (view != ViewState.SHOW_WORKSPACE) {
        view = ViewState.SHOW_WORKSPACE;
        new WorkspaceView(primaryStage, appState);
      }
    }
  }

}

























