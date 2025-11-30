/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

import java.lang.System.Logger;

/**
 * 
 */
public class AppConstants {

  final static int VIEW_WIDTH = 1200;
  final static int VIEW_HEIGHT = 800;

  // no one calls.
  private AppConstants() {  }
  

  
  final static String LOG_NAME = "microchain-ui";
  
  static Logger getLogger() {
    return System.getLogger(LOG_NAME);
  }
  
  final static String ICON_PATH = "/images/icon.png";
  final static String LOGO_PATH = "/images/logo.png";
  
  final static String STYLES_PATH = "/styles.css";
  
  final static String BACKGROUND_GRADIENT =
      "linear-gradient(from 0% 0% to 100% 100%, #98bfeb  0% , #050525 100%)";
  
  /** Either VBox or HBox spacing. */
  final static int RIBBON_SPACING = 8;
  
  enum HikariExtension {
    PROPERTIES(".properties"),
    PROPS(".props"),
    CONFIG(".config"),
    CONF(".conf");
    
    
    final String ext;
    
    HikariExtension(String ext) {
      this.ext = ext;
    }
  }

}
