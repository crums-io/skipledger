/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;

import static io.crums.sldg.mc.fx.AppConstants.LOGO_PATH;

import java.util.List;

import javafx.geometry.Side;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.BackgroundImage;
import javafx.scene.layout.BackgroundPosition;
import javafx.scene.layout.BackgroundRepeat;
import javafx.scene.layout.BackgroundSize;
import javafx.scene.paint.Paint;

/**
 * 
 */
class SceneBuilder {

  
  protected void applyStyles(Scene scene) {
    scene.getStylesheets().add(
        SceneBuilder.class.getResource(AppConstants.STYLES_PATH)
        .toExternalForm());
  }
  
  
  protected Background logoBackground() {
    BackgroundFill bgFill = new BackgroundFill(
        Paint.valueOf(AppConstants.BACKGROUND_GRADIENT),
        null, null);
    return new Background(List.of(bgFill), List.of(logoBackgroundImage()));
  }
  
  protected BackgroundImage logoBackgroundImage() {

    Image logo = new Image(App.class.getResourceAsStream(LOGO_PATH));
    double aspectRatio = logo.getWidth() / logo.getHeight();
    
    return new BackgroundImage(
        logo, BackgroundRepeat.NO_REPEAT, BackgroundRepeat.NO_REPEAT,
        new BackgroundPosition(Side.RIGHT, 0.68, true, Side.BOTTOM, 0.025, true),
        new BackgroundSize((aspectRatio - 1), 1, true, true, false, false));
        
  }

}
