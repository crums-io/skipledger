/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;

import java.lang.System.Logger;

/**
 * 
 */
public class BindleConstants {
  
  /**
   * Bindle filename extension {@code .bindl}
   * 
   * <h4>Why Not {@code .bindle}?</h4>
   * <p>
   * That would have been better, imo: the choice is a compromise with the common
   * filename extensions {@code .bndl} (game data files) and {@code .bundle} (MacOS).
   * The '{@code e}' in bindl<em>e</em> was dropped because
   * {@code .bindle} and {@code .bundle} were deemed too close (typographically
   * similar, only one character different, plus the fact '{@code i}' and '{@code u}'
   * are adjacent keys on QWERTY keyboards).
   * </p>
   */
  public final static String FILENAME_EXT = ".bindl";
  
  /**
   * Package log name.
   * 
   * @see #getLogger()
   */
  public final static String LOG_NAME = "io.crums.sldg.bindle";
  
  /**
   * Returns the package logger: {@code System.getLogger(LOG_NAME)}
   * @see #LOG_NAME
   */
  public static Logger getLogger() {
    return System.getLogger(LOG_NAME);
  }

  // never
  private BindleConstants() {  }

}
