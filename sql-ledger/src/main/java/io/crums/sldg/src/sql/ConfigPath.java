/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;

import java.io.File;
import java.util.Objects;
import java.util.Optional;

/**
 * 
 */
public class ConfigPath {
  
  public record Filepaths(
      File ledgerDef,
      Optional<File> dbConnection,
      Optional<File> saltSeed,
      Optional<File> dbCredentials) {
    
    public Filepaths {
      Objects.requireNonNull(ledgerDef, "null ledgerDef file");
      dbConnection = nullAsEmpty(dbConnection);
      saltSeed = nullAsEmpty(saltSeed);
      dbCredentials = nullAsEmpty(dbCredentials);
    }
    
    private Optional<File> nullAsEmpty(Optional<File> file) {
      return file == null ? Optional.empty() : file;
    }
  }
  
  
  private final Filepaths paths;

  /**
   * 
   */
  public ConfigPath(Filepaths paths) {
    this.paths = Objects.requireNonNull(paths, "null paths");
  }

}
