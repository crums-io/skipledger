/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.model;


import java.util.Objects;
import java.util.Optional;

/**
 * A named parameter with optional description, and optional default value
 * 
 * @param name          is trimmed; should be neither null, nor blank
 * @param description   optional (may be null)
 * @param defaultValue  optional (may be null)
 * 
 * @param <T> default value type (only numbers and strings anticipated)
 */
public record Param<T>(String name, String description, T defaultValue) {
  
  public Param {
    name = Objects.requireNonNull(name, "null name").trim();
    if (name.isEmpty())
      throw new IllegalArgumentException("blank name");
    if (description != null && description.isBlank())
      description = null;
  }
  
  
  
  public Param(String name, String description) {
    this(name, description, null);
  }

  
  
  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
  
  public Optional<T> getDefaultValue() {
    return Optional.ofNullable(defaultValue);
  }
  
}







