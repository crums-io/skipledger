/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.input;


import java.util.Objects;
import java.util.Optional;

/**
 * A named parameter with optional description, and optional default value.
 * 
 * @param <T> default value type (only numbers and strings anticipated)
 */
public record Param<T>(String name, String description, T defaultValue) {
  
  /**
   * @param name          is trimmed; should be neither null, nor blank
   * @param description   optional (may be null)
   * @param defaultValue  optional (may be null)
   */
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
  
  
  public Param(String name) {
    this(name, null, null);
  }

  
  
  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }
  
  public Optional<T> getDefaultValue() {
    return Optional.ofNullable(defaultValue);
  }
  
  
  /** Returns {@code true} if this parameter has a default value. */
  public boolean hasDefault() {
    return defaultValue != null;
  }
  
  /** @return {@code !hasDefault()} */
  public boolean isRequired() {
    return !hasDefault();
  }
  
}







