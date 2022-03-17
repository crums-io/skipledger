/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;


import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import io.crums.sldg.json.SourceInfoParser;
import io.crums.sldg.src.SourceInfo;
import io.crums.util.Lists;
import io.crums.util.json.JsonParsingException;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.NumbersArg;

/**
 * Common functions in {@linkplain Sldg} and {@linkplain Mrsl} gathered here.
 * (Dups are adding up => more work + maintenance.)
 */
public abstract class BaseMain extends MainTemplate {

  public final static String REDACT = "redact";
  
  public final static String META = "meta";
  
  
  
  /**
   * @return not {@code null}
   */
  protected List<Integer> getRedactColumns(ArgList argList) throws IllegalArgumentException {
    String rCols = argList.removeValue(REDACT);
    if (rCols == null || rCols.isEmpty())
      return Collections.emptyList();

    List<Integer> parsed = NumbersArg.parseInts(rCols);
    if (parsed != null && !parsed.isEmpty()) {
      if (new HashSet<>(parsed).size() != parsed.size())
        throw new IllegalArgumentException(
            "duplicate redact column numbers: " + REDACT + "=" + rCols);
      parsed = new ArrayList<>(parsed);
      Collections.sort(parsed);
      if (parsed.get(0) < 1)
        throw new IllegalArgumentException(
            "illegal (1-based) column number with '" + REDACT + "' option: " + parsed.get(0));
      return parsed;
    }
    
    throw new IllegalArgumentException(
          "RHS of " + REDACT + "=" + rCols + " must parse to (column) numbers");
  }
  
  
  
  protected List<Long> getRowNums(ArgList argList) throws IllegalArgumentException {
    var rowNums = argList.removeNumbers(true);
    if (rowNums.isEmpty())
      throw new IllegalArgumentException("missing row-number[s] arg");
    if (rowNums.get(0) < 1 || !Lists.isSortedNoDups(rowNums))
      throw new IllegalArgumentException("row numbers must be > 0");
    return Lists.sort(rowNums, true);
  }
  
  
  protected SourceInfo getMeta(ArgList argList) {
    String metaPath = argList.removeValue(META);
    if (metaPath == null)
      return null;
    
    File metaFile = new File(metaPath);
    if (!metaFile.isFile())
      throw new IllegalArgumentException("file not found (" + META + "=): " + metaFile);
    try {
      return SourceInfoParser.INSTANCE.toEntity(metaFile);
    
    } catch (JsonParsingException jpx) {
      throw new IllegalArgumentException(
          "failed to load: (" + META + "=" + metaPath + "): " + jpx, jpx);
    }
    
  }

}
