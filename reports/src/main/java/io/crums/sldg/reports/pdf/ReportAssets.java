/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import io.crums.io.FileUtils;
import io.crums.io.buffer.NamedParts;
import io.crums.sldg.packs.AssetsBuilder;
import io.crums.sldg.reports.pdf.json.ReportTemplateParser;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.simple.JSONObject;
import io.crums.util.json.simple.parser.JSONParser;
import io.crums.util.json.simple.parser.ParseException;

/**
 * Report assets are stored under the (reserved) {@linkplain AssetsBuilder#SYS_PREFIX}
 * namespace.
 * 
 * @see #setReport(AssetsBuilder, JSONObject, Map)
 * @see #setReport(AssetsBuilder, File)
 * @see #getReport(NamedParts)
 * @see AssetsBuilder#toNamedParts()
 */
public class ReportAssets {
  
  public final static String REPORT = AssetsBuilder.CRUMS + "/report";
  
  public final static String IMAGES_SUBDIR_NAME = "images";
  
  /** (with trailing '/') */
  public final static String IMAGES = REPORT + "/" + IMAGES_SUBDIR_NAME + "/";
  
  private final static int IMAGES_LEN = IMAGES.length();
  
  
  /** Constructs and returns a {@linkplain ReportTemplate}, if present. */
  public static Optional<ReportTemplate> getReport(NamedParts assets) throws JsonParsingException {
    var reportBytes = assets.getPart(REPORT);
    if (reportBytes.isEmpty())
      return Optional.empty();
    
    var imageRefs = new TreeMap<String, ByteBuffer>();
    assets.getPartNames().stream()
    .filter(name -> name.length() > IMAGES_LEN && name.startsWith(IMAGES))
    .forEach(name -> imageRefs.put(name.substring(IMAGES_LEN), assets.part(name)));
    
    var reportTemplate =
        new ReportTemplateParser().setRefedImages(imageRefs).toEntity(
            Strings.utf8String(reportBytes.get()));
    
    return Optional.of(reportTemplate);
  }
  
  

  /**
   * Sets the given JSON reprentation of a {@linkplain ReportTemplate}, along with its
   * image references into the given builder. Overwritting an existing value to a
   * <em>different</em> value is an error.
   * 
   * @param builder   the builder values are set in
   * @param jReport   well formed report JSON
   * @param imageRefs images referenced in report
   * 
   * @throws JsonParsingException  if the arguments do not represent a valid {@code ReportTemplate} instance
   * @throws IllegalStateException if a previous value is overwritten <em>and</em> the
   *                               previous value is not equal to the new value
   */
  public static void setReport(
      AssetsBuilder builder, JSONObject jReport, Map<String, ByteBuffer> imageRefs)
          throws JsonParsingException, IllegalStateException {
    
    new Builder(builder).setReport(jReport, imageRefs);
  }
  
  
  /**
   * Sets the report assets in the given {@code builder} given the following assumed
   * directory structure:
   * <pre>
   *    dir/{filename}.json)
   *    dir/images/{image-files}
   * </pre>
   * The given {@code reportPath} may be either the subdirectory {@code dir} (in which case
   * it must contain exactly one {@code .json} file), or a path to an actual file.
   * 
   * <h3>Image References</h3>
   * <p>
   * Images are loaded from the {@code dir/images} subdirectory (if any). Note all files in the
   * subdirectory should be <em>loadable</em> images by the library. (This method does verify
   * they can indeed be loaded.) Image filenames are interpreted as reference keys. The filename
   * extensions make it in as keys, but they do not figure in how an image is loaded.
   * </p>
   * 
   * @param builder    the builder to be updated
   * @param reportPath path to JSON file, or parent directory
   * 
   * @throws JsonParsingException  if the arguments do not represent a valid {@code ReportTemplate} instance
   * @throws IllegalStateException if a previous value is overwritten <em>and</em> the
   *                               previous value is not equal to the new value
   */
  public static void setReport(AssetsBuilder builder, File reportPath)
      throws JsonParsingException, IllegalStateException {
    
    Objects.requireNonNull(reportPath, "null filepath to report template json");
    if (!reportPath.exists())
      throw new IllegalArgumentException("file or directory not found: " + reportPath);
    

    JSONObject jReport;
    File reportJson;
    
    {
      
      if (reportPath.isDirectory()) {
        File[] files =
            reportPath.listFiles((dir, name) -> name.toLowerCase(Locale.ROOT).endsWith(".json"));
        switch (files.length) {
        case 0:
          throw new IllegalArgumentException("no .json file found in dir " + reportPath);
        case 1:
          break;
        default:
          throw new IllegalArgumentException(
              "ambiguous .json files in dir " + reportPath + " Choices are: " +
               Lists.map(Arrays.asList(files), file -> file.getName()));
        }
        reportJson = files[0];
      } else
        reportJson = reportPath;
      

      try (var reader = new FileReader(reportJson)) {
        
        jReport = (JSONObject) new JSONParser().parse(reader);
      
      } catch (IOException iox) {
        throw new UncheckedIOException(
            "on reading " + reportJson + ": " + iox.getMessage(), iox);
      } catch (ParseException px) {
        throw new JsonParsingException(
            "malformed JSON in " + reportJson  + ": " + px.getMessage(), px);
      }
    }
    
    
    Map<String, ByteBuffer> imageRefs;
    
    {
      File[] images =
          new File(reportJson.getAbsoluteFile().getParentFile(), IMAGES_SUBDIR_NAME)
          .listFiles(f -> f.isFile());
      if (images == null || images.length == 0)
        imageRefs = Map.of();
      else {
        imageRefs = new HashMap<>(Math.max(8, images.length));
        for (var f : images) {
          imageRefs.put(f.getName(), FileUtils.loadFileToMemory(f));
        }
      }
    }
    
    setReport(builder, jReport, imageRefs);
  }
  
  
  
  
  
  
  /**
   * Sets {@linkplain ReportTemplate} assets (bytes) under the {@linkplain AssetsBuilder#SYS_PREFIX}
   * namespace.
   */
  public static class Builder extends AssetsBuilder {
    
    public Builder() {  }
    
    /** Promotion constructor. Is a <em>shallow</em> copy of {@code builder}. */
    public Builder(AssetsBuilder builder) {
      super(Objects.requireNonNull(builder, "null builder"));
    }
    
    
    
    /**
     * @throws JsonParsingException  if the arguments do not represent a valid {@code ReportTemplate} instance
     * @throws IllegalStateException if a previous value is overwritten <em>and</em> the
     *                               previous value is not equal to the new value
     */
    public void setReport(JSONObject jReport, Map<String, ByteBuffer> imageRefs)
        throws JsonParsingException, IllegalStateException {

      // verify well formed
      Objects.requireNonNull(jReport, "null report JSON");
      Objects.requireNonNull(imageRefs, "null image reference map");
      new ReportTemplateParser().setRefedImages(imageRefs).toEntity(jReport);
      
      // ok
      // write the images first
      for (var e : imageRefs.entrySet()) {
        var name = e.getKey();
        if (name.isEmpty())
          throw new IllegalArgumentException(
              "empty ref name for image [" + e.getValue().remaining() + "] bytes");
        
        var prev = setOrRemove(IMAGES + name, e.getValue());
        
        if (prev != null && !prev.equals(e.getValue()))
            throw new IllegalStateException("over-wrote image ref: " + name);
      }
      // write the template json last
      var jsonBytes = ByteBuffer.wrap(Strings.utf8Bytes(jReport.toJSONString()));
      var prev = setOrRemove(REPORT, jsonBytes);
      
      if (prev != null && !prev.equals(jsonBytes))
        throw new IllegalStateException("over-wrote previous report template");
    }
    
  }

}
























