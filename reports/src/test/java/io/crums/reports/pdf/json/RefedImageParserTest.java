/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.reports.pdf.json;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.reports.pdf.ReportTemplateTest;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
abstract class RefedImageParserTest<T> extends SelfAwareTestCase implements ParserRoundtripTest<T> {
  
  protected Object methodLabel;

  @Override
  public void observeJson(JSONObject jObj, T expected) {
    if (methodLabel != null) {
      var out = System.out;
      out.println(" - - - " + method(methodLabel) + " - - -");
      new JsonPrinter(out).print(jObj);
      out.println();
    }
  }
  
  /**
   * Returns a mapping by simple file (resource) name. The named resources (images) are loaded
   * from the parent package {@code io.crums.reports.pdf}.
   * @param resources
   * @return
   * @throws IOException
   */
  protected TreeMap<String, ByteBuffer> getRefedImages(String... resources) throws IOException {
    return ReportTemplateTest.getRefedImages(resources);
  }
  
  
  /**
   * These are loaded the parent package {@code io.crums.reports.pdf}.
   */
  protected ByteBuffer loadResourceBytes(String resourceName) throws IOException {
    return ReportTemplateTest.loadResourceBytes(resourceName);
  }

}
