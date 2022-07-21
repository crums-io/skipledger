/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import org.junit.jupiter.api.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.reports.pdf.CellDataProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.DateProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.ImageProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.NumberProvider;
import io.crums.sldg.reports.pdf.CellDataProvider.StringProvider;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class CellDataProviderTest extends SelfAwareTestCase implements ParserRoundtripTest<CellDataProvider<?>> {
  
  private boolean print;
  
  private Object methodLabel;

  @Test
  public void testString() throws Exception {
    testRoundtrip(new StringProvider());
    testRoundtrip(new StringProvider("# "));
    testRoundtrip(new StringProvider("~", "!"));
  }
  

  @Test
  public void testNumber() throws Exception {
    methodLabel = new Object() {  };
    testRoundtrip(new NumberProvider());
    testRoundtrip(new NumberProvider("###,###.##"));
    testRoundtrip(new NumberProvider("###,###.##", "$"));
    print = true;
    testRoundtrip(new NumberProvider("###,###.##", "$", " (market)"));
    print = false;
  }

  

  @Test
  public void testDate() throws Exception {
    testRoundtrip(new DateProvider("EEE, d MMM yyyy HH:mm:ss Z z"));
  }
  

  @Test
  public void testImage() throws Exception {
    methodLabel = new Object() {  };
    print = true;
    testRoundtrip(new ImageProvider(75, 185));
  }
  
  
  
  
  
  
  
  
  @Override
  public JsonEntityParser<CellDataProvider<?>> parser() throws Exception {
    return CellDataProviderParser.INSTANCE;
  }
  
  
  
  @Override
  public void observeJson(JSONObject jObj, CellDataProvider<?> expected) {
    if (!print)
      return;
    if (methodLabel != null)
      System.out.println(" -- " + method(methodLabel));
    JsonPrinter.println(jObj);
  }

}









