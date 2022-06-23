/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import io.crums.sldg.reports.pdf.BorderContent;
import io.crums.sldg.reports.pdf.Header;
import io.crums.sldg.reports.pdf.LegacyTableTemplate;
import io.crums.sldg.reports.pdf.ReportTemplate;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class ReportTemplateParser implements JsonEntityParser<ReportTemplate> {
  
  /**
   * <em>Sans-ref</em> instance (no images) always suitable for writing JSON.
   * (The referenced images are only needed on the read-path).
   */
  public final static ReportTemplateParser WRITER_INSTANCE = new ReportTemplateParser();
  
  public final static String OBJ_REFS = "objRefs";
  
  public final static String PAGE_SPEC = "pageSpec";
  public final static String W = "w";
  public final static String H = "h";

  public final static String MARGINS = "margins";
  public final static String MARGIN_L = "marginL";
  public final static String MARGIN_R = "marginR";
  public final static String MARGIN_T = "marginT";
  public final static String MARGIN_B = "marginB";
  

  
  public final static String HEAD_CONTENT = "headContent";
  public final static String HEAD_TABLE = "headTable";
  public final static String SUBHEAD_TABLE = "subheadTable";
  
  public final static String MAIN_TABLE = "mainTable";
  
  public final static String FOOTER = "footer";
  
  
  private final Map<String, ByteBuffer> refedImages;

  /**
   * Creates an instance with no referenced images.
   * 
   * @see #WRITER_INSTANCE
   */
  public ReportTemplateParser() {
    this.refedImages = Map.of();
  }

  /**
   * @param refedImages named raw (image) bytes, not null, empty OK.
   *        These are passed around the parser "pipeline" and are packaged as
   *        a component of a {@linkplain RefContext} instance.
   */
  public ReportTemplateParser(Map<String, ByteBuffer> refedImages) {
    this.refedImages = Objects.requireNonNull(refedImages, "null refedImages");
  }
  
  

  /**
   * <p>
   * All info but the image bytes (if any) is recorded in the given JSON object.</p>
   * {@inheritDoc}
   */
  @Override
  public JSONObject injectEntity(ReportTemplate report, JSONObject jObj) {
    injectReferences(report, jObj);
    injectPageSpec(report, jObj);
    injectHeader(report, jObj);
    injectSubheader(report, jObj);
    var refs = report.getReferences();
    jObj.put(
        MAIN_TABLE,
        TableTemplateParser.INSTANCE.toJsonObject(
            report.getMainTable(), refs)
        );
    report.getFooter().ifPresent(
        footer -> jObj.put(
            FOOTER,
            BorderContentParser.INSTANCE.toJsonObject(
                footer, refs)
            ) );
    return jObj;
  }
  
  
  private void injectReferences(ReportTemplate report, JSONObject jObj) {
    var context = report.getReferences();
    if (context.isEmptyJson())
      return;
    jObj.put(
        OBJ_REFS,
        RefContextParser.INSTANCE.toJsonObject(context));
  }

  private void injectPageSpec(ReportTemplate report, JSONObject jObj) {
    var jPageSpec = new JSONObject();
    jPageSpec.put(W, report.getPageWidth());
    jPageSpec.put(H, report.getPageHeight());
    if (report.sameMargins())
      jPageSpec.put(MARGINS, report.getMarginLeft());
    else {
      jPageSpec.put(MARGIN_L, report.getMarginLeft());
      jPageSpec.put(MARGIN_R, report.getMarginRight());
      jPageSpec.put(MARGIN_T, report.getMarginTop());
      jPageSpec.put(MARGIN_B, report.getMarginBottom());
    }
    jObj.put(PAGE_SPEC, jPageSpec);
  }

  @Override
  public ReportTemplate toEntity(JSONObject jObj) throws JsonParsingException {
    return toReportTemplate(jObj, this.refedImages);
  }

  public ReportTemplate toReportTemplate(JSONObject jObj, Map<String, ByteBuffer> refedImages)
      throws JsonParsingException {
    try {
      ReportTemplate report;
      {
        var context = toRefContext(jObj, refedImages);
        var header = toHeader(jObj, context);
        var subheader = toSubheader(jObj, context);
        
        var mainTable =
            TableTemplateParser.INSTANCE.toTableTemplate(
                JsonUtils.getJsonObject(jObj, MAIN_TABLE, true), context);
        BorderContent footer;
        {
          var jFooter = JsonUtils.getJsonObject(jObj, FOOTER, false);
          footer =
              jFooter == null ?
                  null : BorderContentParser.INSTANCE.toEntity(jFooter, context);
        }
        report = new ReportTemplate(header, subheader, mainTable, footer);
      }
      var jPageSpec = JsonUtils.getJsonObject(jObj, PAGE_SPEC, true);
      float width = JsonUtils.getNumber(jPageSpec, W, true).floatValue();
      float height = JsonUtils.getNumber(jPageSpec, H, true).floatValue();
      report.setPageSize(width, height);
      var margins = JsonUtils.getNumber(jPageSpec, MARGINS, false);
      if (margins == null) {
        report.setMarginLeft(JsonUtils.getNumber(jObj, MARGIN_L, true).floatValue());
        report.setMarginRight(JsonUtils.getNumber(jObj, MARGIN_R, true).floatValue());
        report.setMarginTop(JsonUtils.getNumber(jObj, MARGIN_T, true).floatValue());
        report.setMarginBottom(JsonUtils.getNumber(jObj, MARGIN_B, true).floatValue());
      } else
        report.setMargins(margins.floatValue());
      
      return report;
      
    } catch (IllegalArgumentException iax) {
      throw new JsonParsingException("on argument " + jObj + ": " + iax.getMessage());
    }
  }
  
  
  private RefContext toRefContext(JSONObject jObj, Map<String, ByteBuffer> refedImages) {
    var jRefs = JsonUtils.getJsonObject(jObj, OBJ_REFS, false);
    return
        jRefs == null ?
            RefContext.ofImageRefs(refedImages) :
              RefContextParser.INSTANCE.toRefContext(jRefs, refedImages);
  }
  
  
  private void injectHeader(ReportTemplate report, JSONObject jObj) {
    var header = report.getHeader();
    var refs = report.getReferences();
    header.getHeadContent().ifPresent(
        bc -> jObj.put(
            HEAD_CONTENT,
            BorderContentParser.INSTANCE.toJsonObject(bc, refs)));
    jObj.put(
        HEAD_TABLE,
        FixedTableParser.INSTANCE.toJsonObject(header.getHeaderTable(), refs));
  }
  
  
  private Header toHeader(JSONObject jObj, RefContext context) throws JsonParsingException {
    var headTable =
        FixedTableParser.INSTANCE.toFixedTable(
            JsonUtils.getJsonObject(jObj, HEAD_TABLE, true), context);
    
    var jHeadContent = JsonUtils.getJsonObject(jObj, HEAD_CONTENT, false);
    var headContent =
        jHeadContent == null ?
            null : BorderContentParser.INSTANCE.toEntity(jHeadContent, context);
    
    return new Header(headTable, headContent);
  }
  

  
  
  private void injectSubheader(ReportTemplate report, JSONObject jObj) {
    var refs = report.getReferences();
    report.getSubHeader().ifPresent(
        table -> jObj.put(
            SUBHEAD_TABLE,
            TableTemplateParser.INSTANCE.toJsonObject(table, refs)));
  }
  
  
  private LegacyTableTemplate toSubheader(JSONObject jObj, RefContext context) throws JsonParsingException {
    var jSubheader = JsonUtils.getJsonObject(jObj, SUBHEAD_TABLE, false);
    return jSubheader == null ? null : TableTemplateParser.INSTANCE.toEntity(jSubheader, context);
  }
}























