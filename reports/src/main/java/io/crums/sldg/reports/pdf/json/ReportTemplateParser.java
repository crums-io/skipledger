/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.json;


import java.nio.ByteBuffer;
import java.util.Map;

import io.crums.sldg.reports.pdf.BorderContent;
import io.crums.sldg.reports.pdf.Header;
import io.crums.sldg.reports.pdf.ReportTemplate;
import io.crums.sldg.reports.pdf.TableTemplate;
import io.crums.sldg.reports.pdf.input.Query;
import io.crums.util.json.JsonEntityParser;
import io.crums.util.json.JsonParsingException;
import io.crums.util.json.JsonUtils;
import io.crums.util.json.simple.JSONObject;

/**
 * The {@linkplain ReportTemplate} parser. This is stateful parser; not safe
 * under concurrent access, particularly on the write path.
 * 
 * @see #setRefedImages(Map)
 * @see #setReferences(RefContext)
 * @see #getReferences()
 */
public class ReportTemplateParser implements JsonEntityParser<ReportTemplate> {
  
  public final static String QUERY = "query";
  
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
  
  
  
  

  
  
  private RefContext refs = RefContext.EMPTY;
  
  private Map<String, ByteBuffer> refedImages = Map.of();
  

  
  

  /**
   * Returns the references. After a <em>read</em> operation (eg {@linkplain #toEntity(JSONObject)}
   * this returns the references that were defined in the JSON; on a <em>write</em> operation
   * (eg {@code toJsonObject(..)}), these references (which may be set by the user) are used
   * wherever applicable.
   * 
   * @see #setReferences(RefContext)
   */
  public final RefContext getReferences() {
    return refs;
  }
  
  /**
   * Sets the references on the <em>write</em>-path. (On the <em>read</em>-path,
   * the references are read in and set at the conclusion, so this method has
   * would have no effect.) Some downstream parsers use this to decide whether
   * to write out the object defintion or instead write a reference to it.
   * <p>
   * In previous prototypes this lived in the ReportTemplate object itself, but it properly
   * belongs here.
   * </p>
   * @param references    null means empty
   * @return {@code this}
   */
  public ReportTemplateParser setReferences(RefContext references) {
    this.refs = references == null ? RefContext.EMPTY : references;
    return this;
  }
  
  /**
   * Sets the referenced images on the <em>read</em>-path. You don't need to
   * set this on the <em>write</em>-path.
   * 
   * @param refedImages
   * @return {@code this}
   */
  public ReportTemplateParser setRefedImages(Map<String, ByteBuffer> refedImages) {
    this.refedImages = refedImages == null ? Map.of() : refedImages;
    return this;
  }
  
  /** @see #setRefedImages(Map) */
  public final Map<String, ByteBuffer> getRefedImages() {
    return refedImages;
  }
  
  
  
  
  
  
  
  
  
  
  

  @Override
  public JSONObject injectEntity(ReportTemplate report, JSONObject jObj) {
    var jRefs = new JSONObject();
    jObj.put(OBJ_REFS, jRefs);
    
    injectQuery(report, jObj);
    injectPageSpec(report, jObj);
    injectHeader(report, jObj);
    injectSubheader(report, jObj);;
    jObj.put(
        MAIN_TABLE,
        TableTemplateParser.INSTANCE.toJsonObject(
            report.getComponents().mainTable(), refs)
        );
    report.getComponents().footer().ifPresent(
        footer -> jObj.put(
            FOOTER,
            BorderContentParser.INSTANCE.toJsonObject(footer, refs)) );
    injectReferences(report, jObj, jRefs);
    return jObj;
  }

  @Override
  public ReportTemplate toEntity(JSONObject jObj) throws JsonParsingException {
    try {
      ReportTemplate report;
      RefContext context;
      {
        context = toRefContext(jObj, refedImages);
        var query = toQuery(jObj, context);
        var header = toHeader(jObj, context);
        var subheader = toSubheader(jObj, context);
        
        var mainTable =
            TableTemplateParser.INSTANCE.toEntity(
                JsonUtils.getJsonObject(jObj, MAIN_TABLE, true), context);
        BorderContent footer;
        {
          var jFooter = JsonUtils.getJsonObject(jObj, FOOTER, false);
          footer =
              jFooter == null ?
                  null : BorderContentParser.INSTANCE.toEntity(jFooter, context);
        }
        var components = new ReportTemplate.Components(header, subheader, mainTable, footer);
        report = new ReportTemplate(components, query);
      }
      setPageSpec(report, jObj);
      
      // save the context, before returning the object
      this.refs = context;
      
      return report;
      
    } catch (IllegalArgumentException | NullPointerException iax) {
      throw new JsonParsingException("on argument " + jObj + ": " + iax.getMessage());
    }
  }
  
  
  
  
  
  
  
  

  private void injectReferences(ReportTemplate report, JSONObject jObj, JSONObject jRefs) {
    RefContext context;
    if (report.getQuery() != null &&
        !report.getQuery().getNumberArgs().isEmpty()) {
      
      var editContext = new EditableRefContext(refs);
      report.getQuery().getNumberArgs().forEach(editContext::putNumberArg);
      context = editContext;
    } else
      context = refs;
    
    if (context.isEmptyJson())
      jObj.remove(OBJ_REFS);
    else
      RefContextParser.INSTANCE.injectEntity(context, jRefs);
      
  }
  
  
  private RefContext toRefContext(JSONObject jObj, Map<String, ByteBuffer> refedImages) {
    var jRefs = JsonUtils.getJsonObject(jObj, OBJ_REFS, false);
    return
        jRefs == null ?
            RefContext.ofImageRefs(refedImages) :
              RefContextParser.INSTANCE.toRefContext(jRefs, refedImages);
  }

  
  

  private void injectQuery(ReportTemplate report, JSONObject jObj) {
    var query = report.getQuery();
    if (query != null)
      jObj.put(QUERY, QueryParser.INSTANCE.toJsonObject(query));
    
  }
  
  
  private Query toQuery(JSONObject jObj, RefContext context) {
    return QueryParser.INSTANCE.parseIfPresent(jObj, QUERY, context);
  }
  
  
  
  

  private void setPageSpec(ReportTemplate report, JSONObject jObj) {
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

  
  private void injectHeader(ReportTemplate report, JSONObject jObj) {
    var header = report.getComponents().header();
    header.headContent().ifPresent(
        bc -> jObj.put(
            HEAD_CONTENT,
            BorderContentParser.INSTANCE.toJsonObject(bc, refs)));
    jObj.put(
        HEAD_TABLE,
        TableTemplateParser.INSTANCE.toJsonObject(header.headerTable(), refs));
  }
  
  
  private Header toHeader(JSONObject jObj, RefContext context) throws JsonParsingException {
    var headTable =
        TableTemplateParser.INSTANCE.toEntity(
            JsonUtils.getJsonObject(jObj, HEAD_TABLE, true), context);
    
    var jHeadContent = JsonUtils.getJsonObject(jObj, HEAD_CONTENT, false);
    var headContent =
        jHeadContent == null ?
            null : BorderContentParser.INSTANCE.toEntity(jHeadContent, context);
    
    return new Header(headTable, headContent);
  }
  
  
  
  private void injectSubheader(ReportTemplate report, JSONObject jObj) {
    report.getComponents().subHeader().ifPresent(
        table -> jObj.put(
            SUBHEAD_TABLE,
            TableTemplateParser.INSTANCE.toJsonObject(table, refs)));
  }
  
  private TableTemplate toSubheader(JSONObject jObj, RefContext context) throws JsonParsingException {
    var jSubheader = JsonUtils.getJsonObject(jObj, SUBHEAD_TABLE, false);
    return jSubheader == null ? null : TableTemplateParser.INSTANCE.toEntity(jSubheader, context);
  }

}














