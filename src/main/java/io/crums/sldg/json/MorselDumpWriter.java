/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.json;


import java.util.Iterator;

import io.crums.sldg.SkipLedger;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.json.JsonEntityWriter;
import io.crums.util.json.simple.JSONArray;
import io.crums.util.json.simple.JSONObject;

/**
 * 
 */
public class MorselDumpWriter implements JsonEntityWriter<MorselPack> {
  
  public final static MorselDumpWriter INSTANCE = new MorselDumpWriter();
  
  public final static String INFO = "info";
  
  public final static String ROWS = "rows";
  
  
  private final static SourceInfoParser INFO_PARSER = SourceInfoParser.INSTANCE;
  private final static RowHashWriter ROW_HASH_WRITER = RowHashWriter.INSTANCE;
  private final static RowInputWriter ROW_INPUT_WRITER = RowInputWriter.INSTANCE;
  private final static SourceRowParser SRC_WRITER = SourceRowParser.INSTANCE;
  private final static TrailedRowWriter TRAIL_WRITER = TrailedRowWriter.DEFAULT_INSTANCE;
  
  
  

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject injectEntity(MorselPack pack, JSONObject jObj) {
    var info = pack.getMetaPack().getSourceInfo();
    if (info.isPresent())
      jObj.put(INFO, INFO_PARSER.toJsonObject(info.get()));
    
    var fullRns = pack.getFullRowNumbers();
    var srcRns = pack.sourceRowNumbers();
    var refRns = SkipLedger.refOnlyCoverage(fullRns).tailSet(1L); // exclude row [0]
    var trailedRows = pack.getTrailedRows();
    pack.sourceRowNumbers();
    
    var fullIter = fullRns.iterator();
    var srcIter = srcRns.iterator();
    var refIter = refRns.iterator();
    var trailedIter = trailedRows.iterator();
    
    long headFullRn = fullIter.next();
    long headSrcRn = nextOrMax(srcIter);
    long headRefRn = nextOrMax(refIter);
    var headTrail = nextOrNull(trailedIter);
    long headTrailRn = rnOrMax(headTrail);
    
    var jArray = new JSONArray(fullRns.size() + refRns.size());
    
    while (true) {
      var rowObj = new JSONObject();
      jArray.add(rowObj);
      
      if (headRefRn < headFullRn) {
        ROW_HASH_WRITER.injectRowHash(headRefRn, pack.rowHash(headRefRn), rowObj);
        headRefRn = nextOrMax(refIter);
        continue;
      }
      
      ROW_INPUT_WRITER.injectInputHash(headFullRn, pack.inputHash(headFullRn), rowObj);
      
      if (headSrcRn == headFullRn) {
        SRC_WRITER.injectEntity(pack.getSourceByRowNumber(headFullRn), rowObj);
        headSrcRn = nextOrMax(srcIter);
      }
      
      if (headTrailRn == headFullRn) {
        TRAIL_WRITER.injectEntity(headTrail, rowObj);
        headTrail = nextOrNull(trailedIter);
        headTrailRn = rnOrMax(headTrail);
      }
      
      if (fullIter.hasNext())
        headFullRn = fullIter.next();
      else
        break;
    }
    
    jObj.put(ROWS, jArray);
    
    return jObj;
  }
  
  
  
  

  private long nextOrMax(Iterator<Long> iter) {
    return iter.hasNext() ? iter.next() : Long.MAX_VALUE;
  }
  
  
  private <T> T nextOrNull(Iterator<T> iter) {
    return iter.hasNext() ? iter.next() : null;
  }
  
  
  private long rnOrMax(TrailedRow trailed) {
    return trailed == null ? Long.MAX_VALUE : trailed.rowNumber();
  }

}











