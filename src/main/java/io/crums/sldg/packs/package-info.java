/*
 * Copyright 2021 Babak Farhang
 */
/**
 * Implementation of the Xxx<em>Bag</em> interfaces.
 * 
 * <h1>Serial Format</h1>
 * <p>
 * 
 * </p>
 * <h2>Base Definitions</h2>
 * <p>
 * <pre>
 *    SHORT       := BYTE ^2
 *    INT         := BYTE ^4
 *    LONG        := BYTE ^8
 *    HASH        := BYTE ^32
 *    
 *    RN          := LONG   // row number
 *    RHASH       := HASH   // hash of entire row (used as the value of a hash pointer)
 *    IHASH       := HASH   // input hash
 *                          // the hash of row is computed from IHASH RHASH^p with the RHASH's
 *                          // being the row's hash [skip] pointers
 *    
 * </pre>
 * <p>
 * 
 * <h2>RowPack</h2>
 * <p>
 * <pre>
 *    
 *    I_COUNT     := INT                // number of rows with full info (have input hash)
 *    FULL_RNS    := RN ^ICOUNT         // full info row numbers in ascending order
 *    
 *    // The following is inferred from FULL_RNS
 *    // see {@linkplain SkipLedger#refOnlyCoverage(java.util.Collection)}
 *    
 *    R_COUNT     := INT                // (INFERRED)
 *     
 *                              
 *    R_TBL       := RHASH ^R_COUNT     // hash pointer table
 *    I_TBL       := IHASH ^I_COUNT     // input hash table
 *    
 *    HASH_TBL    := R_TBL I_TBL
 *    
 *    ROW_PACK    := ICOUNT FULL_RNS HASH_TBL
 * </pre>
 * <p>
 * 
 * 
 * <h2>PathPack</h2>
 * <p>
 * <pre>
 *    PATH_CNT    := SHORT                              // number of declared paths (<= 64k)
 *    
 *    // per PathInfo
 *    ROW_CNT     := SHORT                              // declarations too are brief
 *    META_LEN    := SHORT
 *    
 *    PATH_INFO   := ROW_CNT META_LEN [RN ^ROW_CNT] [BYTE ^META_LEN]
 *                   
 *    
 *    PATH_PACK   := PATH_CNT [PATH_INFO ^PATH_CNT]     // but note ea PATH_INFO is var-width
 * </pre>
 * </p>
 * 
 * <h2>TrailPack</h2>
 * <p>
 * <pre>
 *    TRAIL_CNT   := SHORT      // number of crumtrails
 *    
 *    TRAIL_SIZE  := INT        // the size of the row's crumtrail in bytes
 *    
 *    TRAIL_ITEM  := RN TRAIL_SIZE
 *    
 *    TRAIL_LIST  := TRAIL_CNT [TRAIL_ITEM ^TRAIL_CNT]
 *    
 *    
 *    CRUMTRAIL   := BYTE ^TRAIL_SIZE      // byte-size of crumtrail
 *    
 *    TRAILS      := CRUMTRAIL ^TRAIL_CNT
 *    
 *    TRAIL_PACK  := TRAIL_LIST  TRAILS
 *    
 *    
 * </pre>
 * </p>
 * 
 * <h2>SourcePack</h2>
 * <p>
 * <pre>
 *    SRC_CNT   := SHORT
 *    
 *    // per row
 *    COL_CNT   := SHORT
 *    // pre column
 *    COL_TYPE  := BYTE         // 
 *    COL_VALUE := <em>function_of</em>( COL_TYPE ) // TODO: document this tedius bit
 *    
 *    COLUMN    := COL_TYPE COL_VALUE
 *    ROW       := RN [COLUMN ^COL_CNT]
 *    
 *    SRC_PACK  := ROW ^SRC_CNT
 * </pre>
 * </p>
 * 
 * <h2>MorselPack</h2>
 * <p>
 * <pre>
 *    PACK_COUNT  := BYTE (current version is 4)
 *    PACK_SIZES  := INT ^PACK_COUNT
 *    MORSEL_PACK := PACK_COUNT PACK_SIZES ROW_PACK TRAIL_PACK SRC_PACK PATH_PACK
 * </pre>
 * </p>
 */
package io.crums.sldg.packs;

import io.crums.sldg.SkipLedger;














