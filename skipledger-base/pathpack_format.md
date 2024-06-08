Path-pack Binary Format
=======================

Path-packs are designed to package hash-information about skip ledger paths
(list of rows whose hashes are linked via hash proofs) using no redundant information. My aim is to describe the binary layout for the structure precisely, but some parts are difficult to specify without wading thru a
general overview of the skip ledger structure and its properties. Most of
those properties are captured as static methods in the
`io.crums.sldg.SkipLedger` class, and are referenced a few times belows.

## Definitions

The basic units:


    INT         := BYTE ^4    // (big endian)
    LONG        := BYTE ^8    // (big endian)
    HASH        := BYTE ^32   // assumed to be a SHA-256 digest

Note, our convention. If 

    Z =         := BYTE       // (unsigned)

then

    FOO         := HASH ^Z

means *FOO* is composed of *Z* units of *HASH* (and is 32*Z* bytes in length).

## Structure

The components of the structure are first defined, and the final object `PATH_PACK` is defined as their composition:


    TYPE        := BYTE           // 0 means full; 1 means condensed
    RS_COUNT    := INT            // stitch row number count
    STITCH_RNS  := LONG ^RS_COUNT // stitch row numbers in strictly ascending order  

    I_COUNT     := INT            // number of rows with full info (have input hash)
                                  // inferred from SkipLedger#stitch(..)
                                  // The full row no.s themselves (I_COUNT -many of them)
                                  // are also known at this point.

    R_COUNT     := INT            // number of rows with only ref-hashes inferred from
                                  // depending on type, the size of:
                                  //
                                  //  TYPE [0] (full)
                                  // SkipLedger.refOnlyCoverage(..)
                                  //
                                  //  TYPE [1] (condensed)
                                  // SkipLedger.refOnlyCondensedCoverage(..)
                                  //
                                  // using the full row no.s 
    
    R_TBL       := HASH ^R_COUNT  // hash pointer table (ref-only hashes)
                                  // cells are laid out in ascending row no.
                                  // (row no.s are inferred from STITCH_RNS)
    
    I_TBL       := HASH ^I_COUNT  // input hash table
                                  // cells are laid out in ascending row no.
                                  // (row no.s are inferred from STITCH_RNS)
    
    FUNNELS     := BYTE ^fs       // funnel block. Present iff TYPE [1] (condensed)

                                  // The first funnel in the block belongs to the
                                  // first row no. (again, inferred from STITCH_RNS)
                                  // that is condensable (c.f. SkipLedge.isCondensable(..)),
                                  // and the byte-length of the funnel is determined
                                  // by {@linkplain SkipLedger#funnelLength(long, long)}.
                                  // The next funnel belongs to the next smallest,
                                  // full row no. that is condensable, and so on.
                                  // In this way, fs the byte-size of the funnel
                                  // block is determined from the STITCH_RNS.
    
    PATH_PACK    := TYPE RS_COUNT STITCH_RNS R_TBL I_TBL [FUNNELS]
 
 