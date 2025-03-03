Morsel
======

A byte format for packaging ledger entries and their proofs in a unified way.

### Background

The first version packaged entries from a single ledger. Ledger hashes were
annotated with witness receipts from crums.io's centralized timechain microservice.
With the re-write of the timechain using skipleder itself (and open sourcing it),
encoding a single timestamped ledger now requires packaging *two* ledgers (the
ledger itself, and a timechain).

### Design Goals

This is designed to package proofs of related entries from *multiple* ledgers.
The simplest use case for *related* ledger entries involves witness records
of ledger hash state committed to timechains (which are themselves ledgers).

Another goal is to allow the *source data* to live outside the file. Depending on
ledger "type", not packaging the source may be more appropriate. For example,
if the "ledger" is a recorded stream, packaging it inside a morsel may be impractical.

### Format

Here's a preliminary sketch. The model will be made flexible: new sections can be added,
existing sections partitioned (e.g. the asset sections below), but for now, the focus
is on enumerating the *kind* of parts we need.



    HEADER        // fixed size (magic + version)
    LEDGER_NAMES  // not-empty list of ledger names/aliases (unique per morsel)
    
    MORSEL_NOTES	// Optional. UTF-8, maybe JSON. Important thing is it's text.
    MORSEL_ASSETS	// Optional. If present, then MORSEL_NOTES is present (a mime type might suffice).
    
    LEDGERS
     
    // per ledger   
    TYPE            // TABLE, TIMECHAIN, LOG, BSTREAM
    ALIAS           // 
    LEDGER_URI      // Optional. This may be a URL (e.g. a timechain).
    LEDGER_NOTES    // Optional. UTF-8, maybe JSON. Important thing is it's text.
    LEDGER_ASSETS   // Optional. If present, then LEDGER_NOTES is present.
    
    // for TIMECHAIN ledgers
    POLICY          // Required. Goes in LEDGER_NOTES
    
    // for TABLE, LOG, BSTREAM
    SALTED          // Flag indicates source ledger is salted
      NOT-SALTED    // cell (column) indices not salted
    
    // for LOG (both log files and journals)
    ROW_DELIMITER   // defaults to new-line character '\n'
    COMMENT_PREFIX  // defaults to none. (For journals, only)
    SKIP_EMPTY_ROW  // defaults to true
    TOKENIZER       // defaults to whitespace tokenizer; allows no-tokenizer
    


### Dependencies (Software)

Since evidence from many types of ledger is to be packaged in morsels, this module
will necessarily integrate and depend on a good many other modules. Other submodule
components, therefore, should not know about morsels.

But this presents a challenge. For example, *logledge*, which commits logs to morsels
(does it?) would not be able to use this library without a lot of SQL dependencies
(which it doesn't need). The *hope* is a common subset of this module can be factored
out.









