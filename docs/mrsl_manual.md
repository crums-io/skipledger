mrsl Manual
========

This is a short manual for the *mrsl* tool.  
Version 0.0.4

## Contents

- [Overview](#overview)
    - [Use Model](#use-model)
- [Commands](#commands)
    - [sum](#sum)
    - [info](#info)
    - [state](#state)
    - [list](#list)
    - [history](#history)
    - [entry](#entry)
    - [merge](#merge)
    - [submerge](#submerge)
    - [dump](#dump)

## Overview

Morsel files are tamper proof binary containers of row data from a historical append-only ledger. Since they're in binary format, we need this program to validate, read and manipulate them.

Here's what you may find in a morsel file (`.mrsl`):

1. *opaque row hashes only*. These are called *state-morsel*s. Such a morsel only reveals how many rows are in the ledger it represents as
well as how the hash of the last row is related to that of the first in the ledger. This information is compact no matter how many rows the ledger has. Think of state morsels as rich fingerprints: as a ledger evolves its new fingerprint can be
validated against its older ones.

2. *source rows*. A morsel may include any reasonably small subset of source rows from a ledger. (Morsels are
designed to be, well.. morsels: they're supposed to fit in memory.) Any column value in these source rows may
have been redacted by its substitution with a hash. (The hashing procedure resists both
rainbow attacks and frequency analysis.)

3. *witness records*. A morsel may also contain one or more tamper proof records (called *crumtrail*s) indicating the time
the hash of a particular row in the ledger (identified by its row number) was witnessed by the `crums.io` service. Since
the hash of every row in the ledger [also] depends on the hash of every row before it, a crumtrail for a given row number
establishes the minimum age of that row and *every row before that row number*.

### Use Model

Morsels originate from ledger owners, but morph once in the wild, as they're lumped together, or sliced and diced with
redactions (coming features), etc. Exactly how these tamper proof files are to be disseminated, is left open. Let's illustrate the "trust model" using a hypothetical example:

Say, you have a morsel. It's a tamper proof window into a historical ledger. Yes, you can verify
that it hasn't been tampered with (by the fact mrsl loads it without a fuss).
But which ledger is it from? Is this morsel from a made-up ledger? Or is it from the "Chinook Music Corp Recievables" ledger
that you were promised?

As it turns out, "Chinook Music Corp" periodically publishes the latest state of this "Receivables" ledger as a state morsel
on their website (the rich-fingerprint thing). This is good: you can verify your morsel indeed came from the same ledger posted
on the Chinook Music Corp website. (See [merge](#merge).)




## Commands

The following is meant to complement the program's `-help` option. None of the commands require a network connection. With the exception of the `dump` command which outputs in JSON, command output defaults to plain text. The reason why is that from a presentation standpoint, tabular data is more intuitive in row-per-line format. Output is switched to JSON using the `--json` option (or `-j`). By default, JSON output is indented; to remove whitespace use the `--pack` option.

Note the command line interface is designed so that argument / options order do not matter. Options (on/off *switches*, really) can be abbreviated using their first letter and where appropriate combined in the usual way (e.g. `-jp` combines `--json` and `--pack`).

### sum

This prints a summary of the morsel's contents.

Here's a summary of a *state* morsel:

>
    $ mrsl chinook/chinook-state-1230.mrsl sum
    
    <chinook-state-1230.mrsl>
    
    Rows:
     count:          16                                    
     # range:        lo: 1                                 hi: 1230   
     with sources:   0                                     
    
    <1230-7fc6df1188a478f650bbc51475351c0d3d0a63afa3f56d0c368709f7a5ffeea0>

There's no ceremony: it says there are zero rows with source attachments. The highest row number in the morsel
is 1230: so this morsel captures the state of the ledger when it had 1230 rows. The hash of row [1230], or equivalently the
hash of the ledger when it had exactly 1230 rows, is displayed on the last line.

And here's a summary of a morsel (from the same ledger) with more stuff in it:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl sum
    
    <chinook-260-259-258-257-.mrsl>
     -- Chinook Invoices -- 
    
    
    Rows:
     count:          36                                    
     # range:        lo: 1                                 hi: 1230                  
     with sources:   8                                     
    
    History:
     witnessed:      Sat Oct 23 22:32:56 MDT 2021          (row 300)                
    
    <1230-7fc6df1188a478f650bbc51475351c0d3d0a63afa3f56d0c368709f7a5ffeea0>

The line immediately after the morsel's filename, indicates the ledger's name. This comes from a meta
file embedded in the morsel. More about that [below](#info).
Inspecting the last line, this morsel captures the state of the ledger when it had 1230 rows. It contains 8 rows with
sources which were all created before Sat Oct 23 22:33 (because their hashes were witnessed then).

### info

This outputs *meta* information about the ledger, if present. This is not validated information (it does not involve hash proofs). It's there to help a user make sense of the ledger data in the morsel.

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl info
    
    Name: 
         Chinook Invoices 
    
    Description: 
        Chinook invoice items ledger example. Each invoice contains a list of
        line-items: each of those line-items is a row in this ledger. Address
        fields are the customer's billing address when the invoice was filled.
        See also: https://crums-io.github.io/skipledger/mrsl_manual.html
    
    Named Columns:
    
        [1]  Invoice Line ID 
             Description: 
                  Line-item ID. (PRIMARY KEY)
    
        [2]  Invoice ID 
             Description: 
                  The invoice ID this line-item occurs in (one-to-many
                  relationship).
    
        [3]  Track ID 
             Description: 
                  Album Track ID
    
        [4]  Unit Price 
             Description: 
                  1 bit = 100 sats
             Units: bits 
    
        [5]  Quantity 
    
        [6]  Customer ID 
    
        [7]  Invoice Date 
    
        [8]  Address 
    
        [9]  City 
    
        [10] State 
    
        [11] Country 
    
        [12] Postal Code 
    
        [13] Total 
             Description: 
                  Total in invoice. (Includes this line-item and might
                  include other line-items.)
             Units: bits 
      
      Date Format:
          Pattern: EEE, d MMM yyyy HH:mm:ss Z z 
          Example: Sun, 12 Dec 2021 14:58:04 -0700 MST 

Note this information is optional, both in whole and in parts. For example, not all columns need be defined--or any, for that matter. The simplest embedded meta file only contains the ledger name. The structure is more evident in JSON:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl info -j
    {
      "name": "Chinook Invoices",
      "desc": "Chinook invoice items ledger example. Each invoice contains a list of line-items: each of those line-items is a row in this ledger. Address fields are the customer's billing address when the invoice was filled. See also: https://crums-io.github.io/skipledger/mrsl_manual.html",
      "columns": [
        {
          "cn": 1,
          "name": "Invoice Line ID",
          "desc": "Line-item ID. (PRIMARY KEY)"
        },
        {
          "cn": 2,
          "name": "Invoice ID",
          "desc": "The invoice ID this line-item occurs in (one-to-many relationship)."
        },
        {
          "cn": 3,
          "name": "Track ID",
          "desc": "Album Track ID"
        },
        {
          "cn": 4,
          "name": "Unit Price",
          "desc": "1 bit = 100 sats",
          "units": "bits"
        },
        {
          "cn": 5,
          "name": "Quantity"
        },
        {
          "cn": 6,
          "name": "Customer ID"
        },
        {
          "cn": 7,
          "name": "Invoice Date"
        },
        {
          "cn": 8,
          "name": "Address"
        },
        {
          "cn": 9,
          "name": "City"
        },
        {
          "cn": 10,
          "name": "State"
        },
        {
          "cn": 11,
          "name": "Country"
        },
        {
          "cn": 12,
          "name": "Postal Code"
        },
        {
          "cn": 13,
          "name": "Total",
          "desc": "Total in invoice. (Includes this line-item and might include other line-items.)",
          "units": "bits"
        }
      ],
      "date_format": "EEE, d MMM yyyy HH:mm:ss Z z"
    }
    

### state

This just prints the last line output by the *sum* command without the brackets.

### list

Lists all the rows in the morsel by row number. Witness dates and row source data, if present, are displayed.

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl list

>
    [1]                                        
    [2]                                        
    [4]                                        
    [8]                                        
    [16]                                       
    [32]                                       
    [64]                                       
    [128]                                      
    [192]                                      
    [224]                                      
    [240]                                      
    [248]                                      
    [252]                                      
    [253]  S   253 47 1518 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [254]  S   254 47 1527 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [255]  S   255 47 1536 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [256]  S W Sat Oct 23 22:32:56 MDT 2021    256 47 ..                            
    [257]  S   257 47 1554 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [258]  S   258 47 1563 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [259]  S   259 47 1572 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [260]  S   260 47 1581 0.99 1 15 Thu Jul 16 00:00:00 MDT 2009 [X] Vancouver BC.. 
    [264]                                      
    [272]                                      
    [288]                                      
    [296]                                      
    [300]   W  Sat Oct 23 22:32:56 MDT 2021    
    [304]                                      
    [320]                                      
    [384]                                      
    [512]                                      
    [1024]                                     
    [1152]                                     
    [1216]                                     
    [1224]                                     
    [1228]                                     
    [1230]                                     
    
    36 rows, 2 crumtrails, 8 source-rows.
    

Rows with source-attachements are marked with `S`; if their hash has been witnessed, then they're marked with a `W` and
the witness date immediately follows.

What's in the empty rows? Nothing but hash data linking each successive row to the previous one.

### history

This lists the *crumtrail*s (witness records) in the morsel evidencing when the hash of rows were witnessed.
Because the hash of each row depends on the hash of every row before it, witnessing the hash of a row at a given [row] number
also implies witnessing every row before that row number.

Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl history
    
    
    2 rows witnessed in <chinook-260-259-258-257-.mrsl>:
    
    [row #] [date; ref hash; ref URL]                                               
    ------- -------------------------                                               
    
    256     Sat Oct 23 22:32:56 MDT 2021                                            
            8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f        
            https://crums.io/api/list_roots?utc=1635050186576&count=-4              
    300     Sat Oct 23 22:32:56 MDT 2021                                            
            8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f        
            https://crums.io/api/list_roots?utc=1635050186576&count=-4              
    
The left column indicates the row number witnessed; the right column is a synopsis of the crumtrail:

1. The date witnessed
2. The root hash of the crumtrail
3. A reference URL for the crumtrail's hash

(The crumtrail for row [256] in the example above is redundant, by the way. It's there because rows at such
numbers--multiples of higher powers of 2, are special and useful to keep around).


### entry

This provides a more detailed listings of source rows (if any). By default, output is in
text format. Example:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253-260

>
    [253]  253 47 1518 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [254]  254 47 1527 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [255]  255 47 1536 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [256]  256 47 1545 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [257]  257 47 1554 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [258]  258 47 1563 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [259]  259 47 1572 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    [260]  260 47 1581 0.99 1 15 Thu, 16 Jul 2009 00:00:00 -0600 MDT [X] Vancouver BC Canada V6C 1G8 13.86
    

By default, column values are separated by a single whitespace character. Redacted column values are marked with an `[X]`. To include history, let's add the `--time` option. We'll also set the column separator so we can better make out the columns:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253-260 -t sep=%s\|%s

>
    [253]  | 253 | 47 | 1518 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [254]  | 254 | 47 | 1527 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [255]  | 255 | 47 | 1536 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [256]  | 256 | 47 | 1545 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [256]  | << Witnessed Sat, 23 Oct 2021 22:32:56 -0600 MDT >>
    [257]  | 257 | 47 | 1554 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [258]  | 258 | 47 | 1563 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [259]  | 259 | 47 | 1572 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [260]  | 260 | 47 | 1581 | 0.99 | 1 | 15 | Thu, 16 Jul 2009 00:00:00 -0600 MDT | [X] | Vancouver | BC | Canada | V6C 1G8 | 13.86
    [300]  | << Witnessed Sat, 23 Oct 2021 22:32:56 -0600 MDT >>

Note, if a row is annotated with a crumtrail (witness record), then it may occur twice. Note the `sep=%s\|%s` argument. `%s` is interpreted as a space character. (The `\|` is to escape piping by the shell, which instead transforms it to `|`.) To turn this into *csv* format, you'd do something like `sep=,%s` --which is problematic if a column-value already has a comma in it. 

In JSON, the output is more structured and machine readable. Since it's a good deal more verbose, let's concentrate on a single entry:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253 -j
    [
      {
        "rn": 253,
        "cols": [
          {
            "type": "L",
            "salt": "3a91c5ff09de56e344b4e8bc0650023150a5e4adb910934123790131d14df4c7",
            "val": 253
          },
          {
            "type": "L",
            "salt": "71b8f5c2c0c7626820761a4264d6f5533097a778064f96c6005719ec53f1fb61",
            "val": 47
          },
          {
            "type": "L",
            "salt": "362211e279521420bee42cede0af73d2b84d90f20f7204ca4390b3e5f8288949",
            "val": 1518
          },
          {
            "type": "D",
            "salt": "18b1d755310d3b00b94b476360c7e7b813cd2f9eee48a27cb45307c3a4b54651",
            "val": 0.99
          },
          {
            "type": "L",
            "salt": "ba2d5cdfd73f28ad941d22f30267e786c77dcea6b31713b0124802eb6018187e",
            "val": 1
          },
          {
            "type": "L",
            "salt": "7ab684dc86563d934913fbfb50da3ab3d8c0b19349bf8cb875f3555d16f6791b",
            "val": 15
          },
          {
            "type": "T",
            "salt": "48c4882236bb1450b19220c679a6917d20ad90ab071e0fd0542ffa17db80b1d8",
            "date": "Thu, 16 Jul 2009 00:00:00 -0600 MDT",
            "val": 1247724000000
          },
          {
            "type": "H",
            "val": "2610cf15189202492107f3accb1f252e398581059ec7d275e140b96a9bc52f1c"
          },
          {
            "type": "S",
            "salt": "1aadd8f5f876cbf80ddf5629c320a66d227c3630cd4582213af1460725fef178",
            "val": "Vancouver"
          },
          {
            "type": "S",
            "salt": "abd15b450c0b2633509c9a2805e0114ab13f24845ba3a7378e92fb042bf07510",
            "val": "BC"
          },
          {
            "type": "S",
            "salt": "2323b1e818adf0031516023588d579d962133fa79a949fd0fb0ffe3c3d55ab7e",
            "val": "Canada"
          },
          {
            "type": "S",
            "salt": "93879763481dd775cbfbe18c580b5eaf56c07029657369b149c71e191ce0a0c0",
            "val": "V6C 1G8"
          },
          {
            "type": "D",
            "salt": "7fc48c039642d8e634fbb50c0c9078515db5729d1d939fc5fe561d5e283b7c22",
            "val": 13.86
          }
        ]
      }
    ]

This contains the necessary information to independently compute the *input*-hash for the skip ledger row [253]. In many use cases, you won't need to do this (`mrsl` has already verified the hashes), but before we move on, here's a brief roundup of how the hash of each column is computed. Note, `salt` is always a 32-byte (pseudo) random byte string.


| Symbol | Name   | Description | Bytes Hashed |
| ------ | ------ | ----------- | ------------ |
| L      | long   | any integral type (includes boolean) is mapped to long | salt + SHA-256(8-byte, big-endian) |
| D      | double | any floating point type is mapped to double            | salt + SHA-256(8-byte, big-endian) |
| T      | time   | date in UTC milliseconds. A long with extra semantics. | salt + SHA-256(8-byte, big-endian) |
| S      | string | UTF-8 string (max length 16,777,215 UTF-8 bytes)       | salt + SHA-256(4-byte, big-endian_len + UTF-8-bytes) |
| B      | bytes  | raw byte string in hex (max length 16,777,215)         | salt + SHA-256(4-byte, big-endian_len + raw-bytes)   |
| H      | hash   | 32-bytes, prehashed (a redacted column value)          | verbatim: it *is* the hash |
| NUL    | null   | a null value (SQL NULLs are mapped to this)            | salt |

The last column in the above table indicates what byte-string is hashed for each type of column value.

Now on to the simpler `--slim` version of JSON. As previously mentioned, most of the time you won't need the full detail:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253 -js
    [
      {
        "rn": 253,
        "cols": [
          253,
          47,
          1518,
          0.99,
          1,
          15,
          1247724000000,
          "[X]",
          "Vancouver",
          "BC",
          "Canada",
          "V6C 1G8",
          13.86
        ]
      }
    ]

In particular, if you know that the ledger's rows uniformly have the same type and number of columns, there's little point in interrogating for this information at every row. Note most of our defined types map sensibly to default JSON types.

Both JSON formats support the `--time` option. Here it is in *slim* form:

>
    $ mrsl chinook/chinook-260-259-258-257-.mrsl entry 253 -jst
    [
      {
        "rn": 253,
        "cols": [
          253,
          47,
          1518,
          0.99,
          1,
          15,
          1247724000000,
          "[X]",
          "Vancouver",
          "BC",
          "Canada",
          "V6C 1G8",
          13.86
        ]
      },
      {
        "rn": 256,
        "rh": "f60873e9a7fe463a3831bfd2fdd480f01f8b1571d65a40196622d9a4f9421db6",
        "wit": "Sat, 23 Oct 2021 22:32:56 -0600 MDT",
        "tref": "https://crums.io/api/list_roots?utc=1635050186576&count=-4",
        "troot": "8a1457684b393a346b461ac674eb4e7a617d5aaa6033d703af9f79173092f38f"
      }
    ]



### merge

The *merge* command takes 2 or more morsel files and merges them into one. But you can't merge just any 2 morsels. In order to be mergeable,
any 2 morsels must satisfy the following criteria:

1. The hash of every row (identified by their row number) in both morsels must agree.
2. The 2 morsels must contain enough information to connect the hash of highest numbered row with that every source row in both morsels.

Put another way, we can only ever merge morsels from the same ledger.

Example:

>
    $ mrsl chinook/*.mrsl merge save=chinook/chinook-merged/
    
    Loading 5 morsels for merge..
     chinook/chinook-260-259-258-257-.mrsl
     chinook/chinook-419-418-417-416-.mrsl
     chinook/chinook-613-458-457-456-.mrsl
     chinook/chinook-778-777-776-775-.mrsl
     chinook/chinook-state-1230.mrsl
    
     authority: chinook/chinook-613-458-457-456-.mrsl
    
    init 48 objects (hi row 1230)
     chinook/chinook-state-1230.mrsl ... 0 objects added
     chinook/chinook-260-259-258-257-.mrsl ... 29 objects added
     chinook/chinook-419-418-417-416-.mrsl ... 26 objects added
     chinook/chinook-778-777-776-775-.mrsl ... 20 objects added
    
    123 objects merged to chinook/chinook-merged/chinook-merged-778-777-776-775-.mrsl

When 2 or more morsels are merged together, the morsel containing [the hash of] the highest row number is the
*authority*. (If there's a tie, one is chosen at random.) Above, `chinook/chinook-613-458-457-456-.mrsl` was the authority since
it had (or tied at) the highest row number.

The newly written morsel `chinook-merged-778-777-776-775-.mrsl` now contains all the source rows in 4 original morsels (5, if you count the state-morsel that didn't contribute anything), and runs
to row [1230]. In this example, one of the morsels was a state morsel (with no source rows). The reported number of *objects added* is just informational: it includes an accounting of full skip ledger rows, source-rows, crumtrails, path- and meta info.


### submerge

The information you gather in morsels from a 3rd party ledger my contain private or sensitive information. In such (and perhaps other) use cases, you may not want to share all your morsel data with another party downstream. To share a *subset* of the data in any given morsel issue the *submerge* command.

Example:

>
    $ mrsl submerge chinook/chinook-merged/chinook-merged-778-777-776-775-.mrsl save=chinook/chinook-sub/ redact=8 773-775
    
    3 source rows, 1 crumtrail written to chinook/chinook-sub/chinook-sub-775-774-773.mrsl

Here only source rows [773] thru [775] were written to the new morsel. The value in column [8] of these rows (the street address) was redacted.


### dump

This command is intended for external programs. With the exception of meta data (see [info](#info)) all the morsel's data is emitted as JSON.









