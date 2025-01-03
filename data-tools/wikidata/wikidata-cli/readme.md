# Overview
The Wikidata CLI is used to download Item Revision data from Wikidata so that it can be used to test Drasi with the Drasi end-to-end performance test framework.

# Item Types
The Wikidata CLI only supports the following Wikidata Item Types:

- Continent (Q5107)
- Country (Q6256)
- City (Q515)

# Usage
The Wikidata CLI supports 3 functions:

- For a list of Item Types, download Items and their Revisions
- For a list of Items, download their Revisions
- Convert downloaded Item Revisions to Test Scripts

## Download Item Revisions by Item Type


## Download Item Revisions by Item


## Generate Test Scripts


# Troubleshooting
The most common problem when running the Wikidata CLI is that it can fail to download a chunk of Item Revisions becuase the Wikidata query response is truncated because it would be larger than the maximum allowed size. The Wikidata CLI doesnt currently report this error specifically, but will fail reporting that there is content missing from a Revision.

The easiest way to work around this issue is to run the same or similar command again but with a smaller `batch_size`. This will result in a smaller response size, avoiding response truncation and allowing the Wikidata CLI to correctly process the query result content.

For example, if you run the following command:

```
wikidata get-types -t country -b 30 -s 2019-01-01T00:00:01 -e 2019-12-30T23:59:59
```

And get an error like the following downloading Revisions for Item 'Q408':

```
[2025-01-03T17:23:59Z ERROR wikidata::wikidata] Chunk download failed - Item ID: "Q408". Revision IDs: "1060059333|1056723095|1052138832|1047371725|1046909139|1045942699|1040577791|1037696626|1035297315|1034670131". Error: error decoding response body

    Caused by:
        invalid type: integer `1060059333`, expected a string at line 1 column 36.
```

You can run the following command using a smaller batch size such as follows:

```
wikidata get-items -t country -i Q408 -b 5 -s 2019-01-01T00:00:01 -e 2019-12-30T23:59:59
```

Because the Wikidata CLI only downloads Revisions it doesn't already have, you will only download the missing Revisions.

# Wikidata Item Schema
## Continent (wd:Q5107)
area
https://www.wikidata.org/wiki/Property:P2046

coordinate_location
https://www.wikidata.org/wiki/Property:P625

name
(label - en)

population
https://www.wikidata.org/wiki/Property:P1082

## Country (wd:Q6256)Coordinate Location
area
https://www.wikidata.org/wiki/Property:P2046

continent_id
https://www.wikidata.org/wiki/Property:P30

coordinate_location
https://www.wikidata.org/wiki/Property:P625

name
(label - en)

population
https://www.wikidata.org/wiki/Property:P1082

### TODO
Life Expectancy
https://www.wikidata.org/wiki/Property:P2250

Capital
https://www.wikidata.org/wiki/Property:P36

Head of State
https://www.wikidata.org/wiki/Property:P35

Nominal GDP
https://www.wikidata.org/wiki/Property:P2131

Per Capita Income
https://www.wikidata.org/wiki/Property:P10622

Founded / Inception
https://www.wikidata.org/wiki/Property:P571

## City (wd:Q515)
area
https://www.wikidata.org/wiki/Property:P2046

coordinate_location
https://www.wikidata.org/wiki/Property:P625

country_id
https://www.wikidata.org/wiki/Property:P17

name
(label - en)

population
https://www.wikidata.org/wiki/Property:P1082

### TODO
Founded / Inception
https://www.wikidata.org/wiki/Property:P571

Located in time zone
https://www.wikidata.org/wiki/Property:P421

Date of Incorporation
https://www.wikidata.org/wiki/Property:P10786

Head of Government
https://www.wikidata.org/wiki/Property:P6

Per Capita Income
https://www.wikidata.org/wiki/Property:P10622

# WikiData Reference URLs
Tools for Programmers:
https://www.wikidata.org/wiki/Wikidata:Tools/For_programmers

Wikibase CLI:
https://github.com/maxlath/wikibase-cli

Data Dump Info:
https://www.wikidata.org/wiki/Wikidata:Database_download

Cities and Towns:
https://www.wikidata.org/wiki/Wikidata:WikiProject_Cities_and_Towns