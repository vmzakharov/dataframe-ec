# Releases
### 0.18.4
* adds support for the decimal type (high/arbitrary precision)
* improves error handling and reporting
* bug fixes
### 0.18.3
* changes data frame sort by column and sort by expression implementations to be stable
* adds format() built-in function which supports rich formatting of numeric and date values
###  0.18.1
* validates that the header names in a CSV data set match the schema
* makes error printer configurable on ErrorReporter
### 0.18.0
* supports data sets in csv files without a header line (requires a schema)
* supports aggregation functions that can handle null values
* changes CsvDataSet setting for different treatment of empty values on load to a toggle
### 0.17.3
* asCsvString on data frame supports dataTime
* adds support for dateTime values to CsvDataSet
* moves ObjectListDataSet to the main source tree from test
* introduces date-time column type
* adds date-time projection tests
* supports fluent API for lookup joins (experimental)
* renames Const() aggregation function to Same() to avoid keyword collision 
### 0.17.1
* adds Const() aggregation function
* adds isEmpty() and isNotEmpty() methods for DataFrame
* adds a dedicated method to error reporter to throw unsupported operation exceptions
* removes avro implementation of hierarchical data set
* adds trim() built-in function
* fixes type inference for the is null/is not null operators
### 0.17.0
* adds handling of null arguments to some built-in functions
* supports custom exception types to be thrown on errors instead of the default RuntimeException 
* prevents object values from containing nulls (use the VOID value instead)
