# Releases
### 0.19.5
* adds `rejectBy()` method to data frame (a companion to `selectBy()`)
* adds a friendly `toString()` implementation to data frame
* fix: data frame join no longer changes the source dataframes sort order 
* minor fixes and enhancements including join performance and expression type inference
### 0.19.4
* ensures data frame based expression evaluation is thread safe
* adds `schema()` method to data frame to report its schema (its column descriptions) as a data frame
* ensures consistency of results for column value comparisons for null values
### 0.19.3
* indexing is possible on the dataframe directly, without needing a utility class. Data frame now supports methods `createIndex`, `index`, `dropIndex`
* introduces `forEach` iterate pattern for data frame and for data frame index
### 0.19.2
* adds `distinct()` method to data frame
* various enhancements and bug fixes
### 0.19.1
* adds `copy()` method to data frame (as discussed in issue #11)
* adds `newColumn()` methods - same as `addColumn()`, but it returns the newly created column rather than the data frame itself
* introduces template based error message formatting and converts all inlined error messages to message lookup by key. This change supports overriding error messages with messages provided by the application. Supports loading message templates from resource bundles, properties, and maps.
* renames `ErrorReporter` to `ExceptionFactory` and refactors the API
* supports type inference of computed column expressions
### 0.18.5
* adds support for ascending and descending column sort order and expression sort order
### 0.18.4
* adds support for the decimal type (high/arbitrary precision)
* improves error handling and reporting
* bug fixes
### 0.18.3
* changes data frame sort by column and sort by expression implementations to be stable
* adds `format()` built-in function which supports rich formatting of numeric and date values
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
* renames `Const()` aggregation function to `Same()` to avoid keyword collision 
### 0.17.1
* adds Const() aggregation function
* adds `isEmpty()` and `isNotEmpty()` methods for DataFrame
* adds a dedicated method to error reporter to throw unsupported operation exceptions
* removes avro implementation of hierarchical data set
* adds `trim()` built-in function
* fixes type inference for the is null/is not null operators
### 0.17.0
* adds handling of null arguments to some built-in functions
* supports custom exception types to be thrown on errors instead of the default RuntimeException 
* prevents object values from containing nulls (use the VOID value instead)
