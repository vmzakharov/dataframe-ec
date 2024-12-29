# Releases
### 1.2.0
* no functional changes, refactors csv/string conversion support classes
### 1.1.0
* adds support for columns of boolean type
### 1.0.0
* **switches to [Semantic Versioning](https://semver.org/)** 
* adds collect() iteration patterns to data frames and data frame indices
* adds support for projection of properties of int, float, and decimal types
* internal changes
  * further refactoring to take advantage of Java 17 features
  * switches unit testing to JUnit 5
  * miscellaneous performance optimizations
### 0.20.0
* **switches to Java 17 as minimum version**. The last release of dataframe-ec to support Java 8+ is 0.19.8
* type inference fixes/enhancements, better type consistency validation for computed columns  
### 0.19.8
* adds support for columns of primitive int and float types
* adds a built-in aggregation function `avg2d` that produces values of type double for all primitive column types
### 0.19.7
* adds support for sorting the order of value columns by their header values when pivoting a data frame 
* add an option to the data frame compare utility to ignore the column order
### 0.19.6
* adds `pivot()` method to support spreadsheet-like pivot functionality
* adds a data frame pretty print utility
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
