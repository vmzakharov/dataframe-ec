# CSV File Support

## Introduction

The `dataframe-ec` framework supports reading data frames from and writing data frames to CSV file. A CSV file here means a plain text file, with the data organized by rows, and the column values in each row separated by commas or another separator. A CSV file can have optional headers.

Here are a few of examples of CSV files:

1. File `donut_orders.csv`
```text
Client,Donut,Quantity,Delivered
"Alice","Jelly",12,true
"Alice","Apple Cider",6,false
"Carol","Jelly",2,true
"Dave","Old Fashioned",10,false
```
This file has a header row and uses the comma as the separator character.

2. File `donut_orders_no_header.csv`
```text
"Alice"|"Jelly"|12|true
"Alice"|"Apple Cider"|6|false
"Carol"|"Jelly"|2|true
"Dave"|"Old Fashioned"|10|false
```
This file does not have a header row and uses the pipe character as the separator.

3. File `employees1.csv`
```text
Name,EmployeeId,HireDate,Salary
'Alice',1234,2020-01-01,110000.00
'Bob',1233,2010-01-01,
'Carl',,2005-11-21,130000.00
'Diane',10001,2012-09-20,130000.00
'Ed',10002,,0.00
```
This file represents nulls as blank values and uses the single quote character as the string value boundary.  

4. File `employees2.csv`
```text
Name|EmployeeId|HireDate|Salary
'Alice'|1234|2020-01-01|110000.00
'Bob'|1233|2010-01-01|-NULL-
'Carl'||2005-11-21|130000.00
'Diane'|10001|2012-09-20|130000.00
'Ed'|10002|-NULL-|0.00
```
This file uses a null marker string (`-NULL-`) to represent null values, the single quote character for string value brackets, and the pipe character as the column separator. In this example the empty value in the second column of the third row will be read as zero (default blank value for type integer) rather than null, because the value there (blank) does not equal the null marker `-NULL-`.   

## Data Types and Formats
| Java Type  | Column Type | Format                                                                                                                                                                                                                                                                               | Format Examples          | Value Examples                                  |
|------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|-------------------------------------------------|
| String     | STRING      | N/A                                                                                                                                                                                                                                                                                  | N/A                      | Hello<br>"A quoted string"                      |
| Date       | DATE        | See [javadoc for DateTimeFormatter](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html)                                                                                                                                            |                          | 2020-01-01<br/>11/21/2005                       |
| DateTime   | DATE_TIME   | See [javadoc for DateTimeFormatter](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/format/DateTimeFormatter.html)                                                                                                                                            |                          | 2023-12-24T13:14:15<br/>2023\*12\*24-13\*15\*15 |
| double     | DOUBLE      | See [javadoc for DecimalFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DecimalFormat.html)<br/>**or**<br/>A single character specifying decimal separator. In this case all other characters except the separator, numbers, and `-` will be ignored. | "."<br/>"$#,##0.0#"<br/> | \$-1,24,021.2345<br/>"$1,123.45"<br/>(2,345.67) |
| long       | LONG        | See [javadoc for DecimalFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DecimalFormat.html)                                                                                                                                                           |                          | 123456789<br/>$1,123                            |
| int        | INT         | See [javadoc for DecimalFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DecimalFormat.html)                                                                                                                                                           | same as `LONG`           |
| float      | FLOAT       | See [javadoc for DecimalFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DecimalFormat.html)                                                                                                                                                           | same as `DOUBLE`         |
| BigDecimal | DECIMAL     | See [javadoc for DecimalFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/DecimalFormat.html)                                                                                                                                                           | same as `DOUBLE`         |
| boolean    | BOOLEAN     | Comma separated string values for the boolean values of `true` and `false`.`"true,false"` is the default                                                                                                                                                                             | "T,F"<br>"Yes,No"        | T<br>No                                         |

## Compressed Files

Reading of zipped and gzipped files is supported. Based on the files extension (`.zip` or `.gz`) the file will be unpacked using the appropriate input stream (Zip or GZIP) and then processed as a regular text file.

Writing of data frames to a compressed file is not currently supported.

## DataSet and Schema Classes

The main class for working with CSV files is `CsvDataSet`. It has methods for loading a data frame from a CSV file, for writing a data frame to a CSV file, as well as methods for describing file and column formats.    

The column types and their formats are specified via instances of the `CsvSchema` class. The schema can be specified explicitly or a data set instance can attempt to infer the schema from the data in the file.

### CsvDataSet Constructors

| Constructor                                                               | Description                                                                                                     |
|---------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `CsvDataSet(String newDataFileName, String newName)`                      | Create a new CSV data set named `newName` pointing to the file with the name `newDataFileName`                   |
| `CsvDataSet(Path newDataFilePath, String newName)`                        | Create a new CSV data set named `newName` pointing to the file stored at the location `newDataFilePath`         |
| `CsvDataSet(String newDataFileName, String newName, CsvSchema newSchema)` | Create a new CSV data set pointing to the file with the name `newDataFileName` described by the provided schema |
| `CsvDataSet(Path newDataFilePath, String newName, CsvSchema newSchema)`   | Create a new CSV data set pointing to a file at the location `newDataFilePath` described by the provided schema |

### CsvDataSet Methods: Handling Empty Elements 

These are the methods defining how to interpret empty elements, i.e. when there are two adjacent column delimiters next to each other or a column delimiter at the very beginning or at the very end of a line.

| Type         | Method                           | Description                                                                                                                                                                                  |
|--------------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CsvDataSet` | `convertEmptyElementsToNulls()`  | Empty values in the source file (two adjacent separators) will be loaded as null values when this data set is loaded into a data frame.                                                      |
| `CsvDataSet` | `convertEmptyElementsToValues()` | Empty values in the source file (two adjacent separators) will be converted to the respective zero or empty values depending on the column type.<br/>**Note:** this is the default behavior. |

### CsvDataSet Methods for Reading And Writing Data Frames

| Type        | Method                           | Description                                                        |
|-------------|----------------------------------|--------------------------------------------------------------------|
| `DataFrame` | `loadAsDataFrame()`              | Loads the entire data set as a data frame                          |
| `DataFrame` | `loadAsDataFrame(int lineCount)` | Loads the first `lineCount` lines of the data set as a data frame  |
| `void`      | `write(DataFrame dataFrame)`     | Writes `dataFrame` to the file specified by this data set instance |

### CsvSchema Methods Describing File Format
These are the methods describing the file-wide properties. The methods that configure the properties return the instance of the `CsvSchema` they are invoked on so this lends itself nicely to using the builder pattern in the code.

| Type        | Method                                    | Description                                                                                                                                   | Default Value |
|-------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `CsvSchema` | `nullMarker(String newNullMarker)`        | if specified:<br/>when reading, this column value is read as `null`<br/>when writing, the data frame value of `null` is written as this value | empty         |
| `CsvSchema` | `separator(char newSeparator)`            | Column value separator                                                                                                                        | `,`           | 
| `CsvSchema` | `quoteCharacter(char newQuoteCharacter)`  | Quote character to denote string value boundary                                                                                               | `"`           |
| `CsvSchema` | `hasHeaderLine(boolean newHasHeaderLine)` | If `true` the CSV file has a header row, otherwise it contains only data rows                                                                 | true          |
| `String`    | `getNullMarker()`                         |                                                                                                                                               | 
| `boolean`   | `hasNullMarker()`                         |                                                                                                                                               |
| `char`      | `getSeparator()`                          |                                                                                                                                               |
| `char`      | `getQuoteCharacter()`                     |                                                                                                                                               |
| `boolean`   | `hasHeaderLine()`                         |                                                                                                                                               |

### CsvSchema Methods Describing Column Format

These methods define the columns in the file and (optionally) their format. The column type parameter value is one of the supported data frame Expression DSL types listed above. This information will be used for both reading and writing CSV files.

| Type        | Method                                                  | Description |
|-------------|---------------------------------------------------------|-------------|
| `CsvSchema` | `addColumn(String name, ValueType type)`                |             |
| `CsvSchema` | `addColumn(String name, ValueType type, String format)` |             |

## Examples

### Reading a CSV File into a Data Frame 

#### Reading file `donut_orders.csv`
```text
Client,Donut,Quantity,Delivered
"Alice","Jelly",12,true
"Alice","Apple Cider",6,false
"Carol","Jelly",2,true
"Dave","Old Fashioned",10,false
```

Java code to define the schema and the data set and load the file into a data frame:

```java
CsvSchema schema = new CsvSchema()
    .addColumn("Client", STRING)
    .addColumn("Donut", STRING)
    .addColumn("Quantity", INT)
    .addColumn("Delivered", BOOLEAN);

CsvDataSet dataSet = new CsvDataSet("donut_orders.csv", "Donut Orders", schema);

DataFrame loaded = dataSet.loadAsDataFrame();
```

### Reading the same file, except with custom formatting for the boolean column "Delivered"

```text
Client,Donut,Quantity,Delivered
"Alice","Jelly",12,.T.
"Alice","Apple Cider",6,.F.
"Carol","Jelly",2,.T.
"Dave","Old Fashioned",10,.F.
```

We need to define formatting for the boolean values in the schema:

```java
CsvSchema schema = new CsvSchema()
    .addColumn("Client", STRING)
    .addColumn("Donut", STRING)
    .addColumn("Quantity", INT)
    .addColumn("Delivered", BOOLEAN,  ".T.,.F.");

CsvDataSet dataSet = new CsvDataSet("donut_orders.csv", "Donut Orders", schema);

DataFrame loaded = dataSet.loadAsDataFrame();
```

#### Reading file `employees2.csv`
```text
Name|EmployeeId|HireDate|Salary
'Alice'|1234|2020-01-01|110000.00
'Bob'|1233|2010-01-01|-NULL-
'Carl'||2005-11-21|130000.00
'Diane'|10001|2012-09-20|130000.00
'Ed'|10002|-NULL-|0.00
```
To read this file we need to override the default values for the sting quote character and column value separator.

```java
CsvSchema schema = new CsvSchema()
        .addColumn("Name", STRING)
        .addColumn("EmployeeId", LONG)
        .addColumn("HireDate", DATE)
        .addColumn("Salary", DOUBLE)
        .separator('|')
        .quoteCharacter('\'')
        .nullMarker("-NULL-");

CsvDataSet dataSet = new CsvDataSet("employees2.csv", "Employees", schema);

DataFrame loaded = dataSet.loadAsDataFrame();
```

### Writing a Data Frame into a CSV File

#### Writing data frame as a CSV file with the default format

```java
DataFrame dataFrame = new DataFrame("source")
        .addStringColumn("Name").addIntColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
        .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
        .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
        .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
        .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null)
        .addRow("Ed", 10002, null, null, 0.0);

CsvDataSet dataSet = new StringBasedCsvDataSet("employees.csv", "Employees", "");
dataSet.write(dataFrame);
```

Result: 

```text
Name,EmployeeId,HireDate,Dept,Salary
"Alice",1234,2020-1-1,"Accounting",110000.0
"Bob",1233,2010-1-1,"Bee-bee-boo-boo",100000.0
"Carl",10000,2005-11-21,"Controllers",130000.0
"Diane",10001,2012-9-20,"",
"Ed",10002,,,0.0
```

#### Using a schema instance to override the default formatting

```java
CsvSchema schema = new CsvSchema()
    .addColumn("Name",       ValueType.STRING)
    .addColumn("EmployeeId", ValueType.LONG)
    .addColumn("HireDate",   ValueType.DATE, "uuuu-MM-dd")
    .addColumn("Dept",       ValueType.STRING)
    .addColumn("Salary",     ValueType.DOUBLE)
    .quoteCharacter('\'')
    .separator('|')
    .nullMarker("-null-");

DataFrame dataFrame = new DataFrame("Employees")
        .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
        .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
        .addRow("Bob", 1233, null, "Bee-bee-boo-boo", 100000.0)
        .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", null)
        .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
        .addRow("Ed", 10002, null, null, 0.0)
        ;

BasedCsvDataSet dataSet = new StringBasedCsvDataSet("employees1.csv", "Employees", schema);
dataSet.write(dataFrame);
```

Result:

```text
Name|EmployeeId|HireDate|Dept|Salary
'Alice'|1234|2020-01-01|'Accounting'|110000.0
'Bob'|1233|-null-|'Bee-bee-boo-boo'|100000.0
'Carl'|10000|2005-11-21|'Controllers'|-null-
'Diane'|10001|2012-09-20|''|130000.0
'Ed'|10002|-null-|-null-|0.0
```


## Unit Testing

This project uses the `StringBasedCsvDataSet` class for unit testing requiring reading and writing of CSV files. This class is a very simple extension of `CsvDataSet`. If you need this functionality in your project you can simply copy the implementation of `StringBasedCsvDataSet`.  

`StringBasedCsvDataSet` can be instantiated with a string which will mock the contents of the file pointed to by that dataset, like this:
```java
CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", """
    Name,EmployeeId,HireDate,Dept,Salary
    "Alice",1234,2020-01-01,"Accounting",110000.00
    "Carl",10000,2005-11-21,"Controllers",130000.00
    "Diane",10001,2012-09-20,"",130000.00
    "Ed",10002,,,0.00"""
);

DataFrame loaded = dataSet.loadAsDataFrame();
```

If a data frame is written to an instance of `StringBasedCsvDataSet` the resulting text is available via the `getWrittenData` method. Writing data to this data set (using the `write` method) will "overwrite" the previously written or stored data.  

```java
DataFrame dataFrame = new DataFrame("source")
        .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
        .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
        .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
        .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", null)
        .addRow("Ed", 10002, null, null, 0.0)
        ;

StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");
dataSet.write(dataFrame);

String result = dataSet.getWrittenData();
```
