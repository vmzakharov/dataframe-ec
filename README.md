# dataframe-ec
A tabular data structure (aka a data frame) based on the Eclipse Collections framework. The underlying data frame structure is columnar, with focus on memory optimization achieved by using efficient Eclipse Collections data structures and APIs.

For more on Eclipse Collections see: https://www.eclipse.org/collections/.

## Where to Get It

Get the latest release of `dataframe-ec` here:

```xml
<dependency>
  <groupId>io.github.vmzakharov</groupId>
  <artifactId>dataframe-ec</artifactId>
  <version>1.3.0</version>
</dependency>
```

Note that starting with the release `0.20.0`, `dataframe-ec` is compatible with JDK 17+. The last release of `dataframe-ec` to support Java 8+ is `0.19.8` which can be found at these coordinates:

```xml
<dependency>
  <groupId>io.github.vmzakharov</groupId>
  <artifactId>dataframe-ec</artifactId>
  <version>0.19.8</version>
</dependency>
```

## Code Kata

Learn dataframe-ec with a Kata! Check out [dataframe-ec kata](https://github.com/vmzakharov/dataframe-ec-kata), an easy and fun way to learn the basics of data frame APIs and common usage patterns through hands-on exercises.

## Data Frame Operations

- **create** a data frame programmatically or load from a csv file
- **add a column** to a data frame, columns can be
    - stored or computed
    - of type: string, long, int, double, float, date, date/time
- **drop** one or more **columns**
- **select** a subset of rows based on a criteria
- **sort** by one or more columns or by an expression
- select **distinct** rows based on all column values or a subset of columns
- **union** - concatenating data frames with the same schemas
- **join** with another data frame, based on the specified column values, inner and outer joins are supported
- **join with complements**, a single operation that returns three data frames - a complement of this data frame in another one, an inner join of the data frames, and a complement of the other data frame in this one
- **lookup join** - enrich a data frame by adding one or more columns based on value lookup in another data frame
- **pivot** - similar to pivot in a spreadsheet where the distinct values in a column become columns in the pivot table and the row values in the table are grouped and aggregated based on the aggregation functions provided
- find relative complements (**set differences**) of two data frames based on the specified column values
- **aggregation** - aggregating the entire data frame or grouping by the specified column values and aggregating within a group
- **flag rows** - individually or matching a criteria
<br>...and more!

## Additional Reading

* [CSV File Support](docs/CSV_FILE_SUPPORT.md) - reading and writing CSV (and CSV-like) files
* [Handling Null Values](docs/NULL_SUPPORT.md) - dealing with real life data requires flexibility in handling nulls
* [Object Pooling](docs/POOLING.md) - the framework uses pooling to save memory
* [JSON Support](https://github.com/vmzakharov/dataframe-ec-json-support) - a companion project that support reading and writing JSON formatted text

## Code Examples

### Creating a Data Frame
A data frame can be **loaded from a CSV file**. For more details on reading and writing CSV (and CSV-like) files see the **[Csv File Support](docs/CSV_FILE_SUPPORT.md)** document.

Let's say there is a file called "donut_orders.csv" with the following contents:

```
Customer,  Count,  Price,  Date
"Archibald", 5.0, 23.45, 2020-10-15
"Bridget", 10.0, 40.34, 2020-11-10
"Clyde", 4.0, 19.5, 2020-10-19
```

Then a data frame can be loaded from this file as shown below. The file schema can be inferred, like in this example, or specified explicitly.
```Java
DataFrame ordersFromFile  = new CsvDataSet("donut_orders.csv", "Donut Orders").loadAsDataFrame();
```

`ordersFromFile`

|Customer |   Count |   Price |   Date|
|---|---:|---:|---|
|"Archibald" | 5.0000 | 23.4500 | 2020-10-15|
|"Bridget" | 10.0000 | 40.3400 | 2020-11-10|
|"Clyde" | 4.0000 | 19.5000 | 2020-10-19|

The `loadAsDataFrame()` method can take a numeric parameter, which specifies how many rows of data to load. For example:
```Java
DataFrame firstTwoOrders = new CsvDataSet("donut_orders.csv", "Donut Orders").loadAsDataFrame(2);
```
`firstTwoOrders`

|Customer |   Count |   Price |   Date|
|---|---:|---:|---|
|"Archibald" | 5.0000 | 23.4500 | 2020-10-15|
|"Bridget" | 10.0000 | 40.3400 | 2020-11-10|

`CsvDataSet` can also accept a **schema** object, which explicitly defines column types and also supports a number of options such as the value separator and the null marker in a source file. For example, let's consider a file "donut_orders_complicated.csv" that looks like

```
Customer|Count|Price|Date
"Archibald"|5|23.45|2020-10-15
"Bridget"|10|*null*|2020-11-10
"Clyde"|4|19.5|*null*
```

A data frame can be loaded from this file by providing a schema

```Java
CsvSchema donutSchema = new CsvSchema()
        .separator('|')
        .nullMarker("*null*");
donutSchema.addColumn("Customer", STRING);
donutSchema.addColumn("Count", LONG);
donutSchema.addColumn("Price", DOUBLE);
donutSchema.addColumn("Date", DATE);

CsvDataSet dataSet = new CsvDataSet("donut_orders_complicated.csv", "Donut Orders", donutSchema);
DataFrame schemingDonuts = dataSet.loadAsDataFrame();
```

`schemingDonuts`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Archibald" | 5 | 23.4500 | 2020-10-15|
|"Bridget" | 10 | null | 2020-11-10|
|"Clyde" | 4 | 19.5000 | null|

A data frame can be created **programmatically** by providing values for individual rows or columns. Here is a sample constructing a data frame row by row:
```Java
DataFrame orders = new DataFrame("Donut Orders")
    .addStringColumn("Customer").addLongColumn("Count").addDoubleColumn("Price").addDateColumn("Date")
    .addRow("Alice",  5, 23.45, LocalDate.of(2020, 10, 15))
    .addRow("Bob",   10, 40.34, LocalDate.of(2020, 11, 10))
    .addRow("Alice",  4, 19.50, LocalDate.of(2020, 10, 19))
    .addRow("Carl",  11, 44.78, LocalDate.of(2020, 12, 25))
    .addRow("Doris",  1,  5.00, LocalDate.of(2020,  9,  1));
```
`orders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Doris" | 1 | 5.0000 | 2020-09-01|

This way of creating a data frame is more useful for contexts like unit tests, where readability matters. In your applications you probably want to load a data frame from a file or populate individual columns with strongly typed values as in the following example, which produces a data frame with the same exact contents as the one in the example above:

```Java
DataFrame ordersByCol = new DataFrame("Donut Orders")
        .addStringColumn("Customer", Lists.immutable.of("Alice", "Bob", "Alice", "Carl", "Doris"))
        .addLongColumn("Count", LongLists.immutable.of(5, 10, 4, 11, 1))
        .addDoubleColumn("Price", DoubleLists.immutable.of(23.45, 40.34, 19.50, 44.78, 5.00))
        .addDateColumn("Date", Lists.immutable.of(LocalDate.of(2020, 10, 15), LocalDate.of(2020, 11, 10),
                LocalDate.of(2020, 10, 19), LocalDate.of(2020, 12, 25), LocalDate.of(2020, 9, 1)));
ordersByCol.seal(); // finished constructing a data frame
```

`ordersByCol`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Doris" | 1 | 5.0000 | 2020-09-01|

A data frame can also be created from a hierarchical data set via a **projection operator** supported by the DSL. Let's say we have two record types: `Person` and `Address`. Then we can use the projection operator to turn a list of `Person` objects into a data frame as follows.

```Java
var visitor = new InMemoryEvaluationVisitor();
visitor.getContext().addDataSet(
        new ObjectListDataSet("Person", Lists.immutable.of(
            new Person("Alice", 30, new Address("Brooklyn", "North Dakota")),
            new Person("Bob", 40, new Address("Lexington", "Nebraska")),
            new Person("Carl", 50, new Address("Northwood", "Idaho"))
        )));

var script = ExpressionParserHelper.DEFAULT.toScript("""
        project {
            Name : Person.name,
            ${Lucky Number} : Person.luckyNumber,
            State : Person.address.state
        }
        """);

DataFrame projectionValue = ((DataFrameValue) script.evaluate(visitor)).dataFrame();
```

`projectionValue`

|Name | Lucky Number | State|
|---|---:|---|
|"Alice" | 30 | "North Dakota"|
|"Bob" | 40 | "Nebraska"|
|"Carl" | 50 | "Idaho"|

### Aggregation Functions

#### Built-in Aggregation Functions

The following aggregation functions are supported out of the box.
- `sum`
- `min`
- `max`
- `avg` - average, the return value is of the same type as the input data
- `avg2d` - average, the return value is of type double for any primitive input type
- `count`
- `same` - the result is `null` if the aggregated values are not the equal to each other, otherwise it equals to that value

It is possible to add custom aggregation functions at runtime (see unit tests for examples).

For the following examples we will use the `orders` data frame we created earlier:

`orders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Doris" | 1 | 5.0000 | 2020-09-01|

#### Aggregation Example

```Java
DataFrame orderStats = orders.aggregate(Lists.immutable.of(max("Price", "MaxPrice"), min("Price", "MinPrice"), sum("Price", "Total")));
```

`orderStats`

|MaxPrice | MinPrice | Total|
|---:|---:|---:|
|44.7800 | 5.0000 | 133.0700|

#### Sum of Columns
```Java
DataFrame totalOrdered = orders.sum(Lists.immutable.of("Count", "Price"));
```

`totalOrdered`

|Count | Price|
|---:|---:|
|31 | 133.0700|

#### Sum With Group By
```Java
DataFrame totalsByCustomer = orders.sumBy(Lists.immutable.of("Count", "Price"), Lists.immutable.of("Customer"));
```

`totalsByCustomer`

|Customer | Count | Price|
|---|---:|---:|
|"Alice" | 9 | 42.9500|
|"Bob" | 10 | 40.3400|
|"Carl" | 11 | 44.7800|
|"Doris" | 1 | 5.0000|

### Add a Calculated Column
```Java
orders.addColumn("AvgDonutPrice", "Price / Count");
```
`orders`

|Customer | Count | Price | Date | AvgDonutPrice|
|---|---:|---:|---|---:|
|"Alice" | 5 | 23.4500 | 2020-10-15 | 4.6900|
|"Bob" | 10 | 40.3400 | 2020-11-10 | 4.0340|
|"Alice" | 4 | 19.5000 | 2020-10-19 | 4.8750|
|"Carl" | 11 | 44.7800 | 2020-12-25 | 4.0709|
|"Doris" | 1 | 5.0000 | 2020-09-01 | 5.0000|

### Filtering
Selection of a dataframe containing the rows matching the filter condition
```Java
DataFrame bigOrders = orders.selectBy("Count >= 10");
```
`bigOrders`

|Customer | Count | Price | Date | AvgDonutPrice|
|---|---:|---:|---|---:|
|"Bob" | 10 | 40.3400 | 2020-11-10 | 4.0340|
|"Carl" | 11 | 44.7800 | 2020-12-25 | 4.0709|

Selection of a dataframe *without* the rows matching the filter condition
```Java
DataFrame smallOrders = orders.rejectBy("Count >= 10");
```
`smallOrders`

|Customer | Count | Price | Date | AvgDonutPrice|
|---|---:|---:|---|---:|
|"Alice" | 5 | 23.4500 | 2020-10-15 | 4.6900|
|"Alice" | 4 | 19.5000 | 2020-10-19 | 4.8750|
|"Doris" | 1 | 5.0000 | 2020-09-01 | 5.0000|

Split the rows of a data frame into two subsets both matching and not matching the filter condition respectively
```Java
Twin<DataFrame> highAndLow = orders.partition("Count >= 10");
```
Result - a pair of data frames:

`highAndLow.getOne()`

|Customer | Count | Price | Date | AvgDonutPrice|
|---|---:|---:|---|---:|
|"Bob" | 10 | 40.3400 | 2020-11-10 | 4.0340|
|"Carl" | 11 | 44.7800 | 2020-12-25 | 4.0709|

`highAndLow.getTwo()`

|Customer | Count | Price | Date | AvgDonutPrice|
|---|---:|---:|---|---:|
|"Alice" | 5 | 23.4500 | 2020-10-15 | 4.6900|
|"Alice" | 4 | 19.5000 | 2020-10-19 | 4.8750|
|"Doris" | 1 | 5.0000 | 2020-09-01 | 5.0000|

### Drop Column
```Java
orders.dropColumn("AvgDonutPrice");
```
`orders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Doris" | 1 | 5.0000 | 2020-09-01|

### Sort
Sort by the order date:
```Java
orders.sortBy(Lists.immutable.of("Date"));
```
`orders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Doris" | 1 | 5.0000 | 2020-09-01|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Carl" | 11 | 44.7800 | 2020-12-25|

Sort by Customer ignoring the first letter of their name
```Java
orders.sortByExpression("substr(Customer, 1)");
```
`orders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Doris" | 1 | 5.0000 | 2020-09-01|

### Union

The union operation concatenates two data frames with the same schema. Note that it does not remove duplicate rows.
```Java
DataFrame otherOrders = new DataFrame("Other Donut Orders")
    .addStringColumn("Customer").addLongColumn("Count").addDoubleColumn("Price").addDateColumn("Date")
    .addRow("Eve",  2, 9.80, LocalDate.of(2020, 12, 5));

DataFrame combinedOrders = orders.union(otherOrders);
```
`combinedOrders`

|Customer | Count | Price | Date|
|---|---:|---:|---|
|"Alice" | 5 | 23.4500 | 2020-10-15|
|"Bob" | 10 | 40.3400 | 2020-11-10|
|"Alice" | 4 | 19.5000 | 2020-10-19|
|"Carl" | 11 | 44.7800 | 2020-12-25|
|"Doris" | 1 | 5.0000 | 2020-09-01|
|"Eve" | 2 | 9.8000 | 2020-12-05|

### Distinct
Say we want to get a list of all clients who placed orders, which are listed in the `orders` data frame above. We can
use the `distinct()` method for that:

```Java
DataFrame distinctCustomers = orders.distinct(Lists.immutable.of("Customer"));
```

`distinctCustomers`

|Customer|
|---|
|"Alice"|
|"Bob"|
|"Carl"|
|"Doris"|

### Joins

#### Inner and Outer Joins
```Java
DataFrame joining1 = new DataFrame("df1")
        .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz")
        .addRow("Pinky", "pink", "B", 8)
        .addRow("Inky", "cyan", "C", 9)
        .addRow("Clyde", "orange", "D", 10);

DataFrame joining2 = new DataFrame("df2")
        .addStringColumn("Name").addStringColumn("Color").addStringColumn("Code").addLongColumn("Number")
        .addRow("Grapefruit", "pink", "B", 2)
        .addRow("Orange", "orange", "D", 4)
        .addRow("Apple", "red", "A", 1);

DataFrame joined = joining1.outerJoin(joining2, Lists.immutable.of("Bar", "Letter"), Lists.immutable.of("Color", "Code"));
```

`joining1`

|Foo | Bar | Letter | Baz|
|---|---|---|---:|
|"Pinky" | "pink" | "B" | 8|
|"Inky" | "cyan" | "C" | 9|
|"Clyde" | "orange" | "D" | 10|

`joining2`

|Name | Color | Code | Number|
|---|---|---|---:|
|"Grapefruit" | "pink" | "B" | 2|
|"Orange" | "orange" | "D" | 4|
|"Apple" | "red" | "A" | 1|

`joined`

|Foo | Bar | Letter | Baz | Name | Number|
|---|---|---|---:|---|---:|
|"Inky" | "cyan" | "C" | 9 | null | null|
|"Clyde" | "orange" | "D" | 10 | "Orange" | 4|
|"Pinky" | "pink" | "B" | 8 | "Grapefruit" | 2|
|null | "red" | "A" | null | "Apple" | 1|

#### Join With Complements
```Java
DataFrame sideA = new DataFrame("Side A")
        .addStringColumn("Key").addLongColumn("Value")
        .addRow("A", 1)
        .addRow("B", 2)
        .addRow("X", 3)
        ;

DataFrame sideB = new DataFrame("Side B")
        .addStringColumn("Id").addLongColumn("Count")
        .addRow("X", 30)
        .addRow("B", 10)
        .addRow("C", 20)
        ;

Triplet<DataFrame> result = sideA.joinWithComplements(sideB, Lists.immutable.of("Key"), Lists.immutable.of("Id"));
```
`sideA`

|Key | Value|
|---|---:|
|"A" | 1|
|"B" | 2|
|"X" | 3|

`sideB`

|Id | Count|
|---|---:|
|"X" | 30|
|"B" | 10|
|"C" | 20|

Complement of B in A:

`result.getOne()`

|Key | Value|
|---|---:|
|"A" | 1|

Intersection (inner join) of A and B based on the key columns:

`result.getTwo()`

|Key | Value | Count|
|---|---:|---:|
|"B" | 2 | 10|
|"X" | 3 | 30|

Complement of A in B:

`result.getThree()`

|Id | Count|
|---|---:|
|"C" | 20|

#### Lookup Join

```Java
DataFrame pets = new DataFrame("Pets")
        .addStringColumn("Name").addLongColumn("Pet Kind Code")
        .addRow("Sweet Pea", 1)
        .addRow("Mittens",   2)
        .addRow("Spot",      1)
        .addRow("Eagly",     5)
        .addRow("Grzgxxch", 99);

DataFrame codes = new DataFrame("Pet Kinds")
        .addLongColumn("Code").addStringColumn("Description")
        .addRow(1, "Dog")
        .addRow(2, "Cat")
        .addRow(5, "Eagle")
        .addRow(7, "Snake");

pets.lookup(DfJoin.to(codes)
        .match("Pet Kind Code", "Code")
        .select("Description")
        .ifAbsent("Unclear"));
```

`pets`

|Name | Pet Kind Code | Description|
|---|---:|---|
|"Sweet Pea" | 1 | "Dog"|
|"Mittens" | 2 | "Cat"|
|"Spot" | 1 | "Dog"|
|"Eagly" | 5 | "Eagle"|
|"Grzgxxch" | 99 | "Unclear"|

There is another option to do lookup using a more fluent API directly on the `DataFrame` class:

```Java
pets.lookupIn(codes)
    .match("Pet Kind Code", "Code")
    .select("Description")
    .ifAbsent("Unclear")
    .resolveLookup();
```

Note that if using this approach, once all the lookup parameters are specified you will need to call the `resolveLookup()` method to actually execute the lookup.

### Pivot

Say we have a data frame of individual donut sales like this:

```Java
  DataFrame donutSales = new DataFrame("Donut Shop Purchases")
          .addStringColumn("Customer").addStringColumn("Month").addStringColumn("Donut Type")
          .addLongColumn("Qty").addDoubleColumn("Amount")
          .addRow("Alice", "Jan", "Blueberry", 10, 10.00)
          .addRow("Alice", "Feb", "Glazed", 10, 12.00)
          .addRow("Alice", "Feb", "Old Fashioned", 10, 8.00)
          .addRow("Alice", "Jan", "Blueberry", 10, 10.00)
          .addRow("Bob", "Jan", "Blueberry", 5, 5.00)
          .addRow("Bob", "Jan", "Pumpkin Spice", 5, 10.00)
          .addRow("Bob", "Jan", "Apple Cider", 4, 4.40)
          .addRow("Bob", "Mar", "Apple Cider", 8, 8.80)
          .addRow("Dave", "Jan", "Blueberry", 10, 10.00)
          .addRow("Dave", "Jan", "Old Fashioned", 20, 16.00)
          .addRow("Carol", "Jan", "Blueberry", 6, 6.00)
          .addRow("Carol", "Feb", "Old Fashioned", 12, 9.60)
          .addRow("Carol", "Mar", "Jelly", 10, 15.00)
          .addRow("Carol", "Jan", "Apple Cider", 12, 13.20)
          ;
```

`donutSales`

|Customer | Month | Donut Type | Qty | Amount|
|---|---|---|---:|---:|
|"Alice" | "Jan" | "Blueberry" | 10 | 10.0000|
|"Alice" | "Feb" | "Glazed" | 10 | 12.0000|
|"Alice" | "Feb" | "Old Fashioned" | 10 | 8.0000|
|"Alice" | "Jan" | "Blueberry" | 10 | 10.0000|
|"Bob" | "Jan" | "Blueberry" | 5 | 5.0000|
|"Bob" | "Jan" | "Pumpkin Spice" | 5 | 10.0000|
|"Bob" | "Jan" | "Apple Cider" | 4 | 4.4000|
|"Bob" | "Mar" | "Apple Cider" | 8 | 8.8000|
|"Dave" | "Jan" | "Blueberry" | 10 | 10.0000|
|"Dave" | "Jan" | "Old Fashioned" | 20 | 16.0000|
|"Carol" | "Jan" | "Blueberry" | 6 | 6.0000|
|"Carol" | "Feb" | "Old Fashioned" | 12 | 9.6000|
|"Carol" | "Mar" | "Jelly" | 10 | 15.0000|
|"Carol" | "Jan" | "Apple Cider" | 12 | 13.2000|

Now we want to see total number of donuts purchased by each customer in each month. So the month when the sale happen becomes a column in the pivot table, the customer is the grouping criteria and the aggregate value is the number of donuts sold to this customer in this month.

```Java
DataFrame qtyByCustomerAndMonth = donutSales.pivot(
        Lists.immutable.of("Customer"),
        "Month",
        Lists.immutable.of(sum("Qty")));
```
`qtyByCustomerAndMonth`

|Customer | Jan | Feb | Mar|
|---|---:|---:|---:|
|"Alice" | 20 | 20 | 0|
|"Bob" | 14 | 0 | 8|
|"Dave" | 30 | 0 | 0|
|"Carol" | 18 | 12 | 10|

It is possible to aggregate more than one value in a pivot table, then the pivot table will contain as many columns per each pivot column value as there are aggregations (in this case for each month there will be two columns):
```Java
DataFrame qtyAndAmountByCustomerAndMonth = donutSales.pivot(
        Lists.immutable.of("Customer"),
        "Month",
        Lists.immutable.of(sum("Qty"), sum("Amount")));
```

`qtyAndAmountByCustomerAndMonth`

|Customer | Jan:Qty | Jan:Amount | Feb:Qty | Feb:Amount | Mar:Qty | Mar:Amount|
|---|---:|---:|---:|---:|---:|---:|
|"Alice" | 20 | 20.0000 | 20 | 20.0000 | 0 | 0.0000|
|"Bob" | 14 | 19.4000 | 0 | 0.0000 | 8 | 8.8000|
|"Dave" | 30 | 26.0000 | 0 | 0.0000 | 0 | 0.0000|
|"Carol" | 18 | 19.2000 | 12 | 9.6000 | 10 | 15.0000|

## Domain Specific Language

The framework supports a simple Domain Specific Language (DSL) for computed column expression and operations on data frames such as filtering.

### Script

A DSL script is a sequence of one or more statements (see below for the kinds of statements supported). The result of executing a script is the value of the last statement (or expression) that was executed in the script.

### Value Types

The language supports variables and literal values of the following types:

| DSL Type    | Corresponding<br>Java Type | Notes                                                          |
|-------------|----------------------------|----------------------------------------------------------------|
| `STRING`    | `String`                   |                                                                |
| `INT`       | `int`                      | 32-bit integer values (up to  2^31 – 1)                        |
| `LONG`      | `long`                     | 64-bit integer values (up to 2^63 - 1)                         |
| `FLOAT`     | `float`                    | 32-bit floating point values                                   |
| `DOUBLE`    | `double`                   | 64-bit floating point values                                   |
| `DECIMAL`   | `BigDecimal`               | arbitrary precision (for all practical reasons) numbers        |
| `DATE`      | `LocalDate`                |                                                                |
| `DATE_TIME` | `LocalDateTime`            |                                                                |
| `BOOLEAN`   | `boolean`                  |                                                                |
| `VECTOR`    | n/a                        | a list of values, a tuple (values could be of different types) |
| `VOID`      | n/a                        | a singleton instance of this type represents `null` value      |

There is no implicit type conversion of values and variables to avoid errors, to minimize surprising results, and to fail early.

### Literals

The following are examples of literals

| Type        | Example                                                                                                                                                             |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `STRING`    | `"Hello"`<br>`'Abracadabra'`<br>(both single and double quotes are supported)                                                                                       |
| `LONG`      | `123`                                                                                                                                                               |
| `DOUBLE`    | `123.456`                                                                                                                                                           |
| `DECIMAL`   | There is no decimal literal per se, however there is a built-in function `toDecimal()` that lets specify decimal constants, e.g. `toDecimal(1234, 3)`               |
| `DATE`      | There is no date literal per se, however there is a built-in function `toDate()` that lets specify date constants, e.g. `toDate(2021, 11, 25)`                      |
| `DATE_TIME` | There is no date literal per se, however there is a built-in function `toDateTime()` that lets specify date/time constants, e.g. `toDate(2021, 11, 25, 12, 13, 14)` |
| `VECTOR`    | `(1, 2, 3)` <br>`('A', 'B', 'C')` <br>`(x, x + 1, x + 2)`                                                                                                           |
| `BOOLEAN`   | there are no literal of boolean type as there was no scenario where they would be required, however boolean variables and expressions are fully supported           |

### Variables

The variables in the DSL are immutable - once assigned, the value of the variable cannot change. This helps avoid errors arising from reusing variables.

A variable type is inferred at runtime and doesn't need to be declared.

Examples:
```
x = 123
123 in (x - 1, x, x + 1) ? 'in' : 'out'
a = "Hello, "
b = "there"
substr(a + b, 3)
```

### Expressions

| Category          | Expression                   | Example                                                                                                                      |
|-------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| Unary             | `-`<br>`not`                 | `-123`<br>`not (a > b)`                                                                                                      |
| Binary Arithmetic | `+` `-` `*` `/`              | `1 + 2`<br>`unit_price * quantity`<br>string concatenation:<br> `"Hello, " + "world!"`                                       |
| Comparison        | `>` `>=` `<` `<=` `==` `!=`  |                                                                                                                              |
| Boolean           | `and`<br>`or`<br>`xor`       |                                                                                                                              |
| Containment       | `in`<br>`not in`             | vectors: <br>`"a" in ("a", "b", "c")`<br>`x not in (1, 2, 3)`<br>strings:<br>`'ello' in 'Hello!'`<br>`"bye" not in "Hello!"` |
| Empty             | `is empty`<br>`is not empty` | `"" is empty`<br>`'Hello' is not empty`<br>vectors:<br>`(1, 2, 3) is not empty`<br>`() is empty`                             |
| Null check        | `is null`<br>`is not null`   | `"" is null`<br>`'Hello' is not null`<br>`x is null ? 0.0 : abs(x)`                                                          |

### Statements

The following statements are available:
- assignment
- conditional
- a freestanding expression

### Functions

There are two types of functions—intrinsic (built-in) and explicitly declared using the DSL `function` declaration.

Recursion (direct or indirect) is not supported.

#### Built-in functions

| Function   | Usage                                                           |
|------------|-----------------------------------------------------------------|
| abs        | abs(number)                                                     |
| contains   | contains(string, substring)                                     |
| print      | print(value)                                                    |
| println    | println(value)                                                  |
| startsWith | startsWith(string, prefix)                                      |
| substr     | substr(string, beginIndex[, endIndex])                          |
| toDate     | toDate(string in the yyyy-mm-dd format)<br>toDate(yyyy, mm, dd) |
| toDateTime | toDateTime(yyyy, mm, dd, hh, mm, ss)                            |
| toDouble   | toDouble(string)                                                |
| toLong     | toLong(string)                                                  |
| toDecimal  | toUpper(unscaledValue, scale)                                   |
| toString   | toString(number)                                                |
| toUpper    | toUpper(string)                                                 |
| withinDays | withinDays(date1, date2, numberOfDays)                          |

#### User Declared Function Example 1
```
function abs(x)
{
  if x > 0 then
    x
  else
    -x
  endif
}
abs(-123)
```

#### User Declared Function Example 2
```
function hello()
{
  'Hello'
}

hello() + ' world!'
```
