# dataframe-ec
A tabular data structure (aka a data frame) based on the Eclipse Collections framework. A data frame store is columnar, with focus on memory optimization achieved by using efficient Eclipse Collections data structures and APIs.

For more on Eclipse Collections see: https://www.eclipse.org/collections/.

## Where to Get It

```xml
<dependency>
  <groupId>io.github.vmzakharov</groupId>
  <artifactId>dataframe-ec</artifactId>
  <version>0.4.1</version>
</dependency>
```

## Data Frame Operations

- create a data frame programmatically or load from a csv file
- add a column to a data frame, columns can be
  - stored or computed
  - of type: string, integer (long), double, date
- drop one or more columns
- select a subset of rows based on a criteria
- sort by one or more columns or by an expression
- union - concatenating data frames with the same schemas
- join with another data frame, based on a spefied column value, inner and outer joins are supported
- aggregation - grouping by or summing column values
- flag rows - individually or matching a criteria. 

### Examples

#### Creating a Data Frame
A data frame can be **loaded from a CSV file**. Let's say there is a file called "donut_orders.csv" with the following contents:
```
Customer,  Count,  Price,  Date
"Archibald", 5.0, 23.45, 2020-10-15
"Bridget", 10.0, 40.34, 2020-11-10
"Clyde", 4.0, 19.5, 2020-10-19
```
Then a data frame can be loaded from this file as shown below. The file schema can be inferred, like in this example, or specified explicitly.
```
DataFrame ordersFromFile  = new CsvDataSet("donut_orders.csv", "Donut Orders").loadAsDataFrame();
```
`ordersFromFile` contains:

Customer |  Count |  Price |  Date
--- | --- | --- | ---
"Archibald" | 5.0 | 23.45 | 2020-10-15
"Bridget" | 10.0 | 40.34 | 2020-11-10
"Clyde" | 4.0 | 19.5 | 2020-10-19

A data frame can be created **programmatically**: 
```
DataFrame orders = new DataFrame("Donut Orders")
    .addStringColumn("Customer").addLongColumn("Count").addDoubleColumn("Price").addDateColumn("Date")
    .addRow("Alice",  5, 23.45, LocalDate.of(2020, 10, 15))
    .addRow("Bob",   10, 40.34, LocalDate.of(2020, 11, 10))
    .addRow("Alice",  4, 19.50, LocalDate.of(2020, 10, 19))
    .addRow("Carl",  11, 44.78, LocalDate.of(2020, 12, 25))
    .addRow("Doris",  1,  5.00, LocalDate.of(2020,  9,  1));
```
`orders` contains:

Customer | Count | Price | Date
 --- |  --- |  --- | ---
"Alice" | 5 | 23.45 | 2020-10-15
"Bob" | 10 | 40.34 | 2020-11-10
"Alice" | 4 | 19.5 | 2020-10-19
"Carl" | 11 | 44.78 | 2020-12-25
"Doris" | 1 | 5.0 | 2020-09-01

#### Sum of Columns
```
DataFrame totalOrdered = orders.sum(Lists.immutable.of("Count", "Price"));
```

`totalOrdered` contains:

Count | Price
 --- | ---
31 | 133.07

#### Sum With Group By
```
DataFrame totalsByCustomer = orders.sumBy(Lists.immutable.of("Count", "Price"), Lists.immutable.of("Customer"));
```

`totalsByCustomer` contains:

Customer | Count | Price
 --- |  --- | ---
"Alice" | 9 | 42.95
"Bob" | 10 | 40.34
"Carl" | 11 | 44.78
"Doris" | 1 | 5.0

#### Add a Calculated Column
```
orders.addDoubleColumn("AvgDonutPrice", "Price / Count");
```
`orders` contains

Customer | Count | Price | Date | AvgDonutPrice
 --- |  --- |  --- |  --- | ---
"Alice" | 5 | 23.45 | 2020-10-15 | 4.69
"Bob" | 10 | 40.34 | 2020-11-10 | 4.034
"Alice" | 4 | 19.5 | 2020-10-19 | 4.875
"Carl" | 11 | 44.78 | 2020-12-25 | 4.071
"Doris" | 1 | 5.0 | 2020-09-01 | 5.0

#### Filter
Selection of a sub dataframe with the rows matching the filter condition
```
DataFrame bigOrders = orders.selectBy("Count >= 10");
```
`bigOrders` contains:

Customer | Count | Price | Date | AvgDonutPrice
 --- |  --- |  --- |  --- | ---
"Bob" | 10 | 40.34 | 2020-11-10 | 4.034
"Carl" | 11 | 44.78 | 2020-12-25 | 4.071

Select two subsets both matching and not matching the filter condition respectively
```
Twin<DataFrame> highAndLow = orders.partition("Count >= 10");
```
Result - a pair of data frames:

`highAndLow.getOne()`

Customer | Count | Price | Date | AvgDonutPrice
--- |  --- |  --- |  --- | ---
"Bob" | 10 | 40.34 | 2020-11-10 | 4.034
"Carl" | 11 | 44.78 | 2020-12-25 | 4.071

`highAndLow.getTwo()`

Customer | Count | Price | Date | AvgDonutPrice
 --- |  --- |  --- |  --- | ---
"Alice" | 5 | 23.45 | 2020-10-15 | 4.69
"Alice" | 4 | 19.5 | 2020-10-19 | 4.875
"Doris" | 1 | 5.0 | 2020-09-01 | 5.0

#### Drop Column
```
orders.dropColumn("AvgDonutPrice");
```
`orders` contains:

Customer | Count | Price | Date
--- |  --- |  --- | ---
"Alice" | 5 | 23.45 | 2020-10-15
"Bob" | 10 | 40.34 | 2020-11-10
"Alice" | 4 | 19.5 | 2020-10-19
"Carl" | 11 | 44.78 | 2020-12-25
"Doris" | 1 | 5.0 | 2020-09-01
#### Sort
Sort by the order date:
```
orders.sortBy(Lists.immutable.of("Date"));
```
`orders`:

Customer | Count | Price | Date
 --- |  --- |  --- | ---
"Doris" | 1 | 5.0 | 2020-09-01
"Alice" | 5 | 23.45 | 2020-10-15
"Alice" | 4 | 19.5 | 2020-10-19
"Bob" | 10 | 40.34 | 2020-11-10
"Carl" | 11 | 44.78 | 2020-12-25

Sort by Customer ignoring the first letter of their name
```
orders.sortByExpression("substr(Customer, 1)");
```
`orders`:

Customer | Count | Price | Date
 --- |  --- |  --- | ---
"Carl" | 11 | 44.78 | 2020-12-25
"Alice" | 5 | 23.45 | 2020-10-15
"Alice" | 4 | 19.5 | 2020-10-19
"Bob" | 10 | 40.34 | 2020-11-10
"Doris" | 1 | 5.0 | 2020-09-01

#### Union
```
DataFrame otherOrders = new DataFrame("Other Donut Orders")
    .addStringColumn("Customer").addLongColumn("Count").addDoubleColumn("Price").addDateColumn("Date")
    .addRow("Eve",  2, 9.80, LocalDate.of(2020, 12, 5));

DataFrame combinedOrders = orders.union(otherOrders);
```
`combinedOrders`:

Customer | Count | Price | Date
--- | --- | --- | ---
"Alice" | 5 | 23.45 | 2020-10-15
"Bob" | 10 | 40.34 | 2020-11-10
"Alice" | 4 | 19.5 | 2020-10-19
"Carl" | 11 | 44.78 | 2020-12-25
"Doris" | 1 | 5.0 | 2020-09-01
"Eve" | 2 | 9.8 | 2020-12-05

## Domain Specific Language

The framework supports a simple Domain Specific Language (DSL) for computed column expression and operations on data frames such as filtering. 

### Script

A DSL script is a one or more statements (see below for the kinds of statements supported). The result of executing a script is the value of the last statement (or expression) that was executed in the script.

### Value Types

The language supports variable and literal values of the following types:
- string
- long
- double
- date
- vector
- boolean

There is no implicit type conversion of values and variables to avoid inadvertent errors, surprises, and to fail early.   

### Literals

The following are examples of literals

Type | Example
--- | ---
String | `"Hello"` or `'Abracadabra'` (both single and double quotes are supported)
Long | `123`
Double | `123.456`
Date | There is not date literal per se, however there is a built-in function `toDate()` that lets specify date constants `toDate(2021, 11, 25)` 
Vector | `(1, 2, 3)` <br>`('A', 'B', 'C')` <br>`(x, x + 1, x + 2)`
Boolean | there are no literal of boolean type as there was no scenario where they would be required, however boolean variables and expressions are fully supported

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

Category | Type | Example
------------ | ------------ | -------------
Unary | `-`<br>`not` | `-123`<br>`not (a > b)` 
Binary Arithmetic | `+` `-` `*` `/` | `1 + 2`<br>`unit_price * quantity`<br>string concatenation:<br> `"Hello, " + "world!"`
Comparison |`>` `>=` `<` `<=` `==` `!=`|
Boolean | `and`<br>`or`<br>`xor` |
Containment | `in`<br>`not in` | vectors: <br>`"a" in ("a", "b", "c")`<br>`x not in (1, 2, 3)`<br>strings:<br>`'ello' in 'Hello!'`<br>`"bye" not in "Hello!"`
Empty | `is empty`<br>`is not empty` | `"" is empty`<br>`'Hello' is not empty`<br>vectors:<br>`(1, 2, 3) is not empty`<br>`() is empty`

### Statements

The following statements are available: 
- assignment
- conditional
- a free-standing expression

### Functions

There are two types of functions - intrinsic (built-in) and explicitly declared using the DSL `function` declaration.

Recursion (direct or indirect) is not supported.

#### Example 1
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

#### Example 2
```
function hello()
{
  'Hello'
} 

hello() + ' world!'
```
