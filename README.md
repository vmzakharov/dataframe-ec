# dataframe-ec
A tabular data structure (aka a data frame) based on the Eclipse Collections framework

## Data Frame Operations

- create a data frame programmatically or load from a csv file
- add a column to a data frame, columns can be
  - stored or computed
  - of type: string, integer (long), double, date
- drop a column
- select a subset of rows based on a criteria
- sort 
- union
- join
- aggregation

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
------------ | -------------
String | `"Hello"` or `'Abracadabra'` (both single and double quotes are supported)
Long | `123`
Double | `123.456`
Date | There is not date literal per se, however there is a built-in function `toDate()` that lets specify date constants `toDate(2021, 11, 25)` 
Vector | `(1, 2, 3)` <br>`('A', 'B', 'C')` <br>`(x, x + 1, x + 2)`
Boolean | there are no literal of boolean type as there was no scenario where they would be required, however boolean variables and expressions are fully supported

### Variables

The variables in the DSL are immutable - once assigned, the value of the variable cannot change. This helps avoid errors arising from reusing variables.

Variable type is inferred at runtime and doesn't need to be declared.

### Expressions

...

### Statements

The following statements are available: 
- assignment
- conditional
- a free-standing expression

### Functions

There are two types of functions - intrinsic (built-in) and explicitly declared using the DSL `function` declaration.

Recursion (direct or indirect) is not supported.

Example
```
function abs(x)
  if x > 0 then
    x
  else
    -x
  endif
  
abs(-123)
```

