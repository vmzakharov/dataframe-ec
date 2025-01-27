# Support For Null Values

## General

Null values are supported for all the column types including primitive types, and for all DSL expressions, functions, and aggregation functions. 

There are scenarios where representing nulls is not possible, usually when extracting primitive data out of a data frame as values of Java primitive types or calculations, the type of the result of which is declared as  Java primitive type. 

In these cases, if a `null` is encountered, an instance of `java.lang.NullPointerException` (NPE) will be thrown.

For example, if `longColumn` is a data frame column of type `DfLongColumn` (stores `long` values):

| Expression                    | Behavior                                                    |
|-------------------------------|-------------------------------------------------------------|
| `longColumn.longColumn(5)`    | throws an NPE if the column value at row index `5` is `null` |
| `longColumn.asLongIterable()` | throws an NPE if one or more column values are `null`        |

## About Nulls

### 1. `null` means "I don't know"

`null` does not mean "empty" or "blank" or "zero" or any other specific value in the valid value range for the type. It means the value is unknown or missing. 

### 2. `null`s are poisonous

Using `null` as an operand in an expression results in that expression evaluating to `null`. This sort of follows property 1. How much is `5 + I don't know`? Well, it's `I don't know`. And so on.

One edge case here is using `null` in boolean expressions. Specifically, one can argue that<br>
`T or null == T`<br>
and<br>
`F and null == F`

### 3. `null == null` is `false`

For the same reason it is impossible to compare nulls as we don't know the values being compared. You can compare null-ity though, for example in a DSL expression like this

```javascript
x is null == y is null // will evaluate to true if both x and y are nulls
```

## Null Support in the Expression DSL

The expression DSL generally conforms to the three principles listed above, including how it treats boolean expressions (that is, `true or null` is `true`, `false and null` is `false`).

Note that in the Java code the `null` value in the DSL is represented by the constant `Value.VOID` which is the only instance of the value type of `VoidValue`. 

### Operators

The data frame expression DSL supports operators for checking for nulls as well as operators for checking for empty values.

| Operator         | Description                                             | Notes                                                                                                     |
|------------------|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `x is null`      | returns `true` if the value of `x` is null              |                                                                                                           |
| `x is not null`  | returns `true` if the value of `x` is not null          |                                                                                                           |
| `x is empty`    | returns `true` if the value of `x` is empty or null     | for most types there is really no "empty" value, but stings and lists<br/> can be properly empty and not nulls |
| `x is not empty` | returns `true` if the value of `x` is not empty |                                                                                                           |


### Built-in Functions

Built-in functions will return `null` (`VOID`) if any of the parameters is `null`, which is a sensible behavior for those functions.

### Aggregation Functions

By default, aggregation functions treat null values as "poisonous" - that is any null value passed in an aggregator will cause the result of the entire aggregation to be null, which is a sensible behavior for most aggregations.

## Reality Requires Flexibility

### Reality

Unfortunately, in many real life scenarios, especially in the context of legacy data flows, the rules 1-3 are not obeyed. It could be due to performance or storage size concerns (most likely no longer valid) or simply due to bad design of the data model and not understanding the meaning of "null".

Thus, for the framework to be broadly useful, it needs to be able to support whatever weird and wonderful treatment of nulls exists in the real live production workflows. 

### Flexibility

The expression DSL allows adding custom functions and aggregation functions at runtime. These functions can define how nulls are treated (e.g., processed correctly, or ignored, or processed as empty values).

For examples of aggregation functions handling nulls and treating nulls differently see the `DataFrameAggregationNullsAreOkayTest` class.



