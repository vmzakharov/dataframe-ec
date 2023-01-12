package io.github.vmzakharov.ecdataframe.util;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.addMessage;

final public class ConfigureMessages
{
    private ConfigureMessages()
    {
        // utility class
    }

    public static void initialize()
    {
        addMessage("AGG_NOT_APPLICABLE",              "Aggregation '${operation}' (${operationDescription}) cannot be performed on ${operationScope}");
        addMessage("AGG_NO_INITIAL_VALUE",            "Aggregation ${operation} does not have a long initial value");
        addMessage("AGG_NO_ACCUMULATOR",              "Aggregation ${operation} does not support a ${type} accumulator");
        addMessage("DF_DUPLICATE_COLUMN",             "Column named '${columnName}' is already in data frame '${dataFrameName}'");
        addMessage("DF_COLUMN_DOES_NOT_EXIST",        "Column '${columnName}' does not exist in data frame '${dataFrameName}'");
        addMessage("DF_ADDING_ROW_TOO_WIDE",          "Adding more row elements (${elementCount}) than there are columns in the data frame (${columnCount})");
        addMessage("DF_ADD_COL_UNKNOWN_TYPE",         "Cannot add a column ${columnName} for values of type ${type}");
        addMessage("DF_DIFFERENT_COL_SIZES",          "Stored column sizes are not the same when attempting to seal data frame '${dataFrameName}'");
        addMessage("DF_UNION_DIFF_COL_COUNT",         "Attempting to union data frames with different numbers of columns");
        addMessage("DF_JOIN_DIFF_KEY_COUNT",          "Attempting to join dataframes by different number of keys on each side: ${side2KeyList} to ${side2KeyList}");
        addMessage("DF_NO_COL_COMPARATOR",            "Column comparator is not implemented for column ${columnName} of type ${type}");
        addMessage("DF_COL_ALREADY_LINKED",           "Column '${columnName}' has already been linked to a data frame");
        addMessage("DF_COL_CLONE_FAILED",             "Failed to clone schema from column ${name}");
        addMessage("DF_BAD_VAL_ADD_TO_COL",           "Attempting to add a value ${value} of type ${valueType} to a column ${columnName} of type ${columnType}");
        addMessage("DF_SET_VAL_ON_COMP_COL",          "Cannot set a value on computed column '${columnName}'");
        addMessage("DF_AGG_VAL_TO_COMP_COL",          "Cannot store aggregated value into a computed column '${columnNane}'");
        addMessage("DF_COMP_COL_MODIFICATION",        "Cannot directly modify computed column '${columnName}'");
        addMessage("CSV_FILE_WRITE_FAIL",             "Failed to write data frame to '${fileName}'");
        addMessage("CSV_UNSUPPORTED_VAL_TO_STR",      "Do not know how to convert value of type ${valueType} to a string");
        addMessage("CSV_INFER_SCHEMA_FAIL",           "Failed to infer schema from  '${fileName}'");
        addMessage("CSV_MISSING_COL_HEADER",          "Error parsing a CSV file: a column header cannot be empty");
        addMessage("CSV_SCHEMA_HEADER_SIZE_MISMATCH", "The number of elements in the header (${headerCount}) does not match the number of columns in the schema (${schemaColumnCount})");
        addMessage("CSV_SCHEMA_HEADER_NAME_MISMATCH", "Mismatch between the column header names in the data set ${headerColumnList} and in the schema: ${schemaColumnList}");
        addMessage("CSV_FILE_LOAD_FAIL",              "Failed to load file as a data frame '${fileName}'");
        addMessage("CSV_POPULATING_BAD_COL_TYPE",     "Attempting to populate unsupported column type: ${columnType}");
        addMessage("CSV_UNBALANCED_QUOTES",           "Unbalanced quotes at index ${index} in ${aString}");
        addMessage("CSV_PARSE_ERR",                   "Failed to parse input string to ${type}: '${inputString}'");
        addMessage("OBJ_END_OF_DATA_SET",             "No more elements in data set ${dataSetName}");
        addMessage("OBJ_METHOD_INVOKE_FAIL",          "Failed to invoke ${method}");
        addMessage("OBJ_PROPERTY_NOT_FOUND",          "Unable to find property ${property} on ${className}");
        addMessage("OBJ_EMPTY_DATA_SET_ACCESS",       "Attempting to access elements of the object data set ${dataSetName} with no data");
        addMessage("DSL_DF_EVAL_NO_DATASET",          "Cannot add a data set to a data frame evaluation context");
        addMessage("DSL_ATTEMPT_TO_REMOVE_DF_VAR",    "Cannot remove variables from a data frame evaluation context");
        addMessage("DSL_VAR_IMMUTABLE",               "Attempting to change immutable variable '${variableName}'");
        addMessage("DSL_FUN_NOT_IMPLEMENTED",         "Function ${functionName} is not implemented");
        addMessage("DSL_INVALID_PARAM_COUNT",         "Invalid number of parameters in a call to '${functionName}'. ${usageString}");
        addMessage("DSL_INVALID_PARAM_TYPE",          "Invalid parameter type in a call to '${functionName}'. ${usageString}");
        addMessage("DSL_OP_NOT_SUPPORTED",            "Cannot apply '${operation}' to ${type}");
        addMessage("DSL_VAR_UNINITIALIZED",           "Uninitialized variable: ${variableName}");
        addMessage("DSL_NULL_VALUE_NOT_ALLOWED",      "${type} value cannot contain null, a void value should be used instead");
        addMessage("DSL_COMPARE_TO_NULL",             "Cannot compare a ${className} to null");
        addMessage("DSL_COMPARE_INCOMPATIBLE",        "Cannot compare a ${className} to a ${otherClassName}");
        addMessage("DSL_DF_COMPARE_UNSUPPORTED",      "Data Frame value comparison is not supported");
        addMessage("DSL_NO_DEC_TO_FLOAT_CONVERSION",  "Cannot convert decimal value to floating point");
        addMessage("DSL_UNDEFINED_OP_ON_VALUE",       "Undefined operation ${operation} on ${value}");
        addMessage("DSL_COMPARE_NOT_SUPPORTED",       "Comparison of values of type ${type} is not supported");
        addMessage("DSL_EXPLICIT_PARAM_MISMATCH",     "Parameter count mismatch in an invocation of '${functionName}'");
        addMessage("DSL_UNKNOWN_FUN",                 "Unknown function: '${functionName}'");
        addMessage("DSL_FAIL_TO_CONVERT_RAW_VAL",     "Don't know how to convert to value ${rawValue}, type: ${rawValueType}");
        addMessage("DSL_FUN_DECLARATION_EVAL",        "A standalone function declaration cannot be evaluated. Function: ${functionName}");
    }
}
