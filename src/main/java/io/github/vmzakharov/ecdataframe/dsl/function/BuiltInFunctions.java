package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import io.github.vmzakharov.ecdataframe.util.Printer;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import static io.github.vmzakharov.ecdataframe.dsl.value.Value.VOID;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.BOOLEAN;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DECIMAL;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.VECTOR;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

final public class BuiltInFunctions
{
    private static final MutableMap<String, IntrinsicFunctionDescriptor> FUNCTIONS_BY_NAME = Maps.mutable.of();

    static
    {
        resetFunctionList();
    }

    private BuiltInFunctions()
    {
        // Utility class
    }

    /**
     * Clears out and repopulates the list of the intrinsic (built-in) functions. This operation will reset the
     * list to its initial state so any functions added later programmatically will no longer be available.
     */
    public static void resetFunctionList()
    {
        FUNCTIONS_BY_NAME.clear();

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("print")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = this.getParameterVectorFrom(context);
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                return VOID;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("println")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = this.getParameterVectorFrom(context);
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                printer.newLine();
                return VOID;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("startsWith")
                .parameterNames("string", "prefix")
                .returnType(BOOLEAN)
                .action(context ->
                        BooleanValue.valueOf(context.getString("string").startsWith(context.getString("prefix")))
                )
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("contains")
                .parameterNames("string", "substring")
                .returnType(BOOLEAN)
                .action(context -> BooleanValue.valueOf(context.getString("string").contains(context.getString("substring"))))
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("toUpper")
                .parameterNames("string")
                .returnType(STRING)
                .action(context -> new StringValue(context.getString("string").toUpperCase()))
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("trim")
                .parameterNames("string")
                .returnType(STRING)
                .action(context -> new StringValue(context.getString("string").trim()))
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("toDecimal").parameterNames("unscaledValue", "scale")
                .returnType(DECIMAL)
                .action(context ->
                    new DecimalValue(BigDecimal.valueOf(context.getLong("unscaledValue"), (int) context.getLong("scale")))
                )
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("substr")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = this.getParameterVectorFrom(context);
                int parameterCount = parameters.size();
                if (parameterCount != 2 && parameterCount != 3)
                {
                    exceptionByKey("DSL_INVALID_PARAM_COUNT")
                            .with("functionName", this.getName()).with("usageString", this.usageString())
                            .fire();
                }

                Value stringParam = parameters.get(0);
                if (stringParam.isVoid())
                {
                    return VOID;
                }

                String aString = stringParam.stringValue();

                int beginIndex = (int) ((LongValue) parameters.get(1)).longValue();

                String substring = (parameterCount == 2)
                        ?
                        aString.substring(beginIndex)
                        :
                        aString.substring(beginIndex, (int) ((LongValue) parameters.get(2)).longValue());

                return new StringValue(substring);
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(string, beginIndex[, endIndex])";
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return STRING;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("abs", Lists.immutable.of("number"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("number");

                if (parameter.isVoid())
                {
                    return VOID;
                }

                if (!parameter.isNumber())
                {
                    this.assertParameterType(Lists.immutable.of(DOUBLE, LONG), parameter.getType());
                }

                if (parameter.isDouble())
                {
                    return new DoubleValue(Math.abs(((DoubleValue) parameter).doubleValue()));
                }

                return new LongValue(Math.abs(((LongValue) parameter).longValue()));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                // todo - error handling
                return parameterTypes.get(0);
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("toString")
                .parameterNames("number")
                .returnType(STRING)
                .action(context -> new StringValue(context.getVariable("number").stringValue()))
        );

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("v")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                return this.getParameterVectorFrom(context);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return VECTOR;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("format", Lists.immutable.of("object", "pattern"))
        {
            private final MutableMap<String, DecimalFormat> decimalFormats = Maps.mutable.of();
            private final MutableMap<String, DateTimeFormatter> dateTimeFormatters = Maps.mutable.of();

            @Override
            public StringValue evaluate(EvalContext context)
            {
                Value value = context.getVariable("object");
                String pattern = context.getString("pattern");

                String result = "not used";
                if (value.isNumber())
                {
                    result = this.decimalFormats
                            .computeIfAbsent(pattern, DecimalFormat::new)
                            .format(
                                value.isLong() ? ((LongValue) value).longValue() : ((DoubleValue) value).doubleValue()
                            );
                }
                else if (value.isTemporal())
                {
                    result = this.dateTimeFormatters
                            .computeIfAbsent(pattern, DateTimeFormatter::ofPattern)
                            .format(
                                value.isDate() ? ((DateValue) value).dateValue() : ((DateTimeValue) value).dateTimeValue()
                            );
                }
                else
                {
                    this.assertParameterType(Lists.immutable.of(LONG, DOUBLE, DATE, DATE_TIME), value.getType());
                }

                return new StringValue(result);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return STRING;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "([number or date value], pattern)";
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDate")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = this.getParameterVectorFrom(context);

                LocalDate date = null;
                if (parameters.size() == 3)
                {
                    Value yearValue = parameters.get(0);
                    Value monthValue = parameters.get(1);
                    Value dayValue = parameters.get(2);

                    if (yearValue.isVoid() || monthValue.isVoid() || dayValue.isVoid())
                    {
                        return VOID;
                    }

                    date = LocalDate.of(
                            (int) ((LongValue) yearValue).longValue(),
                            (int) ((LongValue) monthValue).longValue(),
                            (int) ((LongValue) dayValue).longValue());
                }
                else if (parameters.size() == 1)
                {
                    Value dateAsString = parameters.get(0);

                    if (dateAsString.isVoid())
                    {
                        return VOID;
                    }

                    date = LocalDate.parse(dateAsString.stringValue(), DateTimeFormatter.ISO_DATE);
                }
                else
                {
                    // forces to fail
                    this.assertParameterCount(1, parameters.size());
                }

                return new DateValue(date);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return DATE;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(\"yyyy-mm-dd\") or " + this.getName() + "(yyyy, mm, dd)";
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDateTime")
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = this.getParameterVectorFrom(context);
                LocalDateTime dateTime = null;
                int paramCount = parameters.size();
                if (parameters.size() > 4)
                {
                    int[] params = new int[paramCount];
                    for (int i = 0; i < paramCount; i++)
                    {
                        Value parameter = parameters.get(i);
                        if (parameter.isVoid())
                        {
                            return VOID;
                        }

                        params[i] = (int) ((LongValue) parameter).longValue();
                    }

                    switch (paramCount)
                    {
                        case 5:
                            dateTime = LocalDateTime.of(params[0], params[1], params[2], params[3], params[4]);
                            break;
                        case 6:
                            dateTime = LocalDateTime.of(params[0], params[1], params[2], params[3], params[4], params[5]);
                            break;
                        case 7:
                            dateTime = LocalDateTime.of(params[0], params[1], params[2], params[3], params[4], params[5], params[6]);
                            break;
                        default:
                            this.assertParameterCount(1, parameters.size()); // forced fail
                    }
                }
                else if (parameters.size() == 1)
                {
                    Value dateTimeAsString = parameters.get(0);

                    if (dateTimeAsString.isVoid())
                    {
                        return VOID;
                    }

                    dateTime = LocalDateTime.parse(dateTimeAsString.stringValue(), DateTimeFormatter.ISO_DATE_TIME);
                }
                else
                {
                    // forces to fail
                    this.assertParameterCount(1, parameters.size());
                }

                return new DateTimeValue(dateTime);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return DATE_TIME;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(\"yyyy-mm-ddThh:mm[:ss[.nnnn]]\") or " + this.getName() + "(yyyy, mm, dd, hh, mm[, ss[, nnnn]])";
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toLong", Lists.immutable.of("string"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("string");
                if (parameter.isVoid())
                {
                    return VOID;
                }

                this.assertParameterType(STRING, parameter.getType());
                String aString = parameter.stringValue();

                return new LongValue(Long.parseLong(aString));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return LONG;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDouble", Lists.immutable.of("string"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("string");
                if (parameter.isVoid())
                {
                    return VOID;
                }

                this.assertParameterType(STRING, parameter.getType());
                String aString = parameter.stringValue();

                return new DoubleValue(Double.parseDouble(aString));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return DOUBLE;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDecimal", Lists.immutable.of("unscaledValue", "scale"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value unscaled = context.getVariable("unscaledValue");
                if (unscaled.isVoid())
                {
                    return VOID;
                }

                Value scale = context.getVariable("scale");
                if (scale.isVoid())
                {
                    return VOID;
                }

                this.assertParameterType(LONG, unscaled.getType());
                this.assertParameterType(LONG, scale.getType());

                return new DecimalValue(BigDecimal.valueOf(
                        ((LongValue) unscaled).longValue(),
                        (int) ((LongValue) scale).longValue()
                ));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return DECIMAL;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptorBuilder("withinDays")
                .parameterNames("date1", "date2", "numberOfDays")
                .returnType(BOOLEAN)
                .action(context -> {
                        Period period = Period.between(context.getDate("date1"), context.getDate("date2"));
                        return BooleanValue.valueOf(Math.abs(period.getDays()) <= context.getLong("numberOfDays"));
                    })
        );
    }

    public static void addFunctionDescriptor(IntrinsicFunctionDescriptor fd)
    {
        FUNCTIONS_BY_NAME.put(fd.getNormalizedName(), fd);
    }

    public static IntrinsicFunctionDescriptor getFunctionDescriptor(String name)
    {
        return FUNCTIONS_BY_NAME.get(name);
    }

    public static MapIterable<String, IntrinsicFunctionDescriptor> getFunctionsByName()
    {
        return FUNCTIONS_BY_NAME.toImmutable();
    }
}
