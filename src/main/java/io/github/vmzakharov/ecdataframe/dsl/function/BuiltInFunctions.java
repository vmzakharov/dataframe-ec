package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
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

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

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
            public Value evaluate(VectorValue parameters)
            {
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                return Value.VOID;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("println")
        {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                printer.newLine();
                return Value.VOID;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("startsWith", Lists.immutable.of("string", "prefix"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                String aString = context.getVariable("string").stringValue();
                String aPrefix = context.getVariable("prefix").stringValue();

                return BooleanValue.valueOf(aString.startsWith(aPrefix));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.BOOLEAN;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("contains", Lists.immutable.of("string", "substring"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                String aString = context.getVariable("string").stringValue();
                String substring = context.getVariable("substring").stringValue();

                return BooleanValue.valueOf(aString.contains(substring));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.BOOLEAN;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toUpper", Lists.immutable.of("string"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                return new StringValue(context.getVariable("string").stringValue().toUpperCase());
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return ValueType.STRING;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("substr")
        {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                int parameterCount = parameters.size();
                if (parameterCount != 2 && parameterCount != 3)
                {
                    throw new RuntimeException("Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
                }

                String aString = parameters.get(0).stringValue();
                int beginIndex = (int) ((LongValue) parameters.get(1)).longValue();

                String substring = (parameterCount == 2)
                        ?
                        aString.substring(beginIndex)
                        :
                        aString.substring(beginIndex, (int) ((LongValue) parameters.get(2)).longValue());

                return new StringValue(substring);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.STRING;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(string, beginIndex[, endIndex])";
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("abs", Lists.immutable.of("number"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("number");

                if (!parameter.isNumber())
                {
                    this.assertParameterType(parameter.getType(), ValueType.NUMBER);
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

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toString", Lists.immutable.of("number"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value value = context.getVariable("number");

                return new StringValue(value.stringValue());
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.STRING;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDate")
        {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(1, parameters.size());

                String aString = parameters.get(0).stringValue();

                return new DateValue(LocalDate.parse(aString, DateTimeFormatter.ISO_DATE));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return ValueType.DATE;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(yyyy-mm-dd)";
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toLong", Lists.immutable.of("string"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("string");
                this.assertParameterType(ValueType.STRING, parameter.getType());
                String aString = parameter.stringValue();

                return new LongValue(Long.parseLong(aString));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                return ValueType.LONG;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("toDouble", Lists.immutable.of("string"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                Value parameter = context.getVariable("string");
                this.assertParameterType(ValueType.STRING, parameter.getType());
                String aString = parameter.stringValue();

                return new DoubleValue(Double.parseDouble(aString));
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.DOUBLE;
            }
        });

        addFunctionDescriptor(new IntrinsicFunctionDescriptor("withinDays", Lists.immutable.of("date1", "date2", "numberOfDays"))
        {
            @Override
            public Value evaluate(EvalContext context)
            {
                LocalDate date1 = ((DateValue) context.getVariable("date1")).dateValue();
                LocalDate date2 = ((DateValue) context.getVariable("date2")).dateValue();
                long numberOfDays = ((LongValue) context.getVariable("numberOfDays")).longValue();

                Period period = Period.between(date1, date2);

                return BooleanValue.valueOf(period.getDays() <= numberOfDays);
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.BOOLEAN;
            }
        });
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
