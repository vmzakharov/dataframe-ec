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
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

final public class BuiltInFunctions
{
    private static final ImmutableList<IntrinsicFunctionDescriptor> FUNCTIONS = Lists.immutable.of(
        new IntrinsicFunctionDescriptor("print") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                return Value.VOID;
            }
        },

        new IntrinsicFunctionDescriptor("println") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().collect(Value::stringValue).forEach(printer::print);
                printer.newLine();
                return Value.VOID;
            }
        },

        new IntrinsicFunctionDescriptor("startsWith", Lists.immutable.of("string", "prefix")) {
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
        },

        new IntrinsicFunctionDescriptor("contains", Lists.immutable.of("string", "substring")) {
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
        },

        new IntrinsicFunctionDescriptor("toUpper", Lists.immutable.of("string")) {
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
        },

        new IntrinsicFunctionDescriptor("substr") {
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
        },

        new IntrinsicFunctionDescriptor("abs") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(1, parameters.size());

                Value value = parameters.get(0);
                if (value.isDouble())
                {
                    return new DoubleValue(Math.abs(((DoubleValue) value).doubleValue()));
                }
                else if (value.isLong())
                {
                    return new LongValue(Math.abs(((LongValue) value).longValue()));
                }
                else
                {
                    throw new RuntimeException("Invalid parameter type (" + value.getType() + ") in a call to '" + this.getName() + "'. " + this.usageString());
                }
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(number)";
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> parameterTypes)
            {
                // todo - error handling
                return parameterTypes.get(0);
            }
        },

        new IntrinsicFunctionDescriptor("toString") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(1, parameters.size());

                Value value = parameters.get(0);

                return new StringValue(value.stringValue());
            }

            @Override
            public ValueType returnType(ListIterable<ValueType> paraValueTypes)
            {
                return ValueType.STRING;
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(number)";
            }
        },

        new IntrinsicFunctionDescriptor("toDate") {
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
        },

        new IntrinsicFunctionDescriptor("toLong", Lists.immutable.of("string")) {
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
        },

        new IntrinsicFunctionDescriptor("toDouble", Lists.immutable.of("string")) {
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
        },

        new IntrinsicFunctionDescriptor("withinDays", Lists.immutable.of("date1", "date2", "numberOfDays")) {
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
        }
    );

    private static final MutableMap<String, IntrinsicFunctionDescriptor> FUNCTIONS_BY_NAME =
            FUNCTIONS.toMap(fd -> fd.getNormalizedName(), fd -> fd);

    private BuiltInFunctions()
    {
        // Utility class
    }

    public static void addFunctionDescriptor(IntrinsicFunctionDescriptor fd)
    {
        FUNCTIONS_BY_NAME.put(fd.getNormalizedName(), fd);
    }

    public static IntrinsicFunctionDescriptor getFunctionDescriptor(String name)
    {
        return FUNCTIONS_BY_NAME.get(name);
    }
}
