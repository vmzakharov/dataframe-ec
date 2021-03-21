package io.github.vmzakharov.ecdataframe.dsl.function;

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
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Lists;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class BuiltInFunctions
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

        new IntrinsicFunctionDescriptor("startsWith") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(parameters.size(), 2);

                String aString = parameters.get(0).stringValue();
                String aPrefix = parameters.get(1).stringValue();

                return BooleanValue.valueOf(aString.startsWith(aPrefix));
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(string, prefix)";
            }
        },

        new IntrinsicFunctionDescriptor("contains") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(parameters.size(), 2);

                String aString = parameters.get(0).stringValue();
                String substring = parameters.get(1).stringValue();

                return BooleanValue.valueOf(aString.contains(substring));
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(string, substring)";
            }
        },

        new IntrinsicFunctionDescriptor("toUpper") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(parameters.size(), 1);

                return new StringValue(parameters.get(0).stringValue().toUpperCase());
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(string)";
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

                String substring = (parameterCount == 2) ?
                    aString.substring(beginIndex) :
                    aString.substring(beginIndex, (int) ((LongValue) parameters.get(2)).longValue());

                return new StringValue(substring);
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
            public String usageString()
            {
                return "Usage: " + this.getName() + "(yyyy-mm-dd)";
            }
        },

            new IntrinsicFunctionDescriptor("toLong") {
                @Override
                public Value evaluate(VectorValue parameters)
                {
                    this.assertParameterCount(1, parameters.size());
                    Value parameter = parameters.get(0);
                    this.assertParameterType(ValueType.STRING, parameter.getType());
                    String aString = parameter.stringValue();

                    return new LongValue(Long.parseLong(aString));
                }

                @Override
                public String usageString()
                {
                    return "Usage: " + this.getName() + "(string)";
                }
            },

            new IntrinsicFunctionDescriptor("toDouble") {
                @Override
                public Value evaluate(VectorValue parameters)
                {
                    this.assertParameterCount(1, parameters.size());
                    Value parameter = parameters.get(0);
                    this.assertParameterType(ValueType.STRING, parameter.getType());
                    String aString = parameter.stringValue();

                    return new DoubleValue(Double.parseDouble(aString));
                }

                @Override
                public String usageString()
                {
                    return "Usage: " + this.getName() + "(string)";
                }
            },

            new IntrinsicFunctionDescriptor("withinDays") {
            @Override
            public Value evaluate(VectorValue parameters)
            {
                this.assertParameterCount(3, parameters.size());

                LocalDate date1 = ((DateValue) parameters.get(0)).dateValue();
                LocalDate date2 = ((DateValue) parameters.get(1)).dateValue();
                long numberOfDays = ((LongValue) parameters.get(2)).longValue();

                Period period = Period.between(date1, date2);

                return BooleanValue.valueOf(period.getDays() <= numberOfDays);
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(date1, date2, numberOfDays)";
            }
        }
    );

    private static final ImmutableMap<String, IntrinsicFunctionDescriptor> FUNCTIONS_BY_NAME =
            FUNCTIONS.toMap(fd -> fd.getNormalizedName(), fd -> fd).toImmutable();

    public static IntrinsicFunctionDescriptor getFunctionDescriptor(String name)
    {
        return FUNCTIONS_BY_NAME.get(name);
    }
}
