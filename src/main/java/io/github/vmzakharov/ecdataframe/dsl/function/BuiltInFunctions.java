package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.*;
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
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().forEach(e -> printer.print(e.stringValue()));
                return Value.VOID;
            }
        },

        new IntrinsicFunctionDescriptor("println") {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);
                Printer printer = PrinterFactory.getPrinter();
                parameters.getElements().forEach(e -> printer.print(e.stringValue()));
                printer.newLine();
                return Value.VOID;
            }
        },

            new IntrinsicFunctionDescriptor("startsWith") {
                @Override
                public Value evaluate(EvalContext context)
                {
                    VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);

                    int parameterCount = parameters.size();
                    if (parameterCount != 2)
                    {
                        throw new RuntimeException("Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
                    }

                    String aString = parameters.get(0).stringValue();
                    String aPrefix = parameters.get(1).stringValue();

                    return BooleanValue.valueOf(aString.startsWith(aPrefix));
                }

                @Override
                public String usageString()
                {
                    return "Usage: " + this.getName() + "(aString, aPrefix)";
                }
            },

        new IntrinsicFunctionDescriptor("substr") {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);

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
                return "Usage: " + this.getName() + "(aString, beginIndex[, endIndex])";
            }
        },

            new IntrinsicFunctionDescriptor("abs") {
                @Override
                public Value evaluate(EvalContext context)
                {
                    VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);

                    if (parameters.size() != 1)
                    {
                        throw new RuntimeException("Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
                    }

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

            new IntrinsicFunctionDescriptor("toDate") {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);

                if (parameters.size() != 1)
                {
                    throw new RuntimeException("Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
                }

                String aString = parameters.get(0).stringValue();

                return new DateValue(LocalDate.parse(aString, DateTimeFormatter.ISO_DATE));
            }

            @Override
            public String usageString()
            {
                return "Usage: " + this.getName() + "(yyyy-mm-dd)";
            }
        },

        new IntrinsicFunctionDescriptor("withinDays") {
            @Override
            public Value evaluate(EvalContext context)
            {
                VectorValue parameters = (VectorValue) context.getVariableOrDefault(magicalParameterName(), VectorValue.EMPTY);

                if (parameters.size() != 3)
                {
                    throw new RuntimeException("Invalid number of parameters in a call to '" + this.getName() + "'. " + this.usageString());
                }

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
            FUNCTIONS.toMap(fd -> fd.getName(), fd -> fd).toImmutable();

    public static IntrinsicFunctionDescriptor getFunctionDescriptor(String name)
    {
        return FUNCTIONS_BY_NAME.get(name);
    }
}
