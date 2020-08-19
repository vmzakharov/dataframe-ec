package org.modelscript.expr.function;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.modelscript.expr.EvalContext;
import org.modelscript.expr.value.*;
import org.modelscript.util.Printer;
import org.modelscript.util.PrinterFactory;

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
        }
    );

    private static final ImmutableMap<String, IntrinsicFunctionDescriptor> FUNCTIONS_BY_NAME =
            FUNCTIONS.toMap(fd -> fd.getName(), fd -> fd).toImmutable();

    public static IntrinsicFunctionDescriptor getFunctionDescriptor(String name)
    {
        return FUNCTIONS_BY_NAME.get(name);
    }
}
