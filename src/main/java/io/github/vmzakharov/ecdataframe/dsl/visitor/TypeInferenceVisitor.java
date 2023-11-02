package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.AliasExpr;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.AssingExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.BooleanOp;
import io.github.vmzakharov.ecdataframe.dsl.ComparisonOp;
import io.github.vmzakharov.ecdataframe.dsl.ContainsOp;
import io.github.vmzakharov.ecdataframe.dsl.DecimalExpr;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionCallExpr;
import io.github.vmzakharov.ecdataframe.dsl.FunctionScript;
import io.github.vmzakharov.ecdataframe.dsl.IfElseExpr;
import io.github.vmzakharov.ecdataframe.dsl.IndexExpr;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.PropertyPathExpr;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.StatementSequenceScript;
import io.github.vmzakharov.ecdataframe.dsl.UnaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import io.github.vmzakharov.ecdataframe.dsl.VarExpr;
import io.github.vmzakharov.ecdataframe.dsl.VectorExpr;
import io.github.vmzakharov.ecdataframe.dsl.function.BuiltInFunctions;
import io.github.vmzakharov.ecdataframe.dsl.function.IntrinsicFunctionDescriptor;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.stack.MutableStack;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Stacks;
import org.eclipse.collections.impl.tuple.Tuples;

import static io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders.messageFromKey;

public class TypeInferenceVisitor
implements ExpressionVisitor
{
    private final MutableStack<ValueType> expressionTypeStack = Stacks.mutable.of();
    private final MutableMap<String, ValueType> variableTypes = Maps.mutable.of();

    private ValueType lastExpressionType;
    private MutableMap<String, FunctionScript> functions;

    private final EvalContext evalContext;

    private final MutableList<Twin<String>> errors = Lists.mutable.of();

    public TypeInferenceVisitor()
    {
        this(new SimpleEvalContext());
    }

    public TypeInferenceVisitor(EvalContext newEvalContext)
    {
        this.evalContext = newEvalContext;
    }

    public ValueType inferExpressionType(Expression expr)
    {
        expr.accept(this);
        return this.getLastExpressionType();
    }

    @Override
    public void visitAssignExpr(AssingExpr expr)
    {
        this.storeVariableType(expr.getVarName(), this.processAndPopResult(expr.getExpression()));
    }

    public void storeVariableType(String variableName, ValueType valueType)
    {
        this.variableTypes.put(variableName, valueType);
    }

    private FunctionScript getFunction(String functionName)
    {
        return this.functions.get(functionName);
    }

    @Override
    public void visitBinaryExpr(BinaryExpr expr)
    {
        ValueType resultType;

        ValueType type1 = this.processAndPopResult(expr.getOperand1());

        ValueType type2 = this.processAndPopResult(expr.getOperand2());

        resultType = ValueType.VOID;

        if (expr.getOperation() instanceof ArithmeticOp)
        {
            resultType = this.arithmeticTypeCompatibleWith(type1, type2);
        }
        else if (expr.getOperation() instanceof BooleanOp)
        {
            if (type1.isBoolean() && type2.isBoolean())
            {
                resultType = ValueType.BOOLEAN;
            }
        }
        else if (expr.getOperation() instanceof ContainsOp)
        {
            if (type2.isVector() || (type1.isString() && type2.isString()))
            {
                resultType = ValueType.BOOLEAN;
            }
        }
        else if (expr.getOperation() instanceof ComparisonOp)
        {
            if ((type1.isNumber() && type2.isNumber()) || (type1 == type2))
            {
                resultType = ValueType.BOOLEAN;
            }
        }
        else
        {
            this.recordError("Unknown operation " + expr.getOperation(), PrettyPrintVisitor.exprToString(expr));
        }

        // if type1 or type2 is void it means we have already failed in evaluating them expression
        // so no point in propagating this error as we can't meaningfully evaluate expression
        if (resultType.isVoid() && !type1.isVoid() && !type2.isVoid())
        {
            this.recordError(messageFromKey("TYPE_INFER_TYPES_IN_EXPRESSION").toString(), PrettyPrintVisitor.exprToString(expr));
        }

        this.store(resultType);
    }

    /*
    Only the first error is recorded
     */
    private void recordError(String description, String expressionString)
    {
        this.errors.add(Tuples.twin(description, expressionString));
    }

    /**
     * @return true if the visitor encountered incompatible or undefined expression types, false otherwise
     * @deprecated use {@code hasErrors()}
     */
    public boolean isError()
    {
        return this.hasErrors();
    }

    /**
     * @return true if the visitor encountered incompatible or undefined expression types, false otherwise
     */
    public boolean hasErrors()
    {
        return this.getErrors().notEmpty();
    }

    public String getErrorDescription()
    {
        return this.getErrors().get(0).getOne();
    }

    public String getErrorExpressionString()
    {
        return this.getErrors().get(0).getTwo();
    }

    public ListIterable<Twin<String>> getErrors()
    {
        return this.errors;
    }

    private ValueType arithmeticTypeCompatibleWith(ValueType type1, ValueType type2)
    {
        if (type1.equals(type2))
        {
            return type1;
        }

        if (type1.isNumber() && type2.isNumber())
        {
            return (type1.isDouble() || type2.isDouble()) ? ValueType.DOUBLE : ValueType.LONG;
        }

        return ValueType.VOID;
    }

    @Override
    public void visitUnaryExpr(UnaryExpr expr)
    {
        ValueType operandType = this.processAndPopResult(expr.getOperand());
        UnaryOp operation = expr.getOperation();

        if (operation == UnaryOp.MINUS && operandType.isNumber())
        {
            this.store(operandType);
        }
        else if (operation == UnaryOp.NOT && operandType.isBoolean())
        {
            this.store(ValueType.BOOLEAN);
        }
        else if (operation == UnaryOp.IS_NULL || operation == UnaryOp.IS_NOT_NULL)
        {
            this.store(ValueType.BOOLEAN);
        }
        else if ((operation == UnaryOp.IS_EMPTY || operation == UnaryOp.IS_NOT_EMPTY)
                && (operandType.isString() || operandType.isVector()))
        {
            this.store(ValueType.BOOLEAN);
        }
        else
        {
            this.store(ValueType.VOID);
            this.recordError(messageFromKey("TYPE_INFER_TYPES_IN_EXPRESSION").toString(), PrettyPrintVisitor.exprToString(expr));
        }
    }

    private void store(ValueType valueType)
    {
        this.expressionTypeStack.push(valueType);
        this.lastExpressionType = valueType;
    }

    private ValueType processAndPopResult(Expression expr)
    {
        expr.accept(this);
        return this.expressionTypeStack.pop();
    }

    @Override
    public void visitConstExpr(Value expr)
    {
        this.store(expr.getType());
    }

    @Override
    public void visitFunctionCallExpr(FunctionCallExpr expr)
    {
        IntrinsicFunctionDescriptor functionDescriptor = BuiltInFunctions.getFunctionDescriptor(expr.getNormalizedFunctionName());
        if (functionDescriptor != null)
        {
            this.processBuiltInFunction(expr, functionDescriptor);
        }
        else
        {
            FunctionScript functionScript = this.getFunction(expr.getNormalizedFunctionName());
            if (functionScript != null)
            {
                this.processDeclaredFunction(expr, functionScript);
            }
            else
            {
                this.recordError(messageFromKey("TYPE_INFER_UNDEFINED_FUNCTION").toString(), expr.getFunctionName());
            }
        }
    }

    private void processBuiltInFunction(FunctionCallExpr expr, IntrinsicFunctionDescriptor functionDescriptor)
    {
        ListIterable<ValueType> parameterTypes = expr.getParameters().collect(p -> {
            p.accept(this);
            return this.getLastExpressionType();
        });

        this.store(functionDescriptor.returnType(parameterTypes));
    }

    private void processDeclaredFunction(FunctionCallExpr expr, FunctionScript functionScript)
    {
        TypeInferenceVisitor functionCallContextVisitor = new TypeInferenceVisitor();
        expr.getParameters().forEachWithIndex((p, i) -> {
                    String paramName = functionScript.getParameterNames().get(i);
                    p.accept(this);
                    functionCallContextVisitor.storeVariableType(paramName, this.getLastExpressionType());
                }
        );
        functionScript.accept(functionCallContextVisitor);
        this.store(functionCallContextVisitor.getLastExpressionType());
    }

    @Override
    public void visitPropertyPathExpr(PropertyPathExpr expr)
    {
        HierarchicalDataSet dataSet = this.evalContext.getDataSet(expr.getEntityName());

        this.store(dataSet.getFieldType(expr.getPropertyChainString()));
    }

    @Override
    public void visitAnonymousScriptExpr(AnonymousScript expr)
    {
        this.functions = expr.getFunctions();
        expr.getExpressions().forEachWith(Expression::accept, this);
    }

    @Override
    public void visitStatementSequenceScript(StatementSequenceScript expr)
    {
        expr.getExpressions().forEachWith(Expression::accept, this);
    }

    @Override
    public void visitFunctionScriptExpr(FunctionScript expr)
    {
        // function definitions are inherited from the containing script
        expr.getExpressions().forEachWith(Expression::accept, this);
    }

    @Override
    public void visitAliasExpr(AliasExpr expr)
    {
        expr.getExpression().accept(this);
    }

    @Override
    public void visitVarExpr(VarExpr expr)
    {
        String variableName = expr.getVariableName();
        ValueType variableType = this.variableTypes.get(variableName);
        if (variableType == null)
        {
            if (this.evalContext.hasVariable(variableName))
            {
                Value variableValue = this.evalContext.getVariable(variableName);
                variableType = variableValue.getType();
                this.storeVariableType(variableName, variableType);
            }
            else
            {
                variableType = ValueType.VOID;
                this.recordError(messageFromKey("TYPE_INFER_UNDEFINED_VARIABLE").toString(), PrettyPrintVisitor.exprToString(expr));
            }
        }

        this.store(variableType);
    }

    @Override
    public void visitProjectionExpr(ProjectionExpr expr)
    {
        this.store(ValueType.DATA_FRAME);
    }

    @Override
    public void visitVectorExpr(VectorExpr expr)
    {
        // todo: support vectors of types, perhaps?
        this.store(ValueType.VECTOR);
    }

    @Override
    public void visitIndexExpr(IndexExpr expr)
    {
        Expression vectorExpr = expr.getVectorExpr();
        ValueType vectorType = this.processAndPopResult(vectorExpr);

        Expression indexExpr = expr.getIndexExpr();

        if (indexExpr instanceof Value && vectorExpr instanceof VectorExpr) // the vector and the index are both specified as literals
        {
            Value indexVal = (Value) indexExpr;

            if (indexVal.isLong())
            {
                long index = ((LongValue) indexVal).longValue();
                Expression exprAtIndex = ((VectorExpr) vectorExpr).getElements().get((int) index);
                exprAtIndex.accept(this);
                return;
            }
        }

        // the vector and the index are expressions with an unknown value and/or with wrong types

        if (!vectorType.isVector())
        {
            this.recordError(
                    messageFromKey("IDX_EXPR_VECTOR_TYPE_INVALID").with("vectorType", vectorType.toString()).toString(),
                    PrettyPrintVisitor.exprToString(expr));
        }

        ValueType indexType = this.processAndPopResult(indexExpr);
        if (!indexType.isLong())
        {
            // invalid index type
            this.recordError(
                    messageFromKey("IDX_EXPR_INDEX_TYPE_INVALID").with("indexType", indexType.toString()).toString(),
                    PrettyPrintVisitor.exprToString(expr));
        }

        // all types appear valid, but we can't infer the type at the specific index
        this.store(ValueType.VOID);
    }

    @Override
    public void visitDecimalExpr(DecimalExpr expr)
    {
        ValueType unscaledValueType = this.processAndPopResult(expr.unscaledValueExpr());

        if (!unscaledValueType.isLong() && !unscaledValueType.isVoid())
        {
            this.recordError(
                    this.unexpectedTypeMessage(ValueType.LONG, unscaledValueType),
                    PrettyPrintVisitor.exprToString(expr.unscaledValueExpr()));
        }

        ValueType scaleType = this.processAndPopResult(expr.scaleExpr());

        if (!scaleType.isLong() && !scaleType.isVoid())
        {
            this.recordError(
                    this.unexpectedTypeMessage(ValueType.LONG, scaleType),
                    PrettyPrintVisitor.exprToString(expr.scaleExpr()));
        }

        this.store(ValueType.DECIMAL);
    }

    @Override
    public void visitIfElseExpr(IfElseExpr expr)
    {
        ValueType conditionType = this.processAndPopResult(expr.getCondition());

        // void means we have already failed in the condition expression so no point in propagating this error
        if (!conditionType.isBoolean() && !conditionType.isVoid())
        {
            this.recordError(messageFromKey("TYPE_INFER_CONDITION_NOT_BOOLEAN").toString(), PrettyPrintVisitor.exprToString(expr));
        }

        ValueType ifType = this.processAndPopResult(expr.getIfScript());

        if (expr.hasElseSection())
        {
            ValueType elseType = this.processAndPopResult(expr.getElseScript());

            ValueType valueType = this.arithmeticTypeCompatibleWith(ifType, elseType);
            this.store(valueType);
            if (valueType.isVoid())
            {
                this.recordError(messageFromKey("TYPE_INFER_ELSE_INCOMPATIBLE").toString(), PrettyPrintVisitor.exprToString(expr));
            }
        }
        else
        {
            this.store(ifType);
        }
    }

    public ValueType getLastExpressionType()
    {
        return this.lastExpressionType;
    }

    private String unexpectedTypeMessage(ValueType expected, ValueType actual)
    {
        return messageFromKey("TYPE_INFER_UNEXPECTED_TYPE")
                .with("expectedType", expected.toString())
                .with("actualType", actual.toString())
                .toString();
    }
}
