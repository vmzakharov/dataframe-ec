package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.*;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.stack.MutableStack;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Stacks;

public class TypeInferenceVisitor
implements ExpressionVisitor
{
    public static final String ERR_TXT_IF_ELSE = "Incompatible types in branches of if-else";
    public static final String ERR_TXT_TYPES_IN_EXPRESSION = "Incompatible operand types in expression";

    private final MutableStack<ValueType> expressionTypeStack = Stacks.mutable.of();
    private final MutableMap<String, ValueType> variableTypes = Maps.mutable.of();

    private ValueType lastExpressionType;
    private MutableMap<String, FunctionScript> functions;

    private final EvalContext evalContext;

    private boolean error;
    private String errorDescription = "";
    private String errorExpressionString = "";

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
        expr.getExpression().accept(this);
        this.storeVariableType(expr.getVarName(), this.expressionTypeStack.pop());
    }

    private void storeVariableType(String variableName, ValueType valueType)
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

        if (expr.getOperation() instanceof ArithmeticOp)
        {
            expr.getOperand1().accept(this);
            expr.getOperand2().accept(this);
            ValueType type1 = this.expressionTypeStack.pop();
            ValueType type2 = this.expressionTypeStack.pop();

            resultType = this.typeCompatibleWith(type1, type2);
            if (resultType.isVoid())
            {
                this.recordError(ERR_TXT_TYPES_IN_EXPRESSION, PrettyPrintVisitor.exprToString(expr));
            }
        }
        else
        {
            resultType = ValueType.BOOLEAN;
        }

        this.store(resultType);
    }

    private void recordError(String description, String expressionString)
    {
        this.error = true;
        this.errorDescription = description;
        this.errorExpressionString = expressionString;
    }

    public boolean isError()
    {
        return this.error;
    }

    public String getErrorDescription()
    {
        return this.errorDescription;
    }

    public String getErrorExpressionString()
    {
        return this.errorExpressionString;
    }

    private ValueType typeCompatibleWith(ValueType type1, ValueType type2)
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
        expr.getOperand().accept(this);
        ValueType operandType = this.expressionTypeStack.pop();
        UnaryOp operation = expr.getOperation();

        if (operation == UnaryOp.MINUS)
        {
            this.store(operandType);
        }
        else
        {
            this.store(ValueType.BOOLEAN);
        }
    }

    private void store(ValueType valueType)
    {
        this.expressionTypeStack.push(valueType);
        this.lastExpressionType = valueType;
    }

    @Override
    public void visitConstExpr(ConstExpr expr)
    {
        this.store(expr.getValue().getType());
    }

    @Override
    public void visitFunctionCallExpr(FunctionCallExpr expr)
    {
        FunctionScript functionScript = this.getFunction(expr.getNormalizedFunctionName());

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
            Value variableValue = this.evalContext.getVariable(variableName);
            if (variableValue == null)
            {
                throw new RuntimeException("Undefined variable '" + variableName + "'");
            }
            else
            {
                variableType = variableValue.getType();
                this.storeVariableType(variableName, variableType);
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
        // see the todo above
        this.store(ValueType.VOID);
    }

    @Override
    public void visitIfElseExpr(IfElseExpr expr)
    {
        expr.getIfScript().accept(this);
        ValueType ifType = this.expressionTypeStack.pop();

        if (expr.hasElseSection())
        {
            expr.getElseScript().accept(this);
            ValueType elseType = this.expressionTypeStack.pop();

            ValueType valueType = this.typeCompatibleWith(ifType, elseType);
            this.store(valueType);
            if (valueType.isVoid())
            {
                this.recordError(ERR_TXT_IF_ELSE, PrettyPrintVisitor.exprToString(expr));
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
}
