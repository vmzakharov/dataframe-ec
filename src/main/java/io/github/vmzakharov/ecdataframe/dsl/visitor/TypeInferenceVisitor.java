package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dataset.AvroDataSet;
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
    private final MutableStack<ValueType> expressionTypeStack = Stacks.mutable.of();
    private final MutableMap<String, ValueType> variableTypes = Maps.mutable.of();

    private ValueType lastExpressionType;
    private MutableMap<String, FunctionScript> functions;

    private final EvalContext evalContext;

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

            if (type1.isString() && type2.isString())
            {
                resultType = ValueType.STRING;
            }
            else if (type1.isNumber() && type2.isNumber())
            {
                if (type1.isDouble() || type2.isDouble())
                {
                    resultType = ValueType.DOUBLE;
                }
                else
                {
                    resultType = ValueType.LONG;
                }
            }
            else
            {
                throw new RuntimeException("Incompatible operand types in expression " + PrettyPrintVisitor.exprToString(expr));
            }
        }
        else
        {
            resultType = ValueType.BOOLEAN;
        }

        this.store(resultType);
    }

    @Override
    public void visitUnaryExpr(UnaryExpr expr)
    {
        expr.getOperand().accept(this);
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
        AvroDataSet dataSet = this.evalContext.getDataSet(expr.getEntityName());

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
        // todo: implement
        this.store(ValueType.VOID);
    }

    public ValueType getLastExpressionType()
    {
        return this.lastExpressionType;
    }
}
