package org.modelscript.grammar;

import org.antlr.v4.runtime.Token;
import org.eclipse.collections.api.factory.Stacks;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.stack.MutableStack;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;
import org.modelscript.expr.*;
import org.modelscript.expr.value.DoubleValue;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.StringValue;
import org.modelscript.grammar.ModelScriptParser.*;

import static org.modelscript.grammar.ModelScriptParser.*;

public class ModelScriptTreeBuilderVisitor
extends ModelScriptBaseVisitor<Expression>
{
    private final MutableStack<Script> scriptStack = Stacks.mutable.of();

    public ModelScriptTreeBuilderVisitor()
    {
        this.scriptStack.push(new AnonymousScript());
    }

    @Override
    public Expression visitScript(ScriptContext ctx)
    {
        // todo: need it?
        return super.visitScript(ctx);
    }

    @Override
    public Expression visitFreeExp(FreeExpContext ctx)
    {
        Expression freeExpression = this.visit(ctx.expr());
        return this.addStatementToCurrentScriptContext(freeExpression);
    }

    @Override
    public Expression visitAssignExpr(AssignExprContext ctx)
    {
        String varName = ctx.ID().getText();
        if (ctx.expr() == null)
        {
            throw new RuntimeException("Malformed assignment statement");
        }
        Expression expression = this.visit(ctx.expr());
        return this.addStatementToCurrentScriptContext(new AssingExpr(varName, expression));
    }

    private Expression addStatementToCurrentScriptContext(Expression expression)
    {
        return this.getScript().addStatement(expression);
    }

    @Override
    public Expression visitPropertyPathExpr(PropertyPathExprContext ctx)
    {
        return new PropertyPathExpr(ListIterate.collect(ctx.ID(), Object::toString));
    }

    @Override
    public Expression visitAliasExpr(AliasExprContext ctx)
    {
        return new AliasExpr(ctx.ID().toString(), this.visit(ctx.expr()));
    }

    @Override
    public Expression visitStringLiteralExpr(StringLiteralExprContext ctx)
    {
        return new ConstExpr(new StringValue(this.stripQuotes(ctx.STRING().getText())));
    }

    @Override
    public Expression visitVarExpr(VarExprContext ctx)
    {
        return new VarExpr(ctx.ID().getText());
    }

    @Override
    public Expression visitIntLiteralExpr(IntLiteralExprContext ctx)
    {
        return new ConstExpr(new LongValue(Integer.parseInt(ctx.INT().getText())));
    }

    @Override
    public Expression visitDoubleLiteralExpr(DoubleLiteralExprContext ctx)
    {
        return new ConstExpr(new DoubleValue(Double.parseDouble(ctx.DOUBLE().getText())));
    }

    @Override
    public Expression visitAddSubExpr(AddSubExprContext ctx)
    {
        return  this.visitBinaryOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    @Override
    public Expression visitMulDivExpr(MulDivExprContext ctx)
    {
        return  this.visitBinaryOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    private Expression visitBinaryOperation(ExprContext exprContext1, ExprContext exprContext2, Token opToken)
    {
        ArithmeticOp operation = null;
        switch (opToken.getType())
        {
            case MUL: operation = ArithmeticOp.MULTIPLY; break;
            case DIV: operation = ArithmeticOp.DIVIDE;   break;
            case ADD: operation = ArithmeticOp.ADD;      break;
            case SUB: operation = ArithmeticOp.SUBTRACT; break;
        }

        return new BinaryExpr(this.visit(exprContext1), this.visit(exprContext2), operation);
    }

    @Override
    public Expression visitAndExpr(AndExprContext ctx)
    {
        return this.visitBooleanOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    @Override
    public Expression visitOrExpr(OrExprContext ctx)
    {
        return this.visitBooleanOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    private Expression visitBooleanOperation(ExprContext exprContext1, ExprContext exprContext2, Token opToken)
    {
        BooleanOp operation = null;
        switch (opToken.getType())
        {
            case AND:
                operation = BooleanOp.AND;
                break;
            case OR:
                operation = BooleanOp.OR;
                break;
            case XOR:
                operation = BooleanOp.XOR;
                break;
        }

        return new BinaryExpr(this.visit(exprContext1), this.visit(exprContext2), operation);
    }

    @Override
    public Expression visitCompareExpr(ModelScriptParser.CompareExprContext ctx)
    {
        ComparisonOp operation = null;
        switch (ctx.op.getType())
        {
            case GT : operation = ComparisonOp.GT ; break;
            case GTE: operation = ComparisonOp.GTE; break;
            case LT : operation = ComparisonOp.LT ; break;
            case LTE: operation = ComparisonOp.LTE; break;
            case EQ : operation = ComparisonOp.EQ ; break;
            case NE : operation = ComparisonOp.NE ; break;
        }

        return new BinaryExpr(this.visit(ctx.expr(0)), this.visit(ctx.expr(1)), operation);
    }

    @Override
    public Expression visitParenExpr(ParenExprContext ctx)
    {
        return super.visit(ctx.expr());
    }

    @Override
    public Expression visitFunctionDeclarationExpr(FunctionDeclarationExprContext ctx)
    {
        String functionName = ctx.ID().toString();

        MutableList<String> parameterNames;
        if (ctx.idList() == null)
        {
            parameterNames = Lists.mutable.of();
        }
        else
        {
            parameterNames = ListIterate.collect(ctx.idList().ID(), Object::toString);
        }

        FunctionScript functionScript = new FunctionScript(functionName, parameterNames);
        this.pushScript(functionScript);
        ListIterate.forEach(ctx.statementSequence().statement(), this::visit);
        this.popScript();

        this.getAsAnonymousScript().addFunctionScript(functionScript);

        return null;
    }

    @Override
    public Expression visitInExpr(InExprContext ctx)
    {
        return new BinaryExpr(
                this.visit(ctx.expr()),
                this.visit(ctx.vectorExpr()),
                ContainsOp.IN);
    }

    @Override
    public Expression visitVectorExpr(VectorExprContext ctx)
    {
        ListIterable<Expression> elements;
        if (ctx.exprList() == null)
        {
            elements = Lists.immutable.of();
        }
        else
        {
            elements = ListIterate.collect(ctx.exprList().expr(), this::visit);
        }
        return new VectorExpr(elements);
    }

    @Override
    public Expression visitFunctionCallExpr(FunctionCallExprContext ctx)
    {
        String functionName = ctx.ID().toString();

        ListIterable<Expression> parameters;
        if (ctx.exprList() == null)
        {
            parameters = Lists.immutable.of();
        }
        else
        {
            parameters = ListIterate.collect(ctx.exprList().expr(), this::visit);
        }
        return new FunctionCallExpr(functionName, parameters);
    }

    @Override
    public Expression visitConditionExpr(ConditionExprContext ctx)
    {
        Expression condition = this.visit(ctx.expr());

        Script ifScript = this.processStatementSequence(ctx.ifBody);

        if (ctx.elseBody == null)
        {
            this.addStatementToCurrentScriptContext(new IfElseExpr(condition, ifScript));
        }
        else
        {
            Script elseScript = this.processStatementSequence(ctx.elseBody);

            this.addStatementToCurrentScriptContext(new IfElseExpr(condition, ifScript, elseScript));
        }

        return null;
    }

    private Script processStatementSequence(StatementSequenceContext ctx)
    {
        this.pushScript(new StatementSequenceScript());
        ListIterate.forEach(ctx.statement(), this::visit);
        return this.popScript();
    }

    @Override
    public Expression visitProjectionStatement(ProjectionStatementContext ctx)
    {
        ListIterable<Expression> projectionList = ListIterate.collect(ctx.exprList().expr(), this::visit);

        ProjectionExpr projectionExpr = (ctx.expr() == null) ?
                new ProjectionExpr(projectionList) :
                new ProjectionExpr(projectionList, this.visit(ctx.expr()));

        return this.addStatementToCurrentScriptContext(projectionExpr);
    }

    private String stripQuotes(String aString)
    {
        if (aString.length() < 2) return aString;

        if (aString.charAt(0) == '"' && aString.charAt(aString.length()-1) == '"')
        {
            return aString.substring(1, aString.length()-1);
        }

        return aString;
    }

    public Script getScript()
    {
        return this.scriptStack.peek();
    }

    public AnonymousScript getAsAnonymousScript()
    {
        return (AnonymousScript) this.scriptStack.peek();
    }

    private Script pushScript(Script newScript)
    {
        this.scriptStack.push(newScript);
        return newScript;
    }

    private Script popScript()
    {
        return this.scriptStack.pop();
    }
}
