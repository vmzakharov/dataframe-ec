package io.github.vmzakharov.ecdataframe.grammar;

import io.github.vmzakharov.ecdataframe.dsl.AliasExpr;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.AssignExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.BooleanOp;
import io.github.vmzakharov.ecdataframe.dsl.ComparisonOp;
import io.github.vmzakharov.ecdataframe.dsl.ContainsOp;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionCallExpr;
import io.github.vmzakharov.ecdataframe.dsl.FunctionScript;
import io.github.vmzakharov.ecdataframe.dsl.IfElseExpr;
import io.github.vmzakharov.ecdataframe.dsl.IndexExpr;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.PropertyPathExpr;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.StatementSequenceScript;
import io.github.vmzakharov.ecdataframe.dsl.UnaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import io.github.vmzakharov.ecdataframe.dsl.VarExpr;
import io.github.vmzakharov.ecdataframe.dsl.VectorExpr;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import org.antlr.v4.runtime.Token;
import org.eclipse.collections.api.factory.Stacks;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.stack.MutableStack;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.utility.ListIterate;

public class ModelScriptTreeBuilderVisitor
extends ModelScriptBaseVisitor<Expression>
{
    private final MutableStack<Script> scriptStack = Stacks.mutable.of();

    public ModelScriptTreeBuilderVisitor()
    {
        this.scriptStack.push(new AnonymousScript());
    }

    @Override
    public Expression visitFreeExp(ModelScriptParser.FreeExpContext ctx)
    {
        Expression freeExpression = this.visit(ctx.expr());
        return this.addStatementToCurrentScriptContext(freeExpression);
    }

    @Override
    public Expression visitAssignExpr(ModelScriptParser.AssignExprContext ctx)
    {
        String varName = ctx.ID().getText();
        if (ctx.expr() == null)
        {
            throw new RuntimeException("Malformed assignment statement");
        }
        Expression expression = this.visit(ctx.expr());
        return this.addStatementToCurrentScriptContext(
                new AssignExpr(this.unEscape(varName), this.isEscaped(varName), expression));
    }

    private String unEscape(String varName)
    {
        if (this.isEscaped(varName))
        {
            return varName.substring(2, varName.length() - 1);
        }

        return varName;
    }

    private boolean isEscaped(String varName)
    {
        return varName.startsWith("${") && varName.endsWith("}");
    }

    private Expression addStatementToCurrentScriptContext(Expression expression)
    {
        return this.getScript().addExpression(expression);
    }

    @Override
    public Expression visitPropertyPathExpr(ModelScriptParser.PropertyPathExprContext ctx)
    {
        return new PropertyPathExpr(ListIterate.collect(ctx.ID(), Object::toString));
    }

    @Override
    public Expression visitAliasExpr(ModelScriptParser.AliasExprContext ctx)
    {
        return new AliasExpr(this.unEscape(ctx.ID().toString()), this.visit(ctx.expr()));
    }

    @Override
    public Expression visitStringLiteralExpr(ModelScriptParser.StringLiteralExprContext ctx)
    {
        return new StringValue(this.stripQuotes(ctx.STRING().getText()));
    }

    @Override
    public Expression visitVarExpr(ModelScriptParser.VarExprContext ctx)
    {
        String varName = ctx.ID().getText();

        return new VarExpr(this.unEscape(varName), this.isEscaped(varName));
    }

    @Override
    public Expression visitIntLiteralExpr(ModelScriptParser.IntLiteralExprContext ctx)
    {
        return new LongValue(Long.parseLong(ctx.INT().getText()));
    }

    @Override
    public Expression visitDoubleLiteralExpr(ModelScriptParser.DoubleLiteralExprContext ctx)
    {
        return new DoubleValue(Double.parseDouble(ctx.DOUBLE().getText()));
    }

    @Override
    public Expression visitAddSubExpr(ModelScriptParser.AddSubExprContext ctx)
    {
        return this.visitBinaryOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    @Override
    public Expression visitMulDivExpr(ModelScriptParser.MulDivExprContext ctx)
    {
        return this.visitBinaryOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    private Expression visitBinaryOperation(ModelScriptParser.ExprContext exprContext1, ModelScriptParser.ExprContext exprContext2, Token opToken)
    {
        ArithmeticOp operation = switch (opToken.getType())
        {
            case ModelScriptParser.MUL -> ArithmeticOp.MULTIPLY;
            case ModelScriptParser.DIV -> ArithmeticOp.DIVIDE;
            case ModelScriptParser.ADD -> ArithmeticOp.ADD;
            case ModelScriptParser.SUB -> ArithmeticOp.SUBTRACT;
            default -> throw new RuntimeException("Do not understand token '" + opToken.getText() + "'");
        };

        return new BinaryExpr(this.visit(exprContext1), this.visit(exprContext2), operation);
    }

    @Override
    public Expression visitAndExpr(ModelScriptParser.AndExprContext ctx)
    {
        return this.visitBooleanOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    @Override
    public Expression visitOrExpr(ModelScriptParser.OrExprContext ctx)
    {
        return this.visitBooleanOperation(ctx.expr(0), ctx.expr(1), ctx.op);
    }

    private Expression visitBooleanOperation(ModelScriptParser.ExprContext exprContext1, ModelScriptParser.ExprContext exprContext2, Token opToken)
    {
        BooleanOp operation = switch (opToken.getType())
        {
            case ModelScriptParser.AND -> BooleanOp.AND;
            case ModelScriptParser.OR -> BooleanOp.OR;
            case ModelScriptParser.XOR -> BooleanOp.XOR;
            default -> throw new RuntimeException("Do not understand token '" + opToken.getText() + "'");
        };

        return new BinaryExpr(this.visit(exprContext1), this.visit(exprContext2), operation);
    }

    @Override
    public Expression visitCompareExpr(ModelScriptParser.CompareExprContext ctx)
    {
        ComparisonOp operation = switch (ctx.op.getType())
        {
            case ModelScriptParser.GT -> ComparisonOp.GT;
            case ModelScriptParser.GTE -> ComparisonOp.GTE;
            case ModelScriptParser.LT -> ComparisonOp.LT;
            case ModelScriptParser.LTE -> ComparisonOp.LTE;
            case ModelScriptParser.EQ -> ComparisonOp.EQ;
            case ModelScriptParser.NE -> ComparisonOp.NE;
            default -> throw new RuntimeException("Do not understand token '" + ctx.op.getText() + "'");
        };

        return new BinaryExpr(this.visit(ctx.expr(0)), this.visit(ctx.expr(1)), operation);
    }

    @Override
    public Expression visitParenExpr(ModelScriptParser.ParenExprContext ctx)
    {
        return super.visit(ctx.expr());
    }

    @Override
    public Expression visitFunctionDeclarationExpr(ModelScriptParser.FunctionDeclarationExprContext ctx)
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
    public Expression visitUnaryMinusExpr(ModelScriptParser.UnaryMinusExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.MINUS, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitNotExpr(ModelScriptParser.NotExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.NOT, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitInExpr(ModelScriptParser.InExprContext ctx)
    {
        return new BinaryExpr(
                this.visit(ctx.expr(0)),
                this.visit(ctx.expr(1)),
                ctx.IN() == null ? ContainsOp.NOT_IN : ContainsOp.IN);
    }

    @Override
    public Expression visitIsEmptyExpr(ModelScriptParser.IsEmptyExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.IS_EMPTY, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitIsNotEmptyExpr(ModelScriptParser.IsNotEmptyExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.IS_NOT_EMPTY, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitIsNullExpr(ModelScriptParser.IsNullExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.IS_NULL, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitIsNotNullExpr(ModelScriptParser.IsNotNullExprContext ctx)
    {
        return new UnaryExpr(UnaryOp.IS_NOT_NULL, this.visit(ctx.expr()));
    }

    @Override
    public Expression visitVectorExpr(ModelScriptParser.VectorExprContext ctx)
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
    public Expression visitIndexVectorExpr(ModelScriptParser.IndexVectorExprContext ctx)
    {
        return new IndexExpr(this.visit(ctx.expr(0)), this.visit(ctx.expr(1)));
    }

    @Override
    public Expression visitFunctionCallExpr(ModelScriptParser.FunctionCallExprContext ctx)
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
    public Expression visitTernaryExpr(ModelScriptParser.TernaryExprContext ctx)
    {
        return new IfElseExpr(this.visit(ctx.condExpr), this.visit(ctx.ifExpr), this.visit(ctx.elseExpr), true);
    }

    @Override
    public Expression visitConditionExpr(ModelScriptParser.ConditionExprContext ctx)
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

            this.addStatementToCurrentScriptContext(new IfElseExpr(condition, ifScript, elseScript, false));
        }

        return null;
    }

    private Script processStatementSequence(ModelScriptParser.StatementSequenceContext ctx)
    {
        this.pushScript(new StatementSequenceScript());
        ListIterate.forEach(ctx.statement(), this::visit);
        return this.popScript();
    }

    @Override
    public Expression visitProjectionStatement(ModelScriptParser.ProjectionStatementContext ctx)
    {
        ListIterable<Expression> projectionList = ListIterate.collect(ctx.exprList().expr(), this::visit);

        ProjectionExpr projectionExpr = (ctx.expr() == null)
                ?
                new ProjectionExpr(projectionList)
                :
                new ProjectionExpr(projectionList, this.visit(ctx.expr()));

        return this.addStatementToCurrentScriptContext(projectionExpr);
    }

    private String stripQuotes(String aString)
    {
        if (aString.length() < 2)
        {
            return aString;
        }

        if (aString.charAt(0) == '"' && aString.charAt(aString.length() - 1) == '"')
        {
            return aString.substring(1, aString.length() - 1);
        }

        if (aString.charAt(0) == '\'' && aString.charAt(aString.length() - 1) == '\'')
        {
            return aString.substring(1, aString.length() - 1);
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
