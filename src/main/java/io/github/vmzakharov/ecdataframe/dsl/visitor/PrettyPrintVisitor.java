package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dsl.AliasExpr;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.AssingExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionCallExpr;
import io.github.vmzakharov.ecdataframe.dsl.FunctionScript;
import io.github.vmzakharov.ecdataframe.dsl.IfElseExpr;
import io.github.vmzakharov.ecdataframe.dsl.IndexExpr;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.PropertyPathExpr;
import io.github.vmzakharov.ecdataframe.dsl.StatementSequenceScript;
import io.github.vmzakharov.ecdataframe.dsl.UnaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.VarExpr;
import io.github.vmzakharov.ecdataframe.dsl.VectorExpr;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.Printer;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.utility.StringIterate;

public class PrettyPrintVisitor
implements ExpressionVisitor
{
    final private Printer printer;

    private int offset = 0;
    private boolean startedNewLine = true;

    public PrettyPrintVisitor()
    {
        this(PrinterFactory.getPrinter());
    }

    public PrettyPrintVisitor(Printer newPrinter)
    {
        this.printer = newPrinter;
    }

    static public String exprToString(Expression e)
    {
        CollectingPrinter printer = new CollectingPrinter();
        e.accept(new PrettyPrintVisitor(printer));
        return printer.toString();
    }

    @Override
    public void visitAssignExpr(AssingExpr expr)
    {
        this.print(this.escapeVarNameIfNeeded(expr.getVarName(), expr.isEscaped()))
            .print(" = ")
            .printExpression(expr.getExpression());
    }

    private String escapeVarNameIfNeeded(String name, boolean escaped)
    {
        return escaped ? "${" + name +"}" : name;
    }

    @Override
    public void visitBinaryExpr(BinaryExpr expr)
    {
        this.print("(").printExpression(expr.getOperand1()).print(" ")
            .print(expr.getOperation().asString())
            .print(" ").printExpression(expr.getOperand2()).print(")");
    }

    @Override
    public void visitUnaryExpr(UnaryExpr expr)
    {
        if (expr.getOperation().isPrefix())
        {
            this.print(expr.getOperation().asString()).print("(").printExpression(expr.getOperand()).print(")");
        }
        else
        {
            this.print("(").printExpression(expr.getOperand()).print(") ").print(expr.getOperation().asString());
        }
    }

    @Override
    public void visitConstExpr(Value expr)
    {
        this.print(expr.asStringLiteral());
    }

    @Override
    public void visitFunctionCallExpr(FunctionCallExpr expr)
    {
        this.print(expr.getFunctionName()).print("(").printExpressionList(expr.getParameters()).print(")");
    }

    private PrettyPrintVisitor printExpressionListLn(ListIterable<Expression> expressions)
    {
        int count = expressions.size();
        for (int i = 0; i < count; i++)
        {
            expressions.get(i).accept(this);
            this.newLine();
        }

        return this;
    }

    private PrettyPrintVisitor printExpressionList(ListIterable<Expression> expressions, String separator)
    {
        int count = expressions.size();
        for (int i = 0; i < count; i++)
        {
            expressions.get(i).accept(this);
            if (i < count - 1)
            {
                this.print(separator);
            }
        }

        return this;
    }

    private PrettyPrintVisitor printExpressionList(ListIterable<Expression> expressions)
    {
        return this.printExpressionList(expressions, ", ");
    }

    private PrettyPrintVisitor printExpression(Expression expression)
    {
        expression.accept(this);
        return this;
    }

    @Override
    public void visitPropertyPathExpr(PropertyPathExpr expr)
    {
        this.print(expr.getPathElements().makeString("."));
    }

    @Override
    public void visitAnonymousScriptExpr(AnonymousScript expr)
    {
        expr.getFunctions().forEachValue(this::visitFunctionScriptExpr);
        this.printExpressionListLn(expr.getExpressions());
    }

    @Override
    public void visitFunctionScriptExpr(FunctionScript expr)
    {
        this.print("function ").print(expr.getName());
        if (expr.getExpressions().notEmpty())
        {
            this.print("(").print(expr.getParameterNames().makeString()).print(")");
        }

        this.newLine()
            .print("{").newLine().tab()
            .printExpressionListLn(expr.getExpressions())
            .tabBack()
            .print("}").newLine();
    }

    @Override
    public void visitStatementSequenceScript(StatementSequenceScript expr)
    {
        this.printExpressionListLn(expr.getExpressions());
    }

    @Override
    public void visitVarExpr(VarExpr expr)
    {
          this.print(this.escapeVarNameIfNeeded(expr.getVariableName(), expr.isEscaped()));
    }

    @Override
    public void visitProjectionExpr(ProjectionExpr expr)
    {
        this.print("project {")
            .printExpressionList(expr.getProjectionElements())
            .print("}");

        if (expr.getWhereClause() != null)
        {
            this.print(" where ").printExpression(expr.getWhereClause());
        }
    }

    @Override
    public void visitAliasExpr(AliasExpr expr)
    {
        this.print(expr.getAlias()).print(" : ").printExpression(expr.getExpression());
    }

    @Override
    public void visitVectorExpr(VectorExpr expr)
    {
        this.print("(").printExpressionList(expr.getElements()).print(")");
    }

    @Override
    public void visitIndexExpr(IndexExpr expr)
    {
        this.printExpression(expr.getVectorExpr()).print("[").printExpression(expr.getIndexExpr()).print("]");
    }

    @Override
    public void visitIfElseExpr(IfElseExpr expr)
    {
        if (expr.isTernary())
        {
            this
                .printExpression(expr.getCondition())
                .print(" ? ")
                .printExpression(expr.getIfScript())
                .print(" : ")
                .printExpression(expr.getElseScript());
        }
        else
        {
            this.print("if ").printExpression(expr.getCondition()).print(" then").newLine();
            this.tab();
            expr.getIfScript().accept(this);
            this.tabBack();

            if (expr.hasElseSection())
            {
                this.print("else").newLine();
                this.tab();
                expr.getElseScript().accept(this);
                this.tabBack();
            }

            this.print("endif");
        }
    }

    private PrettyPrintVisitor print(String s)
    {
        if (this.startedNewLine)
        {
            this.printer.print(StringIterate.repeat(' ', this.offset * 2));
            this.startedNewLine = false;
        }
        this.printer.print(s);
        return this;
    }

    private PrettyPrintVisitor tab()
    {
        this.offset++;
        return this;
    }

    private PrettyPrintVisitor tabBack()
    {
        this.offset--;
        return this;
    }

    private PrettyPrintVisitor newLine()
    {
        this.printer.newLine();
        this.startedNewLine = true;
        return this;
    }
}
