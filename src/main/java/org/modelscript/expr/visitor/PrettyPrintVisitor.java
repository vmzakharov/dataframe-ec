package org.modelscript.expr.visitor;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.utility.StringIterate;
import org.modelscript.expr.*;
import org.modelscript.util.CollectingPrinter;
import org.modelscript.util.Printer;
import org.modelscript.util.PrinterFactory;

public class PrettyPrintVisitor
implements ExpressionVisitor
{
    final private Printer printer;

    private int offset = 0;
    private boolean startedNewLine = true;

    static public String exprToString(Expression e)
    {
        CollectingPrinter printer = new CollectingPrinter();
        e.accept(new PrettyPrintVisitor(printer));
        return printer.toString();
    }

    public PrettyPrintVisitor()
    {
        this(PrinterFactory.getPrinter());
    }

    public PrettyPrintVisitor(Printer newPrinter)
    {
        this.printer = newPrinter;
    }

    @Override
    public void visitAssignExpr(AssingExpr expr)
    {
        this.print(expr.getVarName()).print(" = ").printExpression(expr.getExpression());
    }

    @Override
    public void visitBinaryExpr(BinaryExpr expr)
    {
        this.print("(").printExpression(expr.getOperand1()).print(" ")
            .print(expr.getOperation().asString())
            .print(" ").printExpression(expr.getOperand2()).print(")");
    }

    @Override
    public void visitConstExpr(ConstExpr expr)
    {
        this.print(expr.getValue().asStringLiteral());
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
        this.printExpressionListLn(expr.getExpressions());;
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
            .print("{")
            .printExpressionListLn(expr.getExpressions())
            .print("}");
    }

    @Override
    public void visitStatementSequenceScript(StatementSequenceScript expr)
    {
        this.printExpressionListLn(expr.getExpressions()).newLine();
    }

    @Override
    public void visitVarExpr(VarExpr expr)
    {
        this.print(expr.getVariableName());
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
        this.print("[").printExpressionList(expr.getElements()).print("]");
    }

    @Override
    public void visitIfElseExpr(IfElseExpr expr)
    {
        this.print("if ").printExpression(expr.getCondition()).print(" then").newLine();
        this.tab().printExpressionListLn(expr.getIfScript().getExpressions()).tabBack();

        if (expr.hasElseSection())
        {
            this.print("else").newLine();
            this.tab().printExpressionListLn(expr.getElseScript().getExpressions()).tabBack();
        }

        this.print("endif");
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
