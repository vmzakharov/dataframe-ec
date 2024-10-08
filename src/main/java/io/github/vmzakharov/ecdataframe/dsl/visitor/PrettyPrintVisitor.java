package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dsl.AliasExpr;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.AssignExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.DecimalExpr;
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
    public void visitAssignExpr(AssignExpr expr)
    {
        this.print(this.escapeVarNameIfNeeded(expr.varName(), expr.isEscaped()))
            .print(" = ")
            .printExpression(expr.expression());
    }

    private String escapeVarNameIfNeeded(String name, boolean escaped)
    {
        return escaped ? "${" + name + "}" : name;
    }

    @Override
    public void visitBinaryExpr(BinaryExpr expr)
    {
        this.print("(").printExpression(expr.operand1()).print(" ")
            .print(expr.operation().asString())
            .print(" ").printExpression(expr.operand2()).print(")");
    }

    @Override
    public void visitUnaryExpr(UnaryExpr expr)
    {
        if (expr.operation().isPrefix())
        {
            this.print(expr.operation().asString()).print("(").printExpression(expr.operand()).print(")");
        }
        else
        {
            this.print("(").printExpression(expr.operand()).print(") ").print(expr.operation().asString());
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
        this.print(expr.functionName()).print("(").printExpressionList(expr.parameters()).print(")");
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
        this.print(expr.pathElements().makeString("."));
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
          this.print(this.escapeVarNameIfNeeded(expr.variableName(), expr.isEscaped()));
    }

    @Override
    public void visitProjectionExpr(ProjectionExpr expr)
    {
        this.print("project {")
            .printExpressionList(expr.projectionElements())
            .print("}");

        if (expr.whereClause() != null)
        {
            this.print(" where ").printExpression(expr.whereClause());
        }
    }

    @Override
    public void visitAliasExpr(AliasExpr expr)
    {
        this.print(expr.alias()).print(" : ").printExpression(expr.expression());
    }

    @Override
    public void visitVectorExpr(VectorExpr expr)
    {
        this.print("(").printExpressionList(expr.elements()).print(")");
    }

    @Override
    public void visitIndexExpr(IndexExpr expr)
    {
        this.printExpression(expr.vectorExpr()).print("[").printExpression(expr.indexExpr()).print("]");
    }

    @Override
    public void visitDecimalExpr(DecimalExpr expr)
    {
        this.print("[")
            .printExpression(expr.unscaledValueExpr())
            .print(",")
            .printExpression(expr.scaleExpr())
            .print("]");
    }

    @Override
    public void visitIfElseExpr(IfElseExpr expr)
    {
        if (expr.isTernary())
        {
            this
                .printExpression(expr.condition())
                .print(" ? ")
                .printExpression(expr.ifScript())
                .print(" : ")
                .printExpression(expr.elseScript());
        }
        else
        {
            this.print("if ").printExpression(expr.condition()).print(" then").newLine();
            this.tab();
            expr.ifScript().accept(this);
            this.tabBack();

            if (expr.hasElseSection())
            {
                this.print("else").newLine();
                this.tab();
                expr.elseScript().accept(this);
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
