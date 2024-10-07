package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dsl.AliasExpr;
import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.AssignExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.DecimalExpr;
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

public interface ExpressionVisitor
{
    void visitAliasExpr(AliasExpr expr);

    void visitAssignExpr(AssignExpr expr);

    void visitBinaryExpr(BinaryExpr expr);

    void visitUnaryExpr(UnaryExpr expr);

    void visitConstExpr(Value expr);

    void visitFunctionCallExpr(FunctionCallExpr expr);

    void visitIfElseExpr(IfElseExpr expr);

    void visitPropertyPathExpr(PropertyPathExpr expr);

    void visitAnonymousScriptExpr(AnonymousScript expr);

    void visitFunctionScriptExpr(FunctionScript expr);

    void visitStatementSequenceScript(StatementSequenceScript expr);

    void visitVarExpr(VarExpr expr);

    void visitProjectionExpr(ProjectionExpr expr);

    void visitVectorExpr(VectorExpr expr);

    void visitIndexExpr(IndexExpr expr);

    void visitDecimalExpr(DecimalExpr expr);
}
