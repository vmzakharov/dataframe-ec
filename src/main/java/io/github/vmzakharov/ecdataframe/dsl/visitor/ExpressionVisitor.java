package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dsl.*;

public interface ExpressionVisitor
{
    void visitAliasExpr(AliasExpr expr);
    void visitAssignExpr(AssingExpr expr);
    void visitBinaryExpr(BinaryExpr expr);
    void visitUnaryExpr(UnaryExpr expr);
    void visitConstExpr(ConstExpr expr);
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
}
