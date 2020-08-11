package org.modelscript.expr.visitor;

import org.modelscript.expr.*;

public interface ExpressionVisitor
{
    void visitAliasExpr(AliasExpr expr);
    void visitAssignExpr(AssingExpr expr);
    void visitBinaryExpr(BinaryExpr expr);
    void visitConstExpr(ConstExpr expr);
    void visitFunctionCallExpr(FunctionCallExpr expr);
    void visitPropertyPathExpr(PropertyPathExpr expr);
    void visitAnonymousScriptExpr(AnonymousScript expr);
    void visitFunctionScriptExpr(FunctionScript expr);
    void visitVarExpr(VarExpr expr);
    void visitProjectionExpr(ProjectionExpr expr);
    void visitVectorExpr(VectorExpr expr);
}
