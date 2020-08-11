package org.modelscript.expr.visitor;

import org.modelscript.expr.*;
import org.modelscript.expr.value.Value;

public interface ExpressionEvaluationVisitor
{
    Value visitAssignExpr(AssingExpr expr);
    Value visitBinaryExpr(BinaryExpr expr);
    Value visitConstExpr(ConstExpr expr);
    Value visitFunctionCallExpr(FunctionCallExpr expr);
    Value visitPropertyPathExpr(PropertyPathExpr expr);
    Value visitAnonymousScriptExpr(AnonymousScript expr);
    Value visitFunctionScriptExpr(FunctionScript expr);
    Value visitVarExpr(VarExpr expr);
    Value visitProjectionExpr(ProjectionExpr expr);
    Value visitVectorExpr(VectorExpr expr);
}
