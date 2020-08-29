package io.github.vmzakharov.ecdataframe.dsl.visitor;

import io.github.vmzakharov.ecdataframe.dsl.*;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

public interface ExpressionEvaluationVisitor
{
    Value visitAssignExpr(AssingExpr expr);
    Value visitBinaryExpr(BinaryExpr expr);
    Value visitConstExpr(ConstExpr expr);
    Value visitFunctionCallExpr(FunctionCallExpr expr);
    Value visitIfElseExpr(IfElseExpr expr);
    Value visitPropertyPathExpr(PropertyPathExpr expr);
    Value visitAnonymousScriptExpr(AnonymousScript expr);
    Value visitFunctionScriptExpr(FunctionScript expr);
    Value visitStatementSequenceScriptExpr(StatementSequenceScript expr);
    Value visitVarExpr(VarExpr expr);
    Value visitProjectionExpr(ProjectionExpr expr);
    Value visitVectorExpr(VectorExpr expr);
}
