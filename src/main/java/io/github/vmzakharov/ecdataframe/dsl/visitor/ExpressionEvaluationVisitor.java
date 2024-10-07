package io.github.vmzakharov.ecdataframe.dsl.visitor;

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

public interface ExpressionEvaluationVisitor
{
    Value visitAssignExpr(AssignExpr expr);

    Value visitBinaryExpr(BinaryExpr expr);

    Value visitUnaryExpr(UnaryExpr expr);

    Value visitConstExpr(Value expr);

    Value visitFunctionCallExpr(FunctionCallExpr expr);

    Value visitIfElseExpr(IfElseExpr expr);

    Value visitPropertyPathExpr(PropertyPathExpr expr);

    Value visitAnonymousScriptExpr(AnonymousScript expr);

    Value visitFunctionScriptExpr(FunctionScript expr);

    Value visitStatementSequenceScriptExpr(StatementSequenceScript expr);

    Value visitVarExpr(VarExpr expr);

    Value visitProjectionExpr(ProjectionExpr expr);

    Value visitVectorExpr(VectorExpr expr);

    Value visitIndexExpr(IndexExpr expr);

    Value visitDecimalExpr(DecimalExpr expr);
}
