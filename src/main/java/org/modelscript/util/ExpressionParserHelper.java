package org.modelscript.util;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.modelscript.expr.AnonymousScript;
import org.modelscript.expr.Expression;
import org.modelscript.grammar.ModelScriptLexer;
import org.modelscript.grammar.ModelScriptParser;
import org.modelscript.grammar.ModelScriptTreeBuilderVisitor;

public class ExpressionParserHelper
{
    static public Expression toExpression(String s)
    {
        ModelScriptParser parser = stringToParser(s);
        ParseTree tree = parser.statement();

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();

        return visitor.visit(tree);
    }

    static public Expression toProjection(String s)
    {
        ModelScriptParser parser = stringToParser(s);
        ParseTree tree = parser.projectionStatement();

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();

        return visitor.visit(tree);
    }

    static public AnonymousScript toScript(String s)
    {
        ModelScriptParser parser = stringToParser(s);
        ParseTree tree = parser.script();

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();
        visitor.visit(tree);

        return visitor.getScript();
    }

    static private ModelScriptParser stringToParser(String s)
    {
        CharStream charStream = CharStreams.fromString(s);
        ModelScriptLexer lexer = new ModelScriptLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        return new ModelScriptParser(tokens);
    }
}
