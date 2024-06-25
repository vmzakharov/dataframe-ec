package io.github.vmzakharov.ecdataframe.util;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.grammar.ModelScriptLexer;
import io.github.vmzakharov.ecdataframe.grammar.ModelScriptParser;
import io.github.vmzakharov.ecdataframe.grammar.ModelScriptTreeBuilderVisitor;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class ExpressionParserHelper
{
    static public final ExpressionParserHelper DEFAULT = new ExpressionParserHelper();

    private ANTLRErrorListener replacementErrorListener;

    public ExpressionParserHelper(ANTLRErrorListener newReplacementErrorListener)
    {
        this.replacementErrorListener = newReplacementErrorListener;
    }

    public ExpressionParserHelper()
    {
    }

    public Expression toExpression(String s)
    {
        ModelScriptParser parser = this.stringToParser(s);
        ParseTree tree = parser.statement();

        if (parser.getNumberOfSyntaxErrors() > 0)
        {
            return null;
        }

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();

        return visitor.visit(tree);
    }

    public Expression toProjection(String s)
    {
        ModelScriptParser parser = this.stringToParser(s);
        ParseTree tree = parser.projectionStatement();

        if (parser.getNumberOfSyntaxErrors() > 0)
        {
            return null;
        }

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();

        return visitor.visit(tree);
    }

    public Expression toExpressionOrScript(String s)
    {
        AnonymousScript script = this.toScript(s);

        if (script == null)
        {
            return null;
        }

        if (script.getExpressions().size() == 1 && script.getFunctions().isEmpty())
        {
            return script.getExpressions().getFirst();
        }

        return script;
    }

    public AnonymousScript toScript(String s)
    {
        ModelScriptParser parser = this.stringToParser(s);
        ParseTree tree = parser.script();

        if (parser.getNumberOfSyntaxErrors() > 0)
        {
            return null;
        }

        ModelScriptTreeBuilderVisitor visitor = new ModelScriptTreeBuilderVisitor();
        visitor.visit(tree);

        return visitor.getAsAnonymousScript();
    }

    private ModelScriptParser stringToParser(String s)
    {
        CharStream charStream = CharStreams.fromString(s);
        ModelScriptLexer lexer = new ModelScriptLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ModelScriptParser parser = new ModelScriptParser(tokens);
        if (this.replacementErrorListener != null)
        {
            parser.removeErrorListeners();
            parser.addErrorListener(this.replacementErrorListener);
        }
        return parser;
    }
}
