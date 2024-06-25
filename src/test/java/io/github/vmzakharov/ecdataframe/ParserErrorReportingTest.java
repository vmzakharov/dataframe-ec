package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.grammar.CollectingErrorListener;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ParserErrorReportingTest
{
    @Test
    public void simpleErrorReporting()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

        parserHelper.toExpression("5 + ");

        assertTrue(errorListener.hasErrors());
        assertEquals(1, errorListener.getErrors().size());

        CollectingErrorListener.Error error = errorListener.getErrors().getFirst();

        assertEquals(1, error.line());
        assertEquals(4, error.charPositionInLine());
    }

    // todo: make the script rule fail on this input.
    @Test
    public void simpleErrorReporting2()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

//        Script script = parserHelper.toScript(" and x");
        Expression script = parserHelper.toExpression(" and x");

        assertNull(script);

        assertTrue(errorListener.hasErrors());
        assertEquals(1, errorListener.getErrors().size());

        CollectingErrorListener.Error error = errorListener.getErrors().getFirst();

        assertEquals(1, error.line());
        assertEquals(1, error.charPositionInLine());
    }

    @Test
    public void simpleErrorReporting3()
    {
        CollectingErrorListener errorListener = new CollectingErrorListener();
        ExpressionParserHelper parserHelper = new ExpressionParserHelper(errorListener);

        parserHelper.toScript("""
                function boo boo()
                {
                  if a > 7
                    x = 'greater'
                  else
                    x = 'lesser'
                  endif
                  (x == 'greater') ? 'yes' }
                """);

        assertTrue(errorListener.hasErrors());

        assertEquals(3, errorListener.getErrors().size());

        MutableList<IntIntPair> errorPositions = Lists.mutable.of(
                PrimitiveTuples.pair(1, 13), PrimitiveTuples.pair(4, 4), PrimitiveTuples.pair(8, 27));

        errorListener.getErrors().forEachInBoth(errorPositions, (e, a) -> {
            assertEquals(a.getOne(), e.line(), e.briefErrorMessage());
            assertEquals(a.getTwo(), e.charPositionInLine(), e.briefErrorMessage());
        });

        ListIterable<Integer> tokenLengths = Lists.immutable.of(3, 1, 1);
        errorListener.getErrors().forEachInBoth(tokenLengths, (e, a) ->
                assertEquals((int) a, e.tokenStopIndex() - e.tokenStartIndex() + 1));
    }
}
