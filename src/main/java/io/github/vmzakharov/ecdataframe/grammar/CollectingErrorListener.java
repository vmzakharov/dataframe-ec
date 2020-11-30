package io.github.vmzakharov.ecdataframe.grammar;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;

public class CollectingErrorListener
extends BaseErrorListener
{
    private final MutableList<Error> errors = Lists.mutable.of();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer,
                            Object offendingSymbol,
                            int line, int charPositionInLine,
                            String msg,
                            RecognitionException e)
    {
        this.errors.add(new Error(offendingSymbol, line, charPositionInLine, msg));
    }

    public ListIterable<Error> getErrors()
    {
        return this.errors;
    }

    public boolean hasErrors()
    {
        return this.errors.notEmpty();
    }

    public static class Error
    {
        private final Object offendingSymbol;
        private final int line;
        private final int charPositionInLine;
        private final String message;

        public Error(Object newOffendingSymbol, int newLine, int newCharPositionInLine, String newMessage)
        {
            this.offendingSymbol = newOffendingSymbol;
            this.line = newLine;
            this.charPositionInLine = newCharPositionInLine;
            this.message = newMessage;
        }

        public int line()
        {
            return this.line;
        }

        public int charPositionInLine()
        {
            return this.charPositionInLine;
        }

        public int tokenStartIndex()
        {
            return ((Token) this.offendingSymbol).getStartIndex();
        }

        public int tokenStopIndex()
        {
            return ((Token) this.offendingSymbol).getStopIndex();
        }

        public String detailedErrorMessage()
        {
            return "line " + this.line + ":" + this.charPositionInLine + " at " + this.offendingSymbol + ": " + this.message;
        }

        public String briefErrorMessage()
        {
            return "line " + this.line + ":" + this.charPositionInLine + " " + this.message;
        }
    }
}
