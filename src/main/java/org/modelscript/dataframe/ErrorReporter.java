package org.modelscript.dataframe;

import org.modelscript.util.PrinterFactory;

public class ErrorReporter
{
    public static void reportError(boolean badThingHappened, String errorText)
    {
        if (badThingHappened)
        {
            ErrorReporter.reportError(errorText);
        }
    }

    public static void reportError(String errorText)
    {
        PrinterFactory.getPrinter().println("ERROR: " + errorText);
        throw new RuntimeException(errorText);
    }

    public static void reportError(String errorText, Throwable t)
    {
        PrinterFactory.getPrinter().println("ERROR: " + errorText);
        throw new RuntimeException(errorText, t);
    }
}
