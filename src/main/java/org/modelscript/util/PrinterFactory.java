package org.modelscript.util;

public class PrinterFactory
{
    static private Printer printer = new SysOutPrinter();

    public static Printer getPrinter()
    {
        return printer;
    }

    public static void setPrinter(Printer newPrinter)
    {
        printer = newPrinter;
    }
}
