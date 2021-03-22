package io.github.vmzakharov.ecdataframe.util;

final public class PrinterFactory
{
    static private Printer printer = new SysOutPrinter();

    private PrinterFactory()
    {
        // Utility class
    }

    public static Printer getPrinter()
    {
        return printer;
    }

    public static void setPrinter(Printer newPrinter)
    {
        printer = newPrinter;
    }
}
