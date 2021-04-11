package io.github.vmzakharov.ecdataframe.util;

final public class PrinterFactory
{
    static private Printer printer = new SysOutPrinter();
    static private Printer errPrinter = new SysErrPrinter();

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

    public static Printer getErrPrinter()
    {
        return errPrinter;
    }

    public static void setErrPrinter(Printer newPrinter)
    {
        errPrinter = newPrinter;
    }
}
