package io.github.vmzakharov.ecdataframe.util;

public class SysErrPrinter
implements Printer
{
    @Override
    public void print(String value)
    {
        System.err.print(value);
    }

    @Override
    public void newLine()
    {
        System.err.println();
    }
}
