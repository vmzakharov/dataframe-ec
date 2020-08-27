package io.github.vmzakharov.ecdataframe.util;

public class SysOutPrinter
implements Printer
{
    @Override
    public void print(String value)
    {
        System.out.print(value);
    }

    @Override
    public void newLine()
    {
        System.out.println();
    }
}
