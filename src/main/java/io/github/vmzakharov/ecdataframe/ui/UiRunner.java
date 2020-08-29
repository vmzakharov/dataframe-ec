package io.github.vmzakharov.ecdataframe.ui;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;

import javax.swing.*;
import java.awt.*;

public class UiRunner
extends JFrame
{
    private final EvalContext storedContext = new SimpleEvalContext();

    public static void main(String[] args)
    {
        javax.swing.SwingUtilities.invokeLater(() -> {
            UiRunner uiRunner = new UiRunner();
            uiRunner.setVisible(true);
        });
    }

    public UiRunner()
    throws HeadlessException
    {
        super("DSL Runner");
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);

        Container contentPane = this.getContentPane();

        ScriptPanel scriptPanel = new ScriptPanel(this.storedContext);
        MemoryPanel memoryPanel = new MemoryPanel(this.storedContext);
        OutputPanel outputPanel = new OutputPanel();
//        PrinterFactory.getPrinter().println(result.stringValue());

        scriptPanel.addActionPostEvaluation(memoryPanel::refreshVariableList);

        PrinterFactory.setPrinter(outputPanel::addText);

        JSplitPane horizontalSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, scriptPanel, memoryPanel);
        horizontalSplit.setOneTouchExpandable(true);
        horizontalSplit.setResizeWeight(1.0);

        JSplitPane verticalSplit = new JSplitPane(JSplitPane.VERTICAL_SPLIT, horizontalSplit, outputPanel);
        verticalSplit.setOneTouchExpandable(true);
        horizontalSplit.setResizeWeight(0.8);

        contentPane.add(verticalSplit, BorderLayout.CENTER);

        this.pack();
    }
}
