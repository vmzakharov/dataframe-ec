package io.github.vmzakharov.ecdataframe.ui;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.grammar.CollectingErrorListener;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import javax.swing.*;
import java.awt.*;

public class ScriptPanel
extends JPanel
{
    private final MutableList<Runnable> actionsPostEvaluation = Lists.mutable.of();

    public ScriptPanel(EvalContext newStoredContext)
    {
        super(new BorderLayout());

        JTextArea textArea = new JTextArea();
        JScrollPane scrollPane = new JScrollPane(textArea);
        scrollPane.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(5, 5, 0, 5),
                BorderFactory.createLineBorder(Color.BLACK)));
        this.add(scrollPane, BorderLayout.CENTER);

        JButton runButton = new JButton("Run");
        runButton.addActionListener(e -> {
            CollectingErrorListener errorListener = new CollectingErrorListener();
            Script script = new ExpressionParserHelper(errorListener).toScript(textArea.getText());
            if (errorListener.hasErrors())
            {
                PrinterFactory.getPrinter().println(">>> PARSE ERRORS:");
                errorListener.getErrors()
                        .collect(CollectingErrorListener.Error::detailedErrorMessage)
                        .forEach(PrinterFactory.getPrinter()::println);
            }
            else
            {
                Value result = script.evaluate(new InMemoryEvaluationVisitor(newStoredContext));
                PrinterFactory.getPrinter().println(result.stringValue());
            }
            this.actionsPostEvaluation.forEach(Runnable::run);
        });

        JButton parseButton = new JButton("Parse");
        runButton.addActionListener(e -> {
            ExpressionParserHelper.DEFAULT.toScript(textArea.getText());
            this.actionsPostEvaluation.forEach(Runnable::run);
        });

        JButton clearButton = new JButton("Clear");
        clearButton.addActionListener(e -> textArea.setText(""));

        ButtonPanel buttonPanel = new ButtonPanel();
        buttonPanel.addButton(runButton);
        buttonPanel.addButton(parseButton);
        buttonPanel.addButton(clearButton);

        this.add(buttonPanel, BorderLayout.SOUTH);

        this.setPreferredSize(new Dimension(500, 500));
    }

    public void addActionPostEvaluation(Runnable action)
    {
        this.actionsPostEvaluation.add(action);
    }
}
