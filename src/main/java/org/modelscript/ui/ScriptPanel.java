package org.modelscript.ui;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.modelscript.expr.EvalContext;
import org.modelscript.expr.Script;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;
import org.modelscript.util.PrinterFactory;

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
            Script script = ExpressionParserHelper.toScript(textArea.getText());
            Value result = script.evaluate(new InMemoryEvaluationVisitor(newStoredContext));
            PrinterFactory.getPrinter().println("DONE: " + result.asStringLiteral());
            this.actionsPostEvaluation.forEach(Runnable::run);
        });

        JButton parseButton = new JButton("Parse");
        runButton.addActionListener(e -> {
            ExpressionParserHelper.toScript(textArea.getText());
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
