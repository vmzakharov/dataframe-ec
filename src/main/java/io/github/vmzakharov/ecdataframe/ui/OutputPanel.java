package io.github.vmzakharov.ecdataframe.ui;

import javax.swing.*;
import java.awt.*;
import java.awt.datatransfer.StringSelection;

public class OutputPanel
extends JPanel
{
    private final JTextArea textArea = new JTextArea();

    public OutputPanel()
    {
        super(new BorderLayout());

        this.textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);

        scrollPane.setBorder(BorderFactory.createCompoundBorder(
            BorderFactory.createEmptyBorder(0, 5, 5, 5),
            BorderFactory.createLineBorder(Color.BLACK)));

        this.add(scrollPane, BorderLayout.CENTER);

        JButton clearButton = new JButton("Clear");
        clearButton.addActionListener(e -> this.textArea.setText(""));

        JButton copyButton = new JButton("Copy to the Clipboard");
        copyButton.addActionListener(e ->
                Toolkit.getDefaultToolkit()
                        .getSystemClipboard()
                        .setContents(new StringSelection(this.textArea.getText()), null));

        ButtonPanel buttonPanel = new ButtonPanel();
        buttonPanel.addButton(copyButton);
        buttonPanel.addButton(clearButton);

        this.add(buttonPanel, BorderLayout.SOUTH);
    }

    public void addText(String text)
    {
        this.textArea.append(text);
    }
}
