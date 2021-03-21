package io.github.vmzakharov.ecdataframe.ui;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import org.eclipse.collections.api.list.MutableList;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Color;

public class MemoryPanel
extends JPanel
{
    private final JTable variables;
    private final JButton removeButton;
    private final JButton removeAllButton;

    private final EvalContext storedContext;
    private final VariableTableModel variableTableModel;

    public MemoryPanel(EvalContext newStoredContext)
    {
        super(new BorderLayout());

        this.storedContext = newStoredContext;

        this.variableTableModel = new VariableTableModel();

        this.variables = new JTable(this.variableTableModel);
        this.removeButton = new JButton("Remove");
        this.removeAllButton = new JButton("Remove All");

        this.refreshVariableList();

        this.variables.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        this.variables.getSelectionModel()
                .addListSelectionListener(e -> this.removeButton.setEnabled(this.variables.getSelectedRowCount() > 0));

        JScrollPane scrollPane = new JScrollPane(this.variables);
        scrollPane.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(5, 5, 0, 5),
                BorderFactory.createLineBorder(Color.BLACK)));
        this.add(scrollPane, BorderLayout.CENTER);

        this.variables.setFillsViewportHeight(true);

        ButtonPanel buttonPanel = new ButtonPanel();
        buttonPanel.addButton(this.removeButton);
        buttonPanel.addButton(this.removeAllButton);
        this.removeButton.setEnabled(false);

        this.removeButton.addActionListener(e -> {
            int[] selectedRows = this.variables.getSelectedRows();
            for (int i = selectedRows.length - 1; i >= 0; i--)
            {
                String removed = (String) this.variableTableModel.getValueAt(selectedRows[i], 0);
                this.storedContext.removeVariable(removed);
            }
            this.variableTableModel.fireTableDataChanged();
        });

        this.removeAllButton.addActionListener(e -> {
            this.storedContext.removeAllVariables();
            this.refreshVariableList();
        });

        this.add(buttonPanel, BorderLayout.SOUTH);
    }

    public void refreshVariableList()
    {
        this.variableTableModel.fireTableDataChanged();
        this.removeAllButton.setEnabled(this.variableTableModel.getRowCount() > 0);
    }

    private class VariableTableModel
    extends AbstractTableModel
    {
        private final String[] columnNames = new String[] {"Variable", "Type", "Value"};
        private MutableList<String> variableNames;

        public VariableTableModel()
        {
            this.loadVariableNames();
        }

        private void loadVariableNames()
        {
            this.variableNames = MemoryPanel.this.storedContext.getVariableNames().toList();
        }

        @Override
        public int getRowCount()
        {
            return MemoryPanel.this.storedContext.getVariableNames().size();
        }

        @Override
        public int getColumnCount()
        {
            return this.columnNames.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            String variableName = this.variableNames.get(rowIndex);

            switch (columnIndex)
            {
                case 0:
                    return variableName;
                case 1:
                    return MemoryPanel.this.storedContext.getVariable(variableName).getType().toString();
                case 2:
                    String valueAsString = MemoryPanel.this.storedContext.getVariable(variableName).asStringLiteral();
                    if (valueAsString.length() > 50)
                    {
                        valueAsString = valueAsString.substring(0, 50) + "...";
                    }
                    return valueAsString;
                default:
                    return "Highly unlikely";
            }
        }

        @Override
        public String getColumnName(int column)
        {
            return this.columnNames[column];
        }

        @Override
        public void fireTableDataChanged()
        {
            this.loadVariableNames();
            super.fireTableDataChanged();
        }
    }
}
