package com.example.saa_project_db_engine.ui

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.fragment.app.DialogFragment
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.services.models.TableInfo
import de.codecrafters.tableview.TableView
import de.codecrafters.tableview.toolkit.SimpleTableDataAdapter
import de.codecrafters.tableview.toolkit.SimpleTableHeaderAdapter

class TableDetailsDialogFragment(private val tableInfo: TableInfo) : DialogFragment() {
    private lateinit var tableStatsTableView: TableView<Array<String>>
    private lateinit var tableSchemaTableView: TableView<Array<String>>

    companion object {
        const val TAG = "TableDetailsDialogFragment"
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View? {
        return inflater.inflate(R.layout.table_details_dialog, container, false);
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        tableStatsTableView = view.findViewById(R.id.table_stats_table)
        tableSchemaTableView = view.findViewById(R.id.table_schema_table)

        val tableStatsData = arrayOf(
            arrayOf("Heap Size", "%.2f".format(tableInfo.heapSize) + " KB"),
            arrayOf("Indexes size", "%.2f".format(tableInfo.indexSize) + " KB"),
            arrayOf("Records count", "${tableInfo.recordsCount}")
        )
        updateTableStatsTableView(tableStatsData, "Statistic", "Value")

        val tableSchemaData = tableInfo.schema.map {
            arrayOf(it.fieldName, it.fieldType)
        }.toTypedArray()
        updateTableSchemaTableView(tableSchemaData, "Field", "Type")
    }

    private fun updateTableStatsTableView(data: Array<Array<String>>, vararg headers: String) {
        tableStatsTableView.columnCount = 2
        tableStatsTableView.dataAdapter = SimpleTableDataAdapter(requireContext(), data)
        tableStatsTableView.headerAdapter = SimpleTableHeaderAdapter(requireContext(), *headers)
    }

    private fun updateTableSchemaTableView(data: Array<Array<String>>, vararg headers: String) {
        tableSchemaTableView.columnCount = 2
        tableSchemaTableView.dataAdapter = SimpleTableDataAdapter(requireContext(), data)
        tableSchemaTableView.headerAdapter = SimpleTableHeaderAdapter(requireContext(), *headers)
    }
}