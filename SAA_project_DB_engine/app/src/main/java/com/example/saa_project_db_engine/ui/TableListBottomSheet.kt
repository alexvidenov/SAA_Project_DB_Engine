package com.example.saa_project_db_engine.ui

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import com.example.saa_project_db_engine.R
import com.example.saa_project_db_engine.services.models.TableInfo
import com.example.saa_project_db_engine.ui.adapters.TablesAdapter
import com.example.saa_project_db_engine.ui.models.TableUIModel
import com.google.android.material.bottomsheet.BottomSheetDialogFragment

class TableListBottomSheet(
    private val getTables: () -> List<TableUIModel>,
    private val getTableInfo: (tableName: String) -> TableInfo
) :
    BottomSheetDialogFragment() {
    private lateinit var tableListRv: RecyclerView

    companion object {
        const val TAG = "TableListBottomSheet"
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View? {
        return inflater.inflate(R.layout.table_list_bottom_sheet, container, false)
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        tableListRv = view.findViewById(R.id.tables_recycler_view)

        val adapter = TablesAdapter() {
            val tableInfo = getTableInfo(it.name)
            TableDetailsDialogFragment(tableInfo).show(
                requireActivity().supportFragmentManager,
                TableDetailsDialogFragment.TAG
            )
        }

        val tables = getTables()
        adapter.submitList(tables)

        tableListRv.adapter = adapter

    }
}