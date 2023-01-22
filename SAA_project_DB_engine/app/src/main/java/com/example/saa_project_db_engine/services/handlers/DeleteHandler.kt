package com.example.saa_project_db_engine.services.handlers

import android.util.Log
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.services.extensions.RowWithPageId

class DeleteHandler : QueryTypeHandlerOnSelect {
    val rows = mutableListOf<RowWithPageId>()

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        Log.d("DELETE", "deleting row: ${row.rowId}")
        rows.add(RowWithPageId(row, page.id))
        page.delete(index, row)
        manager.recordsCount--
    }

    override fun cleanup() {
    }

}