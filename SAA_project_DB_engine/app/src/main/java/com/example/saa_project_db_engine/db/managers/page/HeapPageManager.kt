package com.example.saa_project_db_engine.db.managers.page

import android.util.Log
import com.example.saa_project_db_engine.db.managers.PageManager
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.HeapPageData
import com.example.saa_project_db_engine.db.storage.models.TableRow

class HeapPageManager constructor(override val fileManager: HeapFileManager) :
    PageManager<TableRow, HeapPageData, HeapLogicalPage>(fileManager) {

    override fun allocateNewLogicalPage(
        page: HeapLogicalPage,
        records: MutableList<TableRow>
    ): HeapLogicalPage {
        val allocated = fileManager.allocateNewHeapLogicalPage(records)
        pool[allocated.id] = allocated
        return allocated
    }

    fun insertRow(row: TableRow) {
        val lastPage = get(fileManager.lastPageId)
        row.rowId = fileManager.nextRowId
        fileManager.nextRowId++
        lastPage?.insert(row)
        commit(lastPage)
    }

    fun insertRows(rows: List<TableRow>) {
        Log.d("TEST", "last page id: ${fileManager.lastPageId}")
        val lastPage = get(fileManager.lastPageId)
        rows.forEach {
            it.rowId = fileManager.nextRowId
            Log.d("TEST", "next row id: ${fileManager.nextRowId}")
            fileManager.nextRowId++
            lastPage?.insert(it)
        }
        commit(lastPage)
    }
}