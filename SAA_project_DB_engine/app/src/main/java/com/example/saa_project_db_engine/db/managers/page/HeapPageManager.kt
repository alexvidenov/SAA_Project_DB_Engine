package com.example.saa_project_db_engine.db.managers.page

import android.util.Log
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.NullPersistenceModelException
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
            val newRowId = fileManager.nextRowId
            it.rowId = newRowId
            Log.d("TEST", "next row id: ${fileManager.nextRowId}")
            fileManager.nextRowId++
            val resultantIndex = lastPage?.insert(it)
            lastPage?.addRowToRowOffsetArray(newRowId, resultantIndex!!)
        }
        commit(lastPage)
    }
}

fun HeapPageManager.forEachRowPageIndexed(action: (row: TableRow, pageId: Int) -> Unit) {
    var curPageId = ROOT_PAGE_ID

    loop@ while (true) {
        try {
            Log.d("TEST", "GETTING $curPageId")
            val page = get(curPageId)
            Log.d("TEST", "records size: ${page!!.records.size}")

            for (row in page.records) {
                Log.d("TEST", "EXECUTING ACTION: ${row.rowId}")
                action(row, page.id)
            }

            curPageId++
        } catch (e: NullPersistenceModelException) {
            Log.d("TEST", "BREAKING LOOP LABEL: ${e.toString()}")
            break@loop
        }
    }
}