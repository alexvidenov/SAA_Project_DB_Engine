package com.example.saa_project_db_engine.db.managers.page

import android.util.Log
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow

class HeapPageManager constructor(override val fileManager: HeapFileManager) :
    PageManager<HeapLogicalPage>(fileManager) {

    fun allocate(initialRecords: MutableList<TableRow>): HeapLogicalPage {
        val page = fileManager.allocateNewHeapLogicalPage(initialRecords)
        pool[page.id] = page
        return page
    }

    fun insertRow(row: TableRow) {
        val lastPage = get(fileManager.lastPageId)
        row.rowId = fileManager.nextRowId
        fileManager.nextRowId++
        lastPage?.insert(row)
        commit(lastPage)
    }
}