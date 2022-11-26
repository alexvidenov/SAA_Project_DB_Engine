package com.example.saa_project_db_engine.db.managers.page

import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.avro.TableRow
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage

class HeapPageManager constructor(override val fileManager: HeapFileManager) :
    PageManager<HeapLogicalPage>(fileManager) {

    fun allocate(initialRecords: MutableList<TableRow>): HeapLogicalPage {
        val page = fileManager.allocateNewHeapLogicalPage(initialRecords)
        pool[page.id] = page
        return page
    }

    fun insertRow(row: TableRow) {
        val lastPage = get(fileManager.nextLogicalPageId?.minus(1))
        lastPage?.insert(fileManager.nextRowId ?: 0, row)
        commit(lastPage)
    }
}