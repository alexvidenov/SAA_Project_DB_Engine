package com.example.saa_project_db_engine.db.managers.page

import android.util.Log
import com.example.saa_project_db_engine.ROOT_PAGE_ID
import com.example.saa_project_db_engine.db.NullPersistenceModelException
import com.example.saa_project_db_engine.db.PageFullException
import com.example.saa_project_db_engine.db.managers.PageManager
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.HeapPageData
import com.example.saa_project_db_engine.db.storage.models.TableRow
import kotlin.properties.Delegates

class HeapPageManager constructor(override val fileManager: HeapFileManager) :
    PageManager<TableRow, HeapPageData, HeapLogicalPage>(fileManager) {
    var recordsCount = fileManager.entriesCount
        get() = fileManager.entriesCount
        set(value) {
            fileManager.entriesCount = value
            field = value
        }

    init {
        get(fileManager.lastPageId) // pre-warms last page since it's very likely to be used
    }

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

    // callback with each row and its corresponding page id and row id
    fun insertRows(rows: List<TableRow>, rowPersistedCbk: (row: TableRow, pageId: Int) -> Unit) {
        Log.d("TEST", "last page id: ${fileManager.lastPageId}")
        val lastPage = get(fileManager.lastPageId)
        var curPageToInsert = lastPage
        rows.forEach {
            fileManager.entriesCount++
            val newRowId = fileManager.nextRowId
            it.rowId = newRowId
            Log.d("TEST", "next row id: ${fileManager.nextRowId}")
            fileManager.nextRowId++
            var resultantIndex: Int?
            try {
                resultantIndex = curPageToInsert?.insert(it)
            } catch (exc: PageFullException) {
                curPageToInsert = split(lastPage!!)
                resultantIndex = curPageToInsert?.insert(it)
            }
            curPageToInsert?.addRowToRowOffsetArray(newRowId, resultantIndex!!)
            rowPersistedCbk(it, curPageToInsert?.id!!)
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