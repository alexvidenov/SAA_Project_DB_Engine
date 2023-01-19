package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.handlers.QueryTypeHandler
import com.example.saa_project_db_engine.services.handlers.SelectHandler

fun TableService.fetchHeapResultsFromIndexValues(
    tableName: String,
    values: IndexValues,
    handler: QueryTypeHandler
) {
    values.records.forEach {
        fetchRowFromIndexValue(tableName, it, handler)
    }
}

// assuming the index stays consistent. (which it does, for now)
fun TableService.fetchRowFromIndexValue(
    tableName: String,
    index: IndexValue,
    queryHandler: QueryTypeHandler
) {
    val data = managerPool[tableName]!!

    val heapManager = data.heapPageManager
    val page = heapManager.get(index.pageId)
    val recordIndex = page!!.getIndexForRowId(index.rowId)
    val old = page.records[recordIndex!!]

    queryHandler.handler.handle(heapManager, page, recordIndex, old)

    // extract general?
    var cbk: (() -> Unit)? = null

    Log.d(
        "TEST",
        "HANDLER: ${queryHandler.handler.javaClass} ${queryHandler.handler !is SelectHandler}"
    )

    if (queryHandler.handler !is SelectHandler) {
        cbk = {
            Log.d("TEST", "committing")
            heapManager.commit(page)
        }
    }

    queryHandler.persistCbk(cbk)
}