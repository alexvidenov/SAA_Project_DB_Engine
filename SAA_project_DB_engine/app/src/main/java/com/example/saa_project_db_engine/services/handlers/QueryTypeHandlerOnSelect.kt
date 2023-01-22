package com.example.saa_project_db_engine.services.handlers

import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow

interface QueryTypeHandlerOnSelect {
    fun handle(manager: HeapPageManager, page: HeapLogicalPage, index: Int, row: TableRow)
    fun cleanup()
}

data class QueryTypeHandler(
    val handler: QueryTypeHandlerOnSelect,
    val persistCbk: (cbk: (() -> Unit)?) -> Unit
)

