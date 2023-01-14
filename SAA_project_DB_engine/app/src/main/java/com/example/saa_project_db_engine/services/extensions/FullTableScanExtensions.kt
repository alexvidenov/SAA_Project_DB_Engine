package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.managers.page.forEachRowPageIndexed
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.models.QueryTypeHandler
import com.example.saa_project_db_engine.services.models.SelectHandler
import com.example.saa_project_db_engine.services.models.WhereClause

fun TableService.fullTableScan(
    tableName: String,
    fields: List<String>,
    whereClause: WhereClause,
    queryHandler: QueryTypeHandler,
) {
    val data = managerPool[tableName]!!

    data.heapPageManager.forEachRowPageIndexed { row, pageId ->
        val record = GenericRecord(data.tableSchema)
        record.load(row.value)

        val res: Boolean = when (whereClause) {
            is WhereClause.LogicalOperations -> parseSubExpression(record, whereClause.ops)
            is WhereClause.SingleCondition -> parseCondition(record, whereClause.cond)
        }

        Log.d("EXPR", "R0W RES: ${res}")
        if (res) {
            val manager = data.heapPageManager
            val page = manager.get(pageId)
            if (page != null) {
//                IndexConsistencyService.addAffectedFieldEntry(
//                    leftPair.first.name,
//                    Pair(leftPair.first.recordKey, indexValues)
//                )
                queryHandler.handler.handle(
                    manager,
                    page,
                    page.getIndexForRowId(row.rowId!!)!!,
                    row
                )
            }

            var cbk: (() -> Unit)? = null

            if (queryHandler.handler !is SelectHandler) {
                cbk = {
                    data.heapPageManager.commit(page)
                }
            }

            queryHandler.persistCbk(cbk)
        }
    }
}
