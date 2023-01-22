package com.example.saa_project_db_engine.services

// TODO: implement implicit indexing on rowId? (since we don't have user defined primary keys yet)

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.extensions.*
import com.example.saa_project_db_engine.services.handlers.*
import com.example.saa_project_db_engine.services.models.*
import java.io.File

class TableService constructor(ctx: Context) {
    val dir: File = ctx.filesDir
    var managerPool = hashMapOf<String, TableManagerData>()

    val files: List<String>
        get() = dir.listFiles()?.filter { !it.isDirectory }?.map { it.name }!!

    fun load(tableName: String) { // TODO: return manager data from here to reduce lines
        loadTable(tableName)
        loadIndex(tableName)
    }

    fun insertRows(
        tableName: String, fields: MutableList<String>,
        inserts: MutableList<MutableList<String>>
    ): Int {
        load(tableName)
        val data = managerPool[tableName]!!
        val tableRows = mutableListOf<TableRow>()
        val schema = data.tableSchema
        val omittedFields = schema.fields.filter {
            !fields.contains(it.name())
        }

        inserts.removeAt(0) // first item it always empty due to way of parsing
        inserts.forEach {
            val record = GenericRecord(data.tableSchema)
            fields.forEachIndexed { index, s ->
                val field = schema.fields.find {
                    it.name() == s
                }
                val value = convertStringToNativeType(it[index], field!!.schema().type)
                Log.d("TEST", "PUTTING: $s, $value")
                record.put(s, value)
            }
            omittedFields.forEach {
                Log.d("TEST", "PUTTING DEFAULT: ${it.name()}, ${it.defaultVal()}")
                record.put(it.name(), it.defaultVal())
            }
            val buf = record.toByteBuffer()
            tableRows.add(TableRow(buf))
        }

        val rows = mutableListOf<RowWithPageId>()

        data.heapPageManager.insertRows(tableRows) { row, pageId ->
            rows.add(RowWithPageId(row, pageId))
        }

        applyConsistencyOnInsert(tableName, rows)

        return rows.size
    }

    private fun whereClauseWithHandlers(
        tableName: String,
        fields: List<String>,
        whereFields: List<String>,
        clauseType: WhereClause,
        handler: QueryTypeHandler,
    ) {
        if (analysePossibleIndexScan(whereFields, tableName)) {
            Log.d("TEST", "INDEX SCAN")
            val indexes = indexScan(tableName, clauseType)
            if (indexes != null) {
                Log.d("TEST", "fetchHeapResultsFromIndexValues")
                fetchHeapResultsFromIndexValues(tableName, indexes, handler)
            } else {
                Log.d("TEST", "INDEXES ARE NULL")
            }
        } else {
            Log.d("TEST", "FULL TABLE SCAN")
            fullTableScan(tableName, fields, clauseType, handler)
        }
    }

    // returns: rows updated
    fun update(
        tableName: String, fields: List<String>, whereFields: List<String>,
        clauseType: WhereClause, updates: Map<String, String>,
    ): Int {
        load(tableName)
        val data = managerPool[tableName]!!
        val cbks = mutableListOf<() -> Unit>()
        val updateHandler = UpdateHandler(data.tableSchema, updates)
        val handler = QueryTypeHandler(handler = updateHandler, persistCbk = {
            it?.let {
                Log.d("TEST", "callback")
                cbks.add(it)
            }
        })
        whereClauseWithHandlers(tableName, fields, whereFields, clauseType, handler)
        cbks.forEach {
            it.invoke()
        }
        applyConsistencyOnUpdate(tableName, updateHandler.oldNewPairList)
        handler.handler.cleanup()
        return updateHandler.rowsUpdated
    }

    // returns: rows deleted
    fun delete(
        tableName: String,
        fields: List<String>,
        whereFields: List<String>,
        clauseType: WhereClause,
    ): Int {
        load(tableName)
        // extract this as well
        val cbks = mutableListOf<() -> Unit>()
        val deleteHandler = DeleteHandler()
        val handler = QueryTypeHandler(handler = deleteHandler, persistCbk = {
            it?.let {
                cbks.add(it)
            }
        })
        whereClauseWithHandlers(tableName, fields, whereFields, clauseType, handler)
        cbks.forEach {
            it.invoke()
        }
        applyConsistencyOnDelete(tableName, deleteHandler.rows)
        handler.handler.cleanup()
        return deleteHandler.rows.size
    }

    fun select(
        tableName: String,
        fields: List<String>,
        whereFields: List<String>,
        clauseType: WhereClause,
        orderByFields: List<String>,
        distinct: Boolean
    ): SelectResultModel {
        load(tableName)
        val data = managerPool[tableName]!!
        val opts = SelectOpts(orderByFields, distinct)
        val selectHandler = SelectHandler(fields, opts, data.tableSchema)
        val handler =
            QueryTypeHandler(handler = selectHandler, persistCbk = {})
        whereClauseWithHandlers(tableName, fields, whereFields, clauseType, handler)
        selectHandler.cleanup()
        return selectHandler.res
    }

}