package com.example.saa_project_db_engine.executor

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.db.models.SelectResultModel
import com.example.saa_project_db_engine.parsers.StatementParser
import com.example.saa_project_db_engine.parsers.models.FieldSchemaDefinition
import com.example.saa_project_db_engine.parsers.models.Query
import com.example.saa_project_db_engine.parsers.models.QueryType
import com.example.saa_project_db_engine.parsers.models.WhereClauseType
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.extensions.buildTableSchemaRepresentation
import com.example.saa_project_db_engine.services.extensions.createIndex
import com.example.saa_project_db_engine.services.extensions.createTable
import com.example.saa_project_db_engine.services.models.WhereClause

class SchemaExecutor constructor(ctx: Context) {
    private val tableService = TableService(ctx)
    private val parser = StatementParser()

    // flows here to emit UI data

    fun execute(raw: String) {
        val parsed = parser.parseQuery(raw)
        Log.d("TEST", "PARSED QUERY: $parsed")
        executeInternal(parsed)
    }

    private fun executeInternal(query: Query) {
        when (query.type) {
            QueryType.Undefined -> {}
            QueryType.CreateTable -> {
                handleCreateTable(query.table, query.schema)
            }
            QueryType.DropTable -> {

            }
            QueryType.ListTables -> TODO()
            QueryType.TableInfo -> TODO()
            QueryType.Insert -> {
                handleInsert(query.table, query.fields, query.inserts)
            }
            QueryType.Select -> {
                handleSelect(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond
                )
            }
            QueryType.Update -> TODO()
            QueryType.Delete -> {
                handleDelete(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond
                )
            }
            QueryType.CreateIndex -> {
                tableService.createIndex(query.table, query.indexName, query.fields.first())
            }
            QueryType.DropIndex -> TODO()
        }
    }

    private fun handleCreateTable(tableName: String, fields: List<FieldSchemaDefinition>) {
        val schema = buildTableSchemaRepresentation(tableName, fields)
        tableService.createTable(tableName, schema)
    }

    private fun handleInsert(
        tableName: String,
        fields: MutableList<String>,
        inserts: MutableList<MutableList<String>>
    ) {
        tableService.insertRows(tableName, fields, inserts)
    }

    private fun handleSelect(
        tableName: String,
        fields: MutableList<String>,
        whereFields: MutableList<String>,
        conditions: MutableList<WhereClauseType.LogicalOperation>,
        currentCond: WhereClauseType.Condition? = null
    ) {
        val res: SelectResultModel = if (conditions.isEmpty() && currentCond != null) {
            tableService.select(
                tableName,
                fields,
                whereFields,
                WhereClause.SingleCondition(currentCond)
            )
        } else {
            tableService.select(
                tableName,
                fields,
                whereFields,
                WhereClause.LogicalOperations(conditions)
            )
        }
        Log.d("TEST", "HANDLE SELECT RES: $res")
    }

    private fun handleDelete(
        tableName: String,
        fields: MutableList<String>,
        whereFields: MutableList<String>,
        conditions: MutableList<WhereClauseType.LogicalOperation>,
        currentCond: WhereClauseType.Condition? = null
    ) {
        Log.d("TEST", "delete")
        if (conditions.isEmpty() && currentCond != null) {
            tableService.delete(
                tableName,
                fields,
                whereFields,
                WhereClause.SingleCondition(currentCond)
            )
        } else {
            tableService.delete(
                tableName,
                fields,
                whereFields,
                WhereClause.LogicalOperations(conditions)
            )
        }
    }

}