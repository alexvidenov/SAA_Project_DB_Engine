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
import com.example.saa_project_db_engine.services.extensions.*
import com.example.saa_project_db_engine.services.models.TableInfo
import com.example.saa_project_db_engine.services.models.WhereClause
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

class SchemaExecutor constructor(ctx: Context) {
    private val tableService = TableService(ctx)
    private val parser = StatementParser()

    private val _state: MutableStateFlow<SelectResultModel> = MutableStateFlow(
        SelectResultModel()
    )
    val state: StateFlow<SelectResultModel>
        get() = _state

    private val _events = MutableSharedFlow<String>()
    val events: SharedFlow<String>
        get() = _events

    private var currentTables: List<String> = mutableListOf()
    private var currentTableInfo: TableInfo? = null

    fun execute(raw: String) {
        val parsed = parser.parseQuery(raw)
        Log.d("TEST", "PARSED QUERY: $parsed")
        executeInternal(parsed)
    }

    fun getTables(): List<String> {
        execute("ListTables")
        return currentTables
    }

    fun getTableInfo(tableName: String): TableInfo {
        execute("TableInfo ${tableName}")
        return currentTableInfo!!
    }

    private fun executeInternal(query: Query) {
        when (query.type) {
            QueryType.Undefined -> {}
            QueryType.CreateTable -> {
                handleCreateTable(query.table, query.schema)
            }
            QueryType.DropTable -> {
                tableService.dropTable(query.table)
            }
            QueryType.ListTables -> {
                currentTables = tableService.listTables()
                Log.d("TEST", "tables :$currentTables")
            }
            QueryType.TableInfo -> {
                currentTableInfo = tableService.getTableInfo(query.table)
                Log.d("TEST", "TABLE INFO: $currentTableInfo")
            }
            QueryType.Insert -> {
                val rows = tableService.insertRows(query.table, query.fields, query.inserts)
                runBlocking {
                    _events.emit("$rows rows inserted")
                }
            }
            QueryType.Select -> {
                val res = handleGeneralCrudOperation(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond
                ) { tableName,
                    fields,
                    whereFields,
                    clauseType ->
                    tableService.select(
                        tableName,
                        fields,
                        whereFields,
                        clauseType,
                        query.orderByFields,
                        query.distinct
                    )
                }
                _state.value = res
            }
            QueryType.Update -> {
                val rowsUpdated = handleGeneralCrudOperation(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond
                ) { tableName,
                    fields,
                    whereFields,
                    clauseType ->
                    tableService.update(
                        tableName,
                        fields,
                        whereFields,
                        clauseType,
                        query.updates
                    )
                }
                runBlocking {
                    _events.emit("${rowsUpdated} rows updated")
                }
            }
            QueryType.Delete -> {
                val deletedRows = handleGeneralCrudOperation(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond, tableService::delete
                )
                runBlocking {
                    _events.emit("${deletedRows} rows deleted")
                }
            }
            QueryType.CreateIndex -> {
                tableService.createIndex(query.table, query.indexName, query.fields.first())
            }
            QueryType.DropIndex -> {
                tableService.dropIndex(query.table, query.indexName)
            }
        }
    }

    private fun handleCreateTable(tableName: String, fields: List<FieldSchemaDefinition>) {
        val schema = buildTableSchemaRepresentation(tableName, fields)
        tableService.createTable(tableName, schema)
    }

    private fun <T> handleGeneralCrudOperation(
        tableName: String,
        fields: MutableList<String>,
        whereFields: MutableList<String>,
        conditions: MutableList<WhereClauseType.LogicalOperation>,
        currentCond: WhereClauseType.Condition? = null,
        handler: (
            tableName: String,
            fields: List<String>,
            whereFields: List<String>,
            clauseType: WhereClause
        ) -> T
    ): T {
        return if (conditions.isEmpty() && currentCond != null) {
            handler(
                tableName,
                fields,
                whereFields,
                WhereClause.SingleCondition(currentCond)
            )
        } else {
            handler(
                tableName,
                fields,
                whereFields,
                WhereClause.LogicalOperations(conditions)
            )
        }
    }

}