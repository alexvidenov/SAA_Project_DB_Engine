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
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import com.example.saa_project_db_engine.services.extensions.*
import com.example.saa_project_db_engine.services.models.WhereClause
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow

class SchemaExecutor constructor(ctx: Context) {
    private val tableService = TableService(ctx)
    private val parser = StatementParser()

    private val _state: MutableStateFlow<SelectResultModel> = MutableStateFlow(
        SelectResultModel()
    )
    val state: StateFlow<SelectResultModel>
        get() = _state

    // here emit all the "Updated 56 rows. Deleted 56 rwos, etc".
//    private val _events = MutableSharedFlow<>()

    fun execute(raw: String) {
        val parsed = parser.parseQuery(raw)
        Log.d("TEST", "PARSED QUERY: $parsed")
        IndexConsistencyService.clear()
        executeInternal(parsed)
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
                val tables = tableService.listTables()
                Log.d("TEST", "tables :$tables")
            }
            QueryType.TableInfo -> {
                val tableInfo = tableService.getTableInfo(query.table)
                Log.d("TEST", "TABLE INFO: $tableInfo")
            }
            QueryType.Insert -> {
                tableService.insertRows(query.table, query.fields, query.inserts)
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
                        query.orderByField,
                        query.distinct
                    )
                }
                _state.value = res
            }
            QueryType.Update -> {
                handleGeneralCrudOperation(
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
            }
            QueryType.Delete -> {
                handleGeneralCrudOperation(
                    query.table,
                    query.fields,
                    query.whereFields,
                    query.operations,
                    query.currentCond, tableService::delete
                )
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