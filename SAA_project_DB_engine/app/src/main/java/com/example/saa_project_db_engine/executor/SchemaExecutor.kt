package com.example.saa_project_db_engine.executor

import android.content.Context
import android.util.Log
import com.example.saa_project_db_engine.parsers.StatementParser
import com.example.saa_project_db_engine.parsers.models.FieldSchemaDefinition
import com.example.saa_project_db_engine.parsers.models.Query
import com.example.saa_project_db_engine.parsers.models.QueryType
import com.example.saa_project_db_engine.services.TableService
import kotlin.math.log

class SchemaExecutor constructor(private val ctx: Context) {
    private val tableService = TableService(ctx)
    private val parser = StatementParser()

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
                handleSelect(query.table, query.fields)
            }
            QueryType.Update -> TODO()
            QueryType.Delete -> TODO()
            QueryType.CreateIndex -> TODO()
            QueryType.DropIndex -> TODO()
        }
    }

    private fun handleCreateTable(tableName: String, fields: List<FieldSchemaDefinition>) {
        val avroSchema = StringBuilder()
        var builder = avroSchema
            .append("{")
            .append("\"name\":")
            .append("\"${tableName}\"")
            .append(",")
            .append("\"type\": \"record\"")
            .append(",")
            .append("\"fields\": [")
        fields.forEachIndexed { i, it ->
            builder = builder
                .append("{\"name\":")
                .append("\"${it.name}\",")
                .append("\"type\": \"${it.type}\"")
            if (it.default != null) {
                var valueToAppend = it.default
                if (it.type == "string") {
                    valueToAppend = "\"${valueToAppend}\""
                }
                builder = builder.append(",\"default\": $valueToAppend")
            }
            builder = builder.append("}")
            if (i != fields.size - 1) {
                builder = builder.append(",")
            }
        }
        builder = builder.append("]}")
        val string = builder.toString()
        tableService.createTable(tableName, string)
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
    ) {
        tableService.select(tableName, fields)
    }

    fun test() {
        tableService.test()
    }
}