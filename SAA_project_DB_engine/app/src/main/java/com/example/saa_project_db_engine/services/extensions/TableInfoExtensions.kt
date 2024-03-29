package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.models.TableInfo
import com.example.saa_project_db_engine.services.models.TableSchemaEntry
import java.io.File

fun TableService.getTableInfo(tableName: String): TableInfo {
    load(tableName)
    val data = managerPool[tableName]!!

    val heapSize = (File(dir, "$tableName.db").length() / 1024).toDouble()

    var indexesSize = 0.0

    getIndexFilesForTable(tableName).forEach {
        indexesSize += it.length()
    }

    val schemaEntries = mutableListOf<TableSchemaEntry>()

    data.tableSchema.fields.forEach {
        schemaEntries.add(TableSchemaEntry(it.name(), it.schema().type.getName()))
    }

    val recordsCount = data.heapPageManager.recordsCount

    return TableInfo(heapSize, indexesSize / 1024, recordsCount, schemaEntries)
}