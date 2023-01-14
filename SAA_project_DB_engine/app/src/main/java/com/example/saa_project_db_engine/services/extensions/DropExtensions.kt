package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.services.TableService
import java.io.File

fun TableService.dropTable(tableName: String) {
    load(tableName)

    val schemaFile = File(dir, "$tableName.avsc")
    schemaFile.delete()

    val dbFile = File(dir, "$tableName.db")
    dbFile.delete()

    getIndexFilesForTable(tableName).forEach {
        it.delete()
    }
}

fun TableService.dropIndex(tableName: String, indexName: String) {
    files.filter {
        it.startsWith("${tableName}_index_${indexName}")
    }.map {
        File(dir, it)
    }.forEach {
        it.delete()
    }
}