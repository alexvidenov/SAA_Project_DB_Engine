package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.KeyCompare
import com.example.saa_project_db_engine.db.indexing.models.BPlusTree
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.file.IndexFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.models.IndexData
import com.example.saa_project_db_engine.services.models.TableManagerData
import org.apache.avro.Schema
import java.io.File

fun TableService.loadTable(tableName: String) {
    if (!managerPool.contains(tableName)) {
        val file = File(dir, "$tableName.db")
        val schemaFile = File(dir, "$tableName.avsc")
        val fileManager = HeapFileManager.load(file)
        val heapPageManager = HeapPageManager(fileManager)
        managerPool[tableName] =
            TableManagerData(
                heapPageManager,
                org.apache.avro.Schema.Parser().parse(schemaFile),
                mutableMapOf()
            )
    }
}

fun TableService.loadIndex(tableName: String) {
    val indexManagers =
        initIndexManagersForTable(tableName)
    val data = managerPool[tableName]
    data!!.indexes = indexManagers
    managerPool[tableName] = data
}

fun TableService.initIndexManagersForTable(tableName: String): Map<String, IndexData> {
    val indexes = mutableMapOf<String, IndexPageManager>()
    val compares = mutableMapOf<String, KeyCompare>()
    val schemas = mutableMapOf<String, Schema>()
    val managers = mutableMapOf<String, IndexData>()

    getIndexFilesForTable(tableName).forEach { file ->
        when (file.extension) {
            "index" -> {
                val fileManager = IndexFileManager.load(file)
                val indexPageManager = IndexPageManager(fileManager)
                val indexFieldName =
                    file.name.substringBeforeLast(".").split("_").last()
                indexes[indexFieldName] = indexPageManager
            }
            "avsc" -> {
                val schema = Schema.Parser().parse(file)
                val record = GenericRecord(schema)
                val split = file.name.substringBeforeLast(".").split("_")
                val indexFieldName = split[split.size - 2]
                compares[indexFieldName] = record.keyCompare
                schemas[indexFieldName] = schema
            }
        }
    }
    indexes.entries.forEach {
        val key = it.key
        val compare = compares[key]
        val schema = schemas[key]
        val tree = BPlusTree(it.value, compare!!)
        managers[key] = IndexData(schema!!, tree)
    }
    return managers
}

fun TableService.getIndexFilesForTable(tableName: String): List<File> {
    return files.filter {
        it.startsWith("${tableName}_index")
    }.map {
        File(dir, it)
    }
}