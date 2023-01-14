package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.db.indexing.models.*
import com.example.saa_project_db_engine.db.managers.file.HeapFileManager
import com.example.saa_project_db_engine.db.managers.file.IndexFileManager
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.managers.page.IndexPageManager
import com.example.saa_project_db_engine.db.managers.page.forEachRowPageIndexed
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.models.TableManagerData
import org.apache.avro.Schema
import java.io.File

fun TableService.createTable(tableName: String, schemaDefinition: String) {
    val schemaFile = File(dir, "$tableName.avsc")
    schemaFile.createNewFile()
    schemaFile.writeText(schemaDefinition)

    val dbFile = File(dir, "$tableName.db")
    dbFile.createNewFile()

    val heapManager = HeapFileManager.new(dbFile)
    val pageManager = HeapPageManager(heapManager)

    managerPool[tableName] =
        TableManagerData(pageManager, Schema.Parser().parse(schemaFile), mapOf())
}

fun TableService.createIndex(tableName: String, indexName: String, fieldName: String) {
    load(tableName)
    val data = managerPool[tableName]!!

    val tableSchema = data.tableSchema

    val indexFile = File(dir, "${tableName}_index_${indexName}_$fieldName.index")
    indexFile.createNewFile()

    val schemaRepresentation = buildIndexSchemaRepresentation(tableName, fieldName, tableSchema)

    val fieldSchemaFile = File(dir, "${tableName}_index_${indexName}_${fieldName}_schema.avsc")
    fieldSchemaFile.createNewFile()
    fieldSchemaFile.writeText(schemaRepresentation)

    val fieldSchema = Schema.Parser().parse(fieldSchemaFile)

    val indexFieldSchema = Schema.Parser().parse(fieldSchemaFile)
    var indexFieldRecord = GenericRecord(fieldSchema)
    val indexFieldKeyCompare = indexFieldRecord.keyCompare

    val indexFileManager = IndexFileManager.new(indexFile)
    val indexPageManager = IndexPageManager(indexFileManager)

    val tree = BPlusTree(indexPageManager, indexFieldKeyCompare)

    data.heapPageManager.forEachRowPageIndexed { row, pageId ->
        val tableRecord = GenericRecord(tableSchema)
        tableRecord.load(row.value)
        val key = tableRecord.get(fieldName)

        indexFieldRecord = GenericRecord(indexFieldSchema)
        indexFieldRecord.put(fieldName, key)

        val indexValue = IndexValue(pageId, row.rowId!!)

        val keyValue =
            KeyValue(
                indexFieldRecord.toByteBuffer(),
                indexValue.toBytes()
            )

        tree.put(IndexRecord(keyValue))
    }
}
