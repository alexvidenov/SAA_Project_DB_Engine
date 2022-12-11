package com.example.saa_project_db_engine.services.models

import com.example.saa_project_db_engine.db.indexing.models.BPlusTree
import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import org.apache.avro.Schema

data class IndexData(val indexSchema: Schema, val tree: BPlusTree)

data class TableManagerData(
    val heapPageManager: HeapPageManager,
    val tableSchema: Schema,
    var indexes: Map<String, IndexData> = mapOf()
)