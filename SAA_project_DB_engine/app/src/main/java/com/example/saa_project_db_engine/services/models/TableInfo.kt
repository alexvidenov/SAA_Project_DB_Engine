package com.example.saa_project_db_engine.services.models

data class TableInfo(
    val heapSize: Long,
    val indexSize: Long,
    val recordsCount: Int,
    val schema: List<TableSchemaEntry>
) {

}

data class TableSchemaEntry(val fieldName: String, val fieldType: String)
