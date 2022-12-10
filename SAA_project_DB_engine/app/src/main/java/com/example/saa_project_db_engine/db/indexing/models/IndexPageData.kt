package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.db.base.PageData
import com.example.saa_project_db_engine.db.indexing.models.nodes.NodeType
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import org.apache.avro.generic.IndexedRecord
import java.nio.ByteBuffer

data class IndexPageData(
    override var id: Int,
    override var previousPageId: Int,
    override var nextPageId: Int,
    var nodeType: NodeType,
    override var records: MutableList<KeyValue>,
) : PageData<KeyValue>() {

    companion object {
        fun fromBytes(bytes: ByteBuffer): IndexPageData {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            val id = record.get("id") as Int
            val previousPageId = record.get("previousPageId") as Int
            val nextPageId = record.get("nextPageId") as Int
            var nodeTypeString = record.get("nodeType")
            if (nodeTypeString is org.apache.avro.util.Utf8) { // guaranteed
                nodeTypeString = nodeTypeString.toString()
            }
            val nodeType = NodeType.fromString(nodeTypeString as String)
            val records = record.get("records") as MutableList<*>
            val mapped = records.map {
                val indexedRecord = it as IndexedRecord
                val row =
                    KeyValue(indexedRecord.get(1) as ByteBuffer, indexedRecord.get(0) as ByteBuffer)
                row
            }.toMutableList()
            return IndexPageData(
                id,
                previousPageId,
                nextPageId,
                nodeType,
                mapped
            )
        }
    }

    override fun toBytes(): ByteBuffer {
        val record = GenericRecord(fileSchema)
        record.put("id", id)
        record.put("previousPageId", previousPageId)
        record.put("nextPageId", nextPageId)
        record.put("records", records)
        record.put("nodeType", nodeType.toString())
        return record.toByteBuffer()
    }
}