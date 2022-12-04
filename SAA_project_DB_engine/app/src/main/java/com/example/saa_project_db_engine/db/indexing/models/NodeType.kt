package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.SchemasServiceLocator
import java.nio.ByteBuffer

enum class NodeType {
    RootNode, InternalNode, LeafNode;

    private val className = this::class.java.simpleName

    companion object {
        fun fromBytes(bytes: ByteBuffer): NodeType {
            val schema =
                SchemasServiceLocator.getSchemaFor(this::class.java.declaringClass.simpleName)
            val record = GenericRecord(schema)
            record.load(bytes)
            return when (record.get("nodeType")) {
                "RootNode" -> RootNode
                "InternalNode" -> InternalNode
                "LeafNode" -> LeafNode
                else -> RootNode
            }
        }

        fun fromString(string: String): NodeType {
            return when (string) {
                "RootNode" -> RootNode
                "InternalNode" -> InternalNode
                "LeafNode" -> LeafNode
                else -> RootNode
            }
        }
    }

    fun toBytes(): ByteBuffer {
        val schema =
            SchemasServiceLocator.getSchemaFor(className)
        val record = GenericRecord(schema)
        var stringRepresentation: String = ""
        stringRepresentation = when (this) {
            RootNode -> "RootNode"
            InternalNode -> "InternalNode"
            LeafNode -> "LeafNode"
        }
        record.put("nodeType", stringRepresentation)
        return record.toByteBuffer()
    }

    override fun toString(): String {
        return when (this) {
            RootNode -> "RootNode"
            InternalNode -> "InternalNode"
            LeafNode -> "LeafNode"
        }
    }
}

