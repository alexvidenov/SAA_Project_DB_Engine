package com.example.saa_project_db_engine.db.indexing.models

import android.util.Log
import com.example.saa_project_db_engine.db.base.LogicalPage
import com.example.saa_project_db_engine.db.indexing.models.nodes.NodeType
import java.nio.ByteBuffer

class IndexLogicalPage private constructor(
    data: IndexPageData
) : LogicalPage<KeyValue, IndexPageData>(data) {

    companion object {

        private val emptyPageData =
            createIndexLogicalPageData(-1, NodeType.LeafNode, mutableListOf())

        val overheadSize = IndexLogicalPage(emptyPageData).dump().limit()

        fun new(
            id: Int,
            nodeType: NodeType,
            initialRecords: MutableList<KeyValue>
        ): IndexLogicalPage {
            Log.d("TEST", "INDEX DATA INIT")
            val data = createIndexLogicalPageData(id, nodeType, initialRecords)
            Log.d("TEST", "INDEX DATA IS ${data}")
            return IndexLogicalPage(data)
        }

        fun load(byteBuffer: ByteBuffer): IndexLogicalPage {
            val data = IndexPageData.fromBytes(byteBuffer)
            return IndexLogicalPage(data)
        }

        private fun createIndexLogicalPageData(
            id: Int,
            nodeType: NodeType,
            init: MutableList<KeyValue>
        ): IndexPageData {
            return IndexPageData(id, -1, -1, nodeType, init)
        }
    }

    override val overheadSize: Int
        get() = IndexLogicalPage.overheadSize
}