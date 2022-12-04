package com.example.saa_project_db_engine.db.indexing.models

import com.example.saa_project_db_engine.db.base.LogicalPage
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.HeapPageData
import com.example.saa_project_db_engine.db.storage.models.TableRow
import java.nio.ByteBuffer

class IndexLogicalPage private constructor(
    val data: IndexPageData
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
            val data = createIndexLogicalPageData(id, nodeType, initialRecords)
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