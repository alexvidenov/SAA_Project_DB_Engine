package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.db.base.LogicalPage
import java.nio.ByteBuffer

class HeapLogicalPage private constructor(
    data: HeapPageData
) : LogicalPage<TableRow, HeapPageData>(data) {
    companion object {
        private val emptyPageData = createHeapLogicalPageData(-1, mutableListOf())

        val overheadSize = HeapLogicalPage(emptyPageData).dump().limit()

        fun new(id: Int, initialRecords: MutableList<TableRow>): HeapLogicalPage {
            val data = createHeapLogicalPageData(id, initialRecords)
            return HeapLogicalPage(data)
        }

        fun load(byteBuffer: ByteBuffer): HeapLogicalPage {
            val data = HeapPageData.fromBytes(byteBuffer)
            return HeapLogicalPage(data)
        }

        private fun createHeapLogicalPageData(id: Int, init: MutableList<TableRow>): HeapPageData {
            return HeapPageData(id, -1, -1, init)
        }

    }

    override val overheadSize: Int
        get() = HeapLogicalPage.overheadSize

}
