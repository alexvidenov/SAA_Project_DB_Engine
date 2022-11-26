package com.example.saa_project_db_engine.db.storage.models

import com.example.saa_project_db_engine.MAX_PAGE_SIZE
import com.example.saa_project_db_engine.avro.HeapPageData
import com.example.saa_project_db_engine.avro.TableRow
import com.example.saa_project_db_engine.db.PageData
import com.example.saa_project_db_engine.db.PageFullException
import com.example.saa_project_db_engine.db.PageInsertingMinimumException
import com.example.saa_project_db_engine.db.storage.models.extensions.getValue
import com.example.saa_project_db_engine.db.storage.models.extensions.setValue
import com.example.saa_project_db_engine.db.storage.models.extensions.toAvroBytesSize
import com.example.saa_project_db_engine.toLengthAvroByteSize
import java.nio.ByteBuffer

class HeapLogicalPage private constructor(
    private val data: HeapPageData
) : PageData() {
    companion object {
        private val emptyPageData = createHeapLogicalPageData(-1, mutableListOf())

        val overheadSize = HeapLogicalPage(emptyPageData).dump().limit()

        fun new(id: Int, initialRecords: MutableList<TableRow>): HeapLogicalPage {
            val data = createHeapLogicalPageData(id, initialRecords)
            return HeapLogicalPage(data)
        }

        fun load(byteBuffer: ByteBuffer): HeapLogicalPage {
            val data = HeapPageData.fromByteBuffer(byteBuffer)
            return HeapLogicalPage(data)
        }

        private fun createHeapLogicalPageData(id: Int, init: MutableList<TableRow>): HeapPageData {
            val builder = HeapPageData.newBuilder()
            builder.id = id
            builder.records = init
            builder.previousPageId = -1
            builder.nextPageId = -1
            return builder.build()
        }

    }

    override val id: Int by data
    override var previousId: Int? by data
    override var nextId: Int? by data

    val records: MutableList<TableRow> get() = data.records!!
    val size: Int get() = byteSize

    private var byteSize = dump().limit()

    fun dump(): ByteBuffer = data.toByteBuffer()

    fun insert(index: Int, row: TableRow) {
        if (index == 0 && previousId != null) throw PageInsertingMinimumException("")
        val newByteSize = calcPageSize(row.toAvroBytesSize(), 1)
        data.records?.add(index, row)
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
    }

    fun update(index: Int, newKeyValue: TableRow) {
        val oldKeyValue = data.records?.get(index)
        val newByteSize =
            calcPageSize(newKeyValue.toAvroBytesSize() - oldKeyValue!!.toAvroBytesSize())
        data.records?.set(index, newKeyValue)
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
    }

    fun delete(index: Int): TableRow {
        val keyValue = data.records?.get(index)
        val newByteSize = calcPageSize(-keyValue?.toAvroBytesSize()!!, -1)
        data.records?.removeAt(index)
        byteSize = newByteSize
        return keyValue
    }

    private fun calcPageSize(changingBytes: Int, changingLength: Int = 0): Int {
        return byteSize + changingBytes + calcChangingLengthBytes(changingLength)
    }

    private fun calcChangingLengthBytes(changingLength: Int): Int {
        if (changingLength == 0) return 0
        val newLength = records.size + changingLength
        return newLength.toLengthAvroByteSize() - records.size.toLengthAvroByteSize()
    }

}
