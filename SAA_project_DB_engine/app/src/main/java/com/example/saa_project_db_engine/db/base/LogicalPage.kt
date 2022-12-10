package com.example.saa_project_db_engine.db.base

import android.util.Log
import com.example.saa_project_db_engine.MAX_PAGE_SIZE
import com.example.saa_project_db_engine.db.PageFullException
import com.example.saa_project_db_engine.toLengthAvroByteSize
import java.nio.ByteBuffer

abstract class LogicalPage<T : WithByteUtils, D : PageData<T>> protected constructor(val data: D) {

    abstract val overheadSize: Int

    val records: MutableList<T>
        get() = data.records

    val id: Int
        get() = data.id

    var previousId: Int
        get() = data.previousPageId
        set(value) {
            data.previousPageId = value
        }

    var nextId: Int
        get() = data.nextPageId
        set(value) {
            data.nextPageId = value
        }

    private var byteSize = dump().limit()

    fun dump(): ByteBuffer = data.toBytes()

    val size: Int get() = byteSize

    fun insert(page: T, index: Int? = null): Int {
        val newByteSize = calcPageSize(page.toAvroBytesSize(), 1)
        if (index != null) {
            records.add(index, page)
        } else {
            records.add(page)
        }
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
        return records.size - 1
    }

    fun update(index: Int, newKeyValue: T) {
        val oldKeyValue = records[index]
        val newByteSize =
            calcPageSize(newKeyValue.toAvroBytesSize() - oldKeyValue.toAvroBytesSize())
        records[index] = newKeyValue
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
    }

    fun delete(index: Int, old: T? = null): T {
        val keyValue = records[index]
        val newByteSize = calcPageSize(-keyValue.toAvroBytesSize(), -1)
        records[index] = old?.empty() as T // I am the one who checks, don't listen to the linter
//        data.records.removeAt(index)
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