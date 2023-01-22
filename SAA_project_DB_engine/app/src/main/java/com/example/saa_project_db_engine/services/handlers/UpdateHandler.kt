package com.example.saa_project_db_engine.services.handlers

import com.example.saa_project_db_engine.db.managers.page.HeapPageManager
import com.example.saa_project_db_engine.db.storage.models.HeapLogicalPage
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.extensions.RowWithPageId
import com.example.saa_project_db_engine.services.extensions.convertOperandToNativeType
import org.apache.avro.Schema

data class OldNewPair(val old: RowWithPageId, val new: RowWithPageId)

class UpdateHandler(private val schema: Schema, private val updates: Map<String, String>) :
    QueryTypeHandlerOnSelect {
    val oldNewPairList = mutableListOf<OldNewPair>()
    var rowsUpdated: Int = 0

    override fun handle(
        manager: HeapPageManager,
        page: HeapLogicalPage,
        index: Int,
        row: TableRow
    ) {
        val record =
            GenericRecord(schema)
        record.load(row.value)
        updates.forEach {
            record.put(it.key, convertOperandToNativeType(it.value, it.key, record))
        }
        val newRow = TableRow(record.toByteBuffer(), row.rowId)
        oldNewPairList.add(OldNewPair(RowWithPageId(row, page.id), RowWithPageId(newRow, page.id)))
        page.update(index, newRow) // TODO: handle overflow
        rowsUpdated++
    }

    override fun cleanup() {
    }

}