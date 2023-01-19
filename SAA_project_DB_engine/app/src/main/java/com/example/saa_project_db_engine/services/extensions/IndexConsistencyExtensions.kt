package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.db.indexing.models.BPlusTree
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.storage.models.TableRow
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.handlers.OldNewPair
import java.nio.ByteBuffer

data class RowWithPageId(val row: TableRow, val pageId: Int)

fun TableService.applyConsistencyOnDelete(tableName: String, deletedRows: List<RowWithPageId>) {
    forEachIndex(tableName, deletedRows) { idxRecord, idxValue, tree, _ ->
        tree.delete(
            IndexRecord(idxRecord.toByteBuffer(), ByteBuffer.allocate(0)),
            idxValue.pageId,
            idxValue.rowId
        )
    }
}

fun TableService.applyConsistencyOnInsert(tableName: String, insertedRows: List<RowWithPageId>) {
    forEachIndex(tableName, insertedRows) { idxRecord, idxValue, tree, _ ->
        tree.put(
            IndexRecord(idxRecord.toByteBuffer(), idxValue.toBytes()),
        )
    }
}

fun TableService.applyConsistencyOnUpdate(tableName: String, oldNewPairList: List<OldNewPair>) {
    applyConsistencyOnDelete(tableName, oldNewPairList.map { it.old })
    applyConsistencyOnInsert(tableName, oldNewPairList.map { it.new })
}

private fun TableService.forEachIndex(
    tableName: String,
    affectedRows: List<RowWithPageId>,
    cbk: (idxRecord: GenericRecord, idxValue: IndexValue, tree: BPlusTree, i: Int) -> Unit
) {
    load(tableName)
    val data = managerPool[tableName]!!
    val indexes = data.indexes
    indexes.forEach { idx ->
        val indexGenericRecord =
            GenericRecord(idx.value.indexSchema)
        val rowGenericRecord = GenericRecord(data.tableSchema)

        val tree = idx.value.tree

        affectedRows.forEachIndexed { i, affRow ->
            rowGenericRecord.load(affRow.row.value)
            val indexFieldValueFromRow = rowGenericRecord.get(idx.key)

            indexGenericRecord.put(idx.key, indexFieldValueFromRow)
            val indexValue = IndexValue(affRow.pageId, affRow.row.rowId!!)

            cbk(indexGenericRecord, indexValue, tree, i)
        }
    }
}