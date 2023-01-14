package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.parsers.models.Operator
import com.example.saa_project_db_engine.parsers.models.WhereClauseType
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import java.nio.ByteBuffer

fun TableService.applyBoundedIndexScanCondition(
    operandName: String,
    lower: ByteBuffer,
    upper: ByteBuffer
): IndexValues? {
    val data = managerPool[operandName]!!
    val indexData = data.indexes[operandName]!!
    val records = indexData.tree.scan(lower, upper)
    return sequenceToIndexValues(records)
}

data class IndexRecordAndIndexName(val name: String, val recordKey: ByteBuffer)

fun TableService.applyIndexCondition(
    tableName: String,
    condition: WhereClauseType.Condition
): Pair<IndexRecordAndIndexName, IndexValues?> {
    val data = managerPool[tableName]!!
    val operand1 = condition.operand1
    val operand2 = condition.operand2

    val indexData = data.indexes[operand1]!!
    val schema = indexData.indexSchema

    Log.d("TEST", "OPERAND1: $operand1")
    Log.d("TEST", "OPERAND2: $operand2")
    val record =
        GenericRecord(schema)
    Log.d(
        "TEST",
        "OP1: $operand1, converted: ${convertOperandToNativeType(operand2, operand1, record)}"
    )
    record.put(
        operand1,
        convertOperandToNativeType(operand2, operand1, record)
    )
    val indexRecord = IndexRecord(record.toByteBuffer(), ByteBuffer.allocate(0))
    val tree = indexData.tree

    var records: Sequence<IndexRecord> = sequenceOf()
    when (condition.operator) {
        Operator.Eq -> {
            val res = tree.get(indexRecord)
            if (res != null) {
                records = sequenceOf(res)
            }
        }
        Operator.Ne -> {
            // full index scan with a filter.. imagine using this shit
            val res = tree.get(indexRecord)
            val filtered = tree.scan().filter {
                if (res != null) {
                    it.key === res.key
                }
                true
            }
            records = filtered
        }
        Operator.Gt -> {
            records = tree.scan(startKey = record.toByteBuffer())
        }
        Operator.Lt -> {
            records = tree.scan(endKey = record.toByteBuffer())
        }
        Operator.Gte -> {
            records = tree.scan(startKey = record.toByteBuffer())
        }
        Operator.Lte -> {
            records = tree.scan(endKey = record.toByteBuffer())
        }
        else -> {}
    }
    records.forEach {
        val record2 =
            GenericRecord(schema)
        record2.load(it.key)
        val values = IndexValues.fromBytes(it.value)
        Log.d("TEST", "INDEX RECORD: $record") // {"Name": "IVAN"}
        Log.d(
            "TEST",
            "INDEX VALUE: ${values}"
        ) // IndexValues(records=[IndexValue(pageId=0, rowId=0), IndexValue(pageId=0, rowId=1), IndexValue(pageId=0, rowId=3), IndexValue(pageId=0, rowId=5)])
    }
    return Pair(
        IndexRecordAndIndexName(operand1, record.toByteBuffer()),
        sequenceToIndexValues(records)
    )
}

private fun sequenceToIndexValues(records: Sequence<IndexRecord>): IndexValues? {
    val indexValuesReturn = mutableListOf<IndexValue>()
    records.forEach {
        val indexValues = IndexValues.fromBytes(it.value)
        indexValuesReturn.addAll(indexValues.records)
    }
    return if (indexValuesReturn.isNotEmpty()) {
        IndexValues(indexValuesReturn)
    } else null
}