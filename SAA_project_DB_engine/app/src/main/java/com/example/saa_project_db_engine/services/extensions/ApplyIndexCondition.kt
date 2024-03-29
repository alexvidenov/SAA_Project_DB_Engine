package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexRecord
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.parsers.models.Operator
import com.example.saa_project_db_engine.parsers.models.WhereClauseType
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
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

fun TableService.applyIndexCondition(
    tableName: String,
    condition: WhereClauseType.Condition
): IndexValues? {
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
            val res = tree.get(indexRecord)
            val filtered = tree.scan().filter {
                if (res != null) {
                    return@filter it.key != res.key
                }
                return@filter true
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
            records = tree.scan(startKey = record.toByteBuffer(), allowEqual = true)
        }
        Operator.Lte -> {
            records = tree.scan(endKey = record.toByteBuffer(), allowEqual = true)
        }
        else -> {}
    }
    return sequenceToIndexValues(records)
}

private fun sequenceToIndexValues(records: Sequence<IndexRecord>): IndexValues? {
    val indexValuesReturn = mutableListOf<IndexValue>()
    records.forEach {
        if (it.value.capacity() != 0) { // in case scan actually performs full scan returning empty buffer
            val indexValues = IndexValues.fromBytes(it.value)
            indexValuesReturn.addAll(indexValues.records)
        }
    }
    return if (indexValuesReturn.isNotEmpty()) {
        IndexValues(indexValuesReturn)
    } else null
}