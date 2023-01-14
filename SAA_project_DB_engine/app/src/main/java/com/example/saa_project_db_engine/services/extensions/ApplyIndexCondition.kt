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
    IndexConsistencyService.addAffectedFieldEntry() // add the operandName and the index record
    val records = indexData.tree.scan(lower, upper)
    records.toList() // use
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

    // have lateinit res here and update the global consistency with it
    return when (condition.operator) {
        Operator.Eq -> {
            val res = tree.get(indexRecord)
            if (res != null) {
                return IndexValues.fromBytes(res.value)
            }
            null
        }
        Operator.Ne -> {
            // full index scan with a filter.. kms
            val res = tree.get(indexRecord)
            val filtered = tree.scan().filter {
                if (res != null) {
                    it.key === res.key
                }
                true
            }
            sequenceToIndexValues(filtered)
        }
        Operator.Gt -> {
            val res = tree.scan(startKey = record.toByteBuffer())
            sequenceToIndexValues(res)
        }
        Operator.Lt -> {
            val res = tree.scan(endKey = record.toByteBuffer())
            sequenceToIndexValues(res)
        }
        Operator.Gte -> {
            val res = tree.scan(startKey = record.toByteBuffer())
            sequenceToIndexValues(res)
        }
        Operator.Lte -> {
            val res = tree.scan(endKey = record.toByteBuffer())
            sequenceToIndexValues(res)
        }
        else -> null
    }
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