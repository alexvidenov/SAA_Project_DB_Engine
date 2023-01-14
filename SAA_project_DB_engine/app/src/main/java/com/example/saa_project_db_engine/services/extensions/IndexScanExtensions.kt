package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.parsers.models.LogicalOperator
import com.example.saa_project_db_engine.parsers.models.Operator
import com.example.saa_project_db_engine.parsers.models.WhereClauseType
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.consistency.IndexConsistencyService
import com.example.saa_project_db_engine.services.models.WhereClause
import java.nio.ByteBuffer

/*
    AND -> return only the rows that exist in both index scan sets
    OR -> just merge the two record sets from index scan

 */

// TODO: Keep cache and index in sync after update and delete.
// get page id and row id, and evict these. call them affected fields

fun TableService.indexScan(tableName: String, clause: WhereClause): IndexValues? {
    val data = managerPool[tableName]!!
    when (clause) {
        is WhereClause.LogicalOperations -> {
            val op = clause.ops.first()
            val leftCond = op.leftNode
            val rightCond = op.rightNode
            when (op.operator) {
                LogicalOperator.AND -> {
                    val leftOperand1 = leftCond!!.operand1
                    val rightOperand1 = rightCond!!.operand1
                    var lower: ByteBuffer = ByteBuffer.allocate(0)
                    var upper: ByteBuffer = ByteBuffer.allocate(0)
                    var shouldUseBoundedScan = true
                    if (leftOperand1 == rightOperand1) {
                        val indexData = data.indexes[leftOperand1]
                        val record = GenericRecord(indexData!!.indexSchema)
                        record.put(
                            leftOperand1,
                            convertOperandToNativeType(leftCond.operand2, leftOperand1, record)
                        )
                        when (leftCond.operator) {
                            Operator.Gt -> lower = record.toByteBuffer()
                            Operator.Lt -> upper = record.toByteBuffer()
                            Operator.Gte -> lower = record.toByteBuffer()
                            Operator.Lte -> upper = record.toByteBuffer()
                            else -> {
                                shouldUseBoundedScan = false
                            }
                        }
                        when (rightCond.operator) {
                            Operator.Gt -> lower = record.toByteBuffer()
                            Operator.Lt -> upper = record.toByteBuffer()
                            Operator.Gte -> lower = record.toByteBuffer()
                            Operator.Lte -> upper = record.toByteBuffer()
                            else -> {
                                shouldUseBoundedScan = false
                            }
                        }
                        return if (shouldUseBoundedScan) {
                            applyBoundedIndexScanCondition(leftOperand1, lower, upper)
                        } else {
                            logicalAndIndexHandler(tableName, leftCond, rightCond)
                        }
                    } else {
                        return logicalAndIndexHandler(tableName, leftCond, rightCond)
                    }
                }
                LogicalOperator.OR -> {
                    return logicalOrIndexHandler(tableName, leftCond, rightCond)
                }
                LogicalOperator.NOT -> {

                }
                else -> {}
            }
        }
        is WhereClause.SingleCondition -> {
            val res = applyIndexCondition(tableName, clause.cond)
            val indexValues = res.second
            if (indexValues != null) {
                IndexConsistencyService.addAffectedFieldEntry(
                    res.first.name,
                    Pair(res.first.recordKey, indexValues)
                )
            }
            return res.second
        }
    }
    return null
}

fun TableService.logicalAndIndexHandler(
    tableName: String,
    leftCond: WhereClauseType.Condition?,
    rightCond: WhereClauseType.Condition?
): IndexValues? {
    val leftPair = applyIndexCondition(tableName, leftCond!!)
    val rightPair = applyIndexCondition(tableName, rightCond!!)
    val leftRes = leftPair.second
    val rightRes = rightPair.second
    Log.d("TEST", "leftRes.records: ${leftRes?.records}")
    Log.d("TEST", "rightRes.records: ${rightRes?.records}")
    return if (leftRes != null && rightRes != null) {
        val filteredRecords = mutableListOf<IndexValue>()
        // CHECK SIZE or left and right and iterate first over the one with bigger size (otherwise we miss)
        leftRes.records.forEach {
            if (rightRes.records.contains(it)) {
                filteredRecords.add(it)
            }
        }
        Log.d("TEST", "filtered: ${filteredRecords}")
        val indexValues = IndexValues(filteredRecords)
        IndexConsistencyService.addAffectedFieldEntry(
            leftPair.first.name,
            Pair(leftPair.first.recordKey, indexValues)
        )
        IndexConsistencyService.addAffectedFieldEntry(
            rightPair.first.name,
            Pair(rightPair.first.recordKey, indexValues)
        )
        indexValues
    } else null
}

fun TableService.logicalOrIndexHandler(
    tableName: String,
    leftCond: WhereClauseType.Condition?,
    rightCond: WhereClauseType.Condition?
): IndexValues? {
    val indexScanLeftNodePair = applyIndexCondition(tableName, leftCond!!)
    val indexScanRightNodePair = applyIndexCondition(tableName, rightCond!!)
    val indexScanLeftNodeRes = indexScanLeftNodePair.second
    val indexScanRightNodeRes = indexScanRightNodePair.second
    val filteredRecords = mutableListOf<IndexValue>()
    indexScanLeftNodeRes?.records?.forEach {
        filteredRecords.add(it)
    }
    indexScanRightNodeRes?.records?.forEach {
        filteredRecords.add(it)
    }
    if (filteredRecords.isEmpty()) {
        return null
    }
    val indexValues = IndexValues(filteredRecords)
    IndexConsistencyService.addAffectedFieldEntry(
        indexScanLeftNodePair.first.name,
        Pair(indexScanLeftNodePair.first.recordKey, indexValues)
    )
    IndexConsistencyService.addAffectedFieldEntry(
        indexScanRightNodePair.first.name,
        Pair(indexScanRightNodePair.first.recordKey, indexValues)
    )
    return indexValues
}