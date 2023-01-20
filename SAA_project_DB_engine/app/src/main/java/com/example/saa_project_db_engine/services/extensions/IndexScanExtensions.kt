package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.db.indexing.models.IndexValue
import com.example.saa_project_db_engine.db.indexing.models.IndexValues
import com.example.saa_project_db_engine.parsers.models.LogicalOperator
import com.example.saa_project_db_engine.parsers.models.Operator
import com.example.saa_project_db_engine.parsers.models.WhereClauseType
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import com.example.saa_project_db_engine.services.models.WhereClause
import java.nio.ByteBuffer

/*
    AND -> return only the rows that exist in both index scan sets
    OR -> just merge the two record sets from index scan
 */

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
                    Log.d("TEST", "NOT BRO")
                    val invertedCond = rightCond!!
                    invertedCond.operator = invertOperator(invertedCond.operator)
                    val res = singleCond(tableName, invertedCond)
                    Log.d("TEST", "RES FROM NOT :${res}")
                    return res
                }
                else -> {}
            }
        }
        is WhereClause.SingleCondition -> {
            return singleCond(tableName, clause.cond)
        }
    }
    return null
}

fun TableService.logicalAndIndexHandler(
    tableName: String,
    leftCond: WhereClauseType.Condition?,
    rightCond: WhereClauseType.Condition?
): IndexValues? {
    val leftRes = applyIndexCondition(tableName, leftCond!!)
    val rightRes = applyIndexCondition(tableName, rightCond!!)
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
        indexValues
    } else null
}

fun TableService.logicalOrIndexHandler(
    tableName: String,
    leftCond: WhereClauseType.Condition?,
    rightCond: WhereClauseType.Condition?
): IndexValues? {
    val indexScanLeftNodeRes = applyIndexCondition(tableName, leftCond!!)
    val indexScanRightNodeRes = applyIndexCondition(tableName, rightCond!!)
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
    return IndexValues(filteredRecords)
}

fun TableService.singleCond(tableName: String, cond: WhereClauseType.Condition): IndexValues? {
    return applyIndexCondition(tableName, cond)
}