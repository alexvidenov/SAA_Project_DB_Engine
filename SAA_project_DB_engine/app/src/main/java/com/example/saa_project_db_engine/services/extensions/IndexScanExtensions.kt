package com.example.saa_project_db_engine.services.extensions

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
    for the sake of simplicity, I won't write a goddamn query analyser to optimise usage of indexes in complex
    queries with subexpressions. Way the fuck out of scope. Next time delve into the topic when you create assignments.

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
                            else -> {}
                        }
                        return applyBoundedIndexScanCondition(leftOperand1, lower, upper)
                    } else {
                        val leftRes = applyIndexCondition(tableName, leftCond)
                        val rightRes = applyIndexCondition(tableName, leftCond)
                        return if (leftRes != null && rightRes != null) {
                            val filteredRecords = mutableListOf<IndexValue>()
                            leftRes.records.forEach {
                                if (rightRes.records.contains(it)) {
                                    filteredRecords.add(it)
                                }
                            }
                            IndexValues(filteredRecords)
                        } else null
                    }
                }
                LogicalOperator.OR -> {
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
                LogicalOperator.NOT -> {

                }
                else -> {}
            }
        }
        is WhereClause.SingleCondition -> return applyIndexCondition(tableName, clause.cond)
    }
    return null
}
