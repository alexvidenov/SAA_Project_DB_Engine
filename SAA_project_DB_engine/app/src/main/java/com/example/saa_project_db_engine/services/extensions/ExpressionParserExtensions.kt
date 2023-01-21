package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.parsers.models.*
import com.example.saa_project_db_engine.serialization.GenericRecord
import com.example.saa_project_db_engine.services.TableService
import org.apache.avro.Schema

fun TableService.parseSubExpression(
    record: GenericRecord,
    ops: List<WhereClauseType.LogicalOperation>
): Boolean {
    var currentOpType: LogicalOperator?
    var currentCachedOperationResult = true
    var i = 0
    Log.d("EXPR", "operations: $ops")
    while (i <= ops.size - 1) {
        val op = ops[i]
        Log.d("EXPR", "operation: $op")
        currentOpType = op.operator
        val parseState = parseLogicalOperation(record, op)
        Log.d("EXPR", "Empty state: ${parseState.empty}")
        when (parseState.empty) {
            LogicalOperationParseResultEmpty.NONE -> { // only the init value in the logical foldr (from FP)
                i++
                currentCachedOperationResult = parseState.result
            }
            LogicalOperationParseResultEmpty.LEFT -> {
                i++
                currentCachedOperationResult = applyOperation(
                    currentCachedOperationResult,
                    parseState.result,
                    currentOpType!!
                )
            }
            LogicalOperationParseResultEmpty.RIGHT -> {
                val next = ops[i + 1]
                Log.d("EXPR", "operation after i + 1: $next")
                i += 2
                val rightNode = next.rightNode!!.copy()
                if (next.operator === LogicalOperator.NOT) {
                    rightNode.operator = invertOperator(next.rightNode!!.operator)
                }
                val copied = next.copy(rightNode = rightNode)
                val nextState = parseLogicalOperation(record, copied)
                currentCachedOperationResult = applyOperation(
                    parseState.result, // currentCachedOperationResult
                    nextState.result,
                    currentOpType!!
                )
            }
        }
        Log.d("EXPR", "currentCachedOperationResult: $currentCachedOperationResult")
    }
    return currentCachedOperationResult
}

fun TableService.parseLogicalOperation(
    record: GenericRecord,
    op: WhereClauseType.LogicalOperation
): LogicalOperationParseResult {
    var left: Boolean? = null
    var right: Boolean? = null
    op.leftNode?.let {
        left = parseCondition(record, it)
        Log.d("EXPR", "parsed left node: $left")
    }
    op.rightNode?.let {
        right = parseCondition(record, it)
        Log.d("EXPR", "parsed right node: $right")
    }
    op.leftSubExpr?.let {
        left = parseSubExpression(record, it)
        Log.d("EXPR", "parsed left subexpr: $left")
    }
    op.rightSubExpr?.let {
        right = parseSubExpression(record, it)
        Log.d("EXPR", "parsed right subexpr: $right")
    }

    return if (left != null && right != null) {
        val res = applyOperation(left, right, op.operator!!)
        Log.d("EXPR", "after applying operation: $res")
        LogicalOperationParseResult(res, LogicalOperationParseResultEmpty.NONE)
    } else {
        if (left == null) {
            LogicalOperationParseResult(right!!, LogicalOperationParseResultEmpty.LEFT)
        } else {
            LogicalOperationParseResult(left!!, LogicalOperationParseResultEmpty.RIGHT)
        }
    }
}

private fun applyOperation(op1: Boolean?, op2: Boolean?, operator: LogicalOperator): Boolean {
    Log.d("EXPR", "applyOperation: ${op1} ${op2} ${operator}")
    return when (operator) {
        LogicalOperator.AND -> op1!! && op2!!
        LogicalOperator.OR -> op1!! || op2!!
        LogicalOperator.NOT -> !op2!!
    }
}

fun parseCondition(record: GenericRecord, cond: WhereClauseType.Condition): Boolean {
    var op1: Any? = null
    var op2: Any? = null
    var typeToConvert: Schema.Type? = null
    var res = false
    Log.d("EXPR", "PARSE CONDITION: $cond")
    when (cond.operand1Type) {
        ConditionType.LITERAL -> {
            op1 = cond.operand1
        }
        ConditionType.FIELD -> {
            op1 = record.get(cond.operand1)
            if (op1 is org.apache.avro.util.Utf8) {
                op1 = op1.toString()
            }
            typeToConvert = fieldType(record, cond.operand1)
        }
        else -> {}
    }
    when (cond.operand2Type) {
        ConditionType.LITERAL -> {
            op2 = cond.operand2
            op2 = convertStringToNativeType(op2, typeToConvert!!)
        }
        ConditionType.FIELD -> {
            op2 = record.get(cond.operand2)
        }
        else -> {}
    }
    Log.d(
        "EXPR",
        "CONDITION: \n$op1\n$op2\n${cond.operator} $op1 ${op1!!.javaClass.name} $op2 ${op2!!.javaClass.name}"
    )
    op1 = (op1 as Comparable<Any>)
    op2 = (op2 as Comparable<*>)
    when (cond.operator) {
        Operator.Eq -> res = op1 == op2
        Operator.Ne -> res = op1 != op2
        Operator.Gt -> res = op1 > op2
        Operator.Lt -> res = op1 < op2
        Operator.Gte -> res = op1 >= op2
        Operator.Lte -> res = op1 <= op2
        else -> {}
    }
    return res
}