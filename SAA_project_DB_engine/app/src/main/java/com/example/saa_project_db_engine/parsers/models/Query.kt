package com.example.saa_project_db_engine.parsers.models

import android.drm.DrmStore.RightsStatus

enum class QueryType {
    Undefined, CreateTable, DropTable, ListTables, TableInfo, CreateIndex, DropIndex, Insert, Select, Update, Delete
}

enum class Operator {
    Undefined, Eq, Ne, Gt, Lt, Gte, Lte,
}

enum class ConditionType {
    Undefined, LITERAL, FIELD
}

// thing And (THING OR THING) OR NOT THING

// Select Id from Sample WHERE Id > 5 AND Name == "TEST" AND NOT BirthDate < "12.02.2002"

enum class LogicalOperator {
    AND, OR, NOT,
}

// thing or thing and thing

// where not name > 5

// WHERE (name > 5 AND name > 5) OR NOT (name > 5 AND name > 5) AND (name > 5 AND name > 5)

// WHERE NOT name > 5 AND birthdate == "21.12" AND Id == 6

/*
    After WHERE: (, NOT, expr
    ( -> parse subexpr (wherefield step, operator step, etc). After ) encountered -> save subexpr in query

    NOT ->  parse subexpr.

    expr -> parse expr -> save it as WhereClauseType.Condition (current cond field)
     and upon seeing logical keyword after it (AND / OR), promote it to WhereClauseType.LogicalOperation and add
     to array.

     If AND sees empty where clause array, assign both its nodes. Later AND's will only have right nodes.
     In executing, the current result operation will be cached and and's with only right nodes will use the result from
     the last operation.

    After that perform precedence traversal and upon seeing AND, check if previous operator is OR and if it is,
       assign the OR's right node as the AND's left node. Update the OR to remove its right node. (if precedence)
       Upon executing, the OR's right operand will be the result of the next item in the operator array.

    Select Name, DateBirth FROM Sample WHERE Id == 5 AND DateBirth > “01.01.2000”.

    Parse either full subexpr (thing) or just expr. Track both.

    (
 */

sealed class WhereClauseType {
    data class LogicalOperation(
        var operator: LogicalOperator? = null,
        var leftNode: Condition? = null, // always null in case of NOT
        var rightNode: Condition? = null,
        var leftSubExpr: MutableList<LogicalOperation>? = null,
        var rightSubExpr: MutableList<LogicalOperation>? = null,
        var hasPrecedence: Boolean = false
    ) : WhereClauseType()

    data class Condition(
        var operand1: String = "",
        var operand1Type: ConditionType = ConditionType.Undefined,
        var operator: Operator = Operator.Undefined,
        var operand2: String = "",
        var operand2Type: ConditionType = ConditionType.Undefined,
    ) : WhereClauseType()

}


fun logicalOperationInit() {
//    cond1 &&
//            AND OR AND
//    var yielded = record.get("op1") >= op2 || record.get("op1") <= op2
//    yielded && nextThing
}

data class FieldSchemaDefinition(
    var name: String = "",
    var type: String = "",
    var default: String? = null
)

data class Query(
    var type: QueryType = QueryType.Undefined,
    var table: String = "",
    var updates: MutableMap<String, String> = mutableMapOf(),
    var inserts: MutableList<MutableList<String>> = mutableListOf(),
    var fields: MutableList<String> = mutableListOf(),
    var schema: MutableList<FieldSchemaDefinition> = mutableListOf(),
    var currentCond: WhereClauseType.Condition = WhereClauseType.Condition(),
    var isParsingSubExpr: Boolean = false,
    var operations: MutableList<WhereClauseType.LogicalOperation> = mutableListOf(),
    var currentSubExprOperations: MutableList<WhereClauseType.LogicalOperation>? = null,
    var err: String? = null
)