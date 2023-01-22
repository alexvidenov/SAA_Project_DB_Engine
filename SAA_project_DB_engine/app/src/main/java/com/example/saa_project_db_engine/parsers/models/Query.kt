package com.example.saa_project_db_engine.parsers.models

/*
    After WHERE: (, NOT, expr
    ( -> parse subexpr (wherefield step, operator step, etc). After ) encountered -> save subexpr in query

     NOT ->  parse subexpr.

     expr -> parse expr -> save it as WhereClauseType.Condition (current cond field)
     and upon seeing logical keyword after it (AND / OR), promote it to WhereClauseType.LogicalOperation and add
     to array.

     If AND sees empty where clause array, assign both its nodes. Later AND's will only have right nodes.
     In executing, the current result operation will be cached and AND's with only right nodes will use the result from
     the last operation.
 */

enum class QueryType {
    Undefined, CreateTable, DropTable, ListTables, TableInfo, CreateIndex, DropIndex, Insert, Select, Update, Delete
}

enum class Operator {
    Undefined, Eq, Ne, Gt, Lt, Gte, Lte,
}

enum class ConditionType {
    Undefined, LITERAL, FIELD
}

enum class LogicalOperator {
    AND, OR, NOT,
}

sealed class WhereClauseType {
    data class LogicalOperation(
        var operator: LogicalOperator? = null,
        var leftNode: Condition? = null, // always null in case of NOT
        var rightNode: Condition? = null,
        var leftSubExpr: MutableList<LogicalOperation>? = null,
        var rightSubExpr: MutableList<LogicalOperation>? = null,
    ) : WhereClauseType()

    data class Condition(
        var operand1: String = "",
        var operand1Type: ConditionType = ConditionType.Undefined,
        var operator: Operator = Operator.Undefined,
        var operand2: String = "",
        var operand2Type: ConditionType = ConditionType.Undefined,
    ) : WhereClauseType()

}

data class FieldSchemaDefinition(
    var name: String = "",
    var type: String = "",
    var default: String? = null
)

data class Query(
    var type: QueryType = QueryType.Undefined,
    var indexName: String = "",
    var table: String = "",
    var updates: MutableMap<String, String> = mutableMapOf(),
    var inserts: MutableList<MutableList<String>> = mutableListOf(),
    var fields: MutableList<String> = mutableListOf(),
    var whereFields: MutableList<String> = mutableListOf(),
    var schema: MutableList<FieldSchemaDefinition> = mutableListOf(),
    var currentCond: WhereClauseType.Condition = WhereClauseType.Condition(),
    var operations: MutableList<WhereClauseType.LogicalOperation> = mutableListOf(),
    var currentSubExprOperations: MutableList<WhereClauseType.LogicalOperation>? = null,
    var orderByFields: MutableList<String> = mutableListOf(),
    var isParsingSubExpr: Boolean = false,
    var distinct: Boolean = false,
    var err: String? = null
)

enum class LogicalOperationParseResultEmpty {
    NONE, LEFT, RIGHT
}

data class LogicalOperationParseResult(
    val result: Boolean,
    val empty: LogicalOperationParseResultEmpty
)