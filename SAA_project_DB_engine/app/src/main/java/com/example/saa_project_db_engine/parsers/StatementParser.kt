package com.example.saa_project_db_engine.parsers

import android.util.Log
import com.example.saa_project_db_engine.parsers.models.*

enum class ParseStateStep {
    Type,
    CreateTableName,
    CreateTableOpeningParens,
    CreateTableFieldName,
    CreateTableColon,
    CreateTableFieldType,
    CreateTableFieldCommaOrClosingParentheses,
    CreateIndexName,
    CreateIndexOnWord,
    CreateIndexTableName,
    CreateIndexOpeningParenthesis,
    CreateIndexFieldName,
    CreateIndexClosingParenthesis,
    SelectDistinct,
    SelectField,
    SelectFrom,
    SelectComma,
    SelectFromTable,
    InsertTable,
    InsertFieldsOpeningParentheses,
    InsertFields,
    InsertFieldsCommaOrClosingParentheses,
    InsertValuesOpeningParentheses,
    InsertValuesWord,
    InsertValues,
    InsertValuesCommaOrClosingParens,
    InsertValuesCommaBeforeOpeningParens,
    UpdateTable,
    UpdateSet,
    UpdateField,
    UpdateEquals,
    UpdateValue,
    UpdateComma,
    DeleteFromTable,
    Where,
    WhereField,
    WhereOperator,
    WhereValue,
    WhereOrderByField,
    WhereOrderByComma,
    DropTableName,
    DropIndexTableName,
    DropIndexIndexName,
    TableInfoTableName,
}

class StatementParser {
    private lateinit var state: ParseStmtState

    fun parseQuery(sql: String): Query {
        state = ParseStmtState(0, sql, ParseStateStep.Type, Query(), "")
        while (true) {
            if (state.pos >= state.sql.length) {
                return state.query
            }
            when (state.step) {
                ParseStateStep.Type -> {
                    when (state.peek()) {
                        "CreateTable" -> {
                            state.query.type = QueryType.CreateTable
                            state.pop()
                            state.step = ParseStateStep.CreateTableName
                        }
                        "CreateIndex" -> {
                            state.query.type = QueryType.CreateIndex
                            state.pop()
                            state.step = ParseStateStep.CreateIndexName
                        }
                        "DropTable" -> {
                            state.query.type = QueryType.DropTable
                            state.pop()
                            state.step = ParseStateStep.DropTableName
                        }
                        "DropIndex" -> {
                            state.query.type = QueryType.DropIndex
                            state.pop()
                            state.step = ParseStateStep.DropIndexTableName
                        }
                        "TableInfo" -> {
                            state.query.type = QueryType.TableInfo
                            state.pop()
                            state.step = ParseStateStep.TableInfoTableName
                        }
                        "ListTables" -> {
                            state.query.type = QueryType.ListTables
                            state.pop()
                        }
                        "Select" -> {
                            state.query.type = QueryType.Select
                            state.pop()
                            val optionalDistinct = state.peek()
                            if (optionalDistinct == "DISTINCT") {
                                state.step = ParseStateStep.SelectDistinct
                                continue
                            }
                            state.step = ParseStateStep.SelectField
                        }
                        "Insert INTO" -> {
                            state.query.type = QueryType.Insert
                            state.pop()
                            state.step = ParseStateStep.InsertTable
                        }
                        "Update" -> {
                            state.query.type = QueryType.Update
                            state.pop()
                            state.query.updates = mutableMapOf()
                            state.step = ParseStateStep.UpdateTable
                        }
                        "Delete FROM" -> {
                            state.query.type = QueryType.Delete
                            state.pop()
                            state.step = ParseStateStep.DeleteFromTable
                        }
                        else -> {}
                    }
                }
                ParseStateStep.SelectDistinct -> {
                    val distinct = state.peek()
                    if (distinct != "DISTINCT") {

                    }
                    state.query.distinct = true
                    state.pop()
                    state.step = ParseStateStep.SelectField
                }
                ParseStateStep.SelectField -> {
                    val identifier = state.peek()
                    state.query.fields.add(identifier)
                    state.pop()
                    val optionalFrom = state.peek()
                    if (optionalFrom == "FROM") {
                        state.step = ParseStateStep.SelectFrom
                        continue
                    }
                    state.step = ParseStateStep.SelectComma
                }
                ParseStateStep.SelectFrom -> {
                    val identifier = state.peek()
                    if (identifier.uppercase() != "FROM") {

                    }
                    state.pop()
                    state.step = ParseStateStep.SelectFromTable
                }
                ParseStateStep.SelectComma -> {
                    val comma = state.peek()
                    if (comma != ",") {

                    }
                    state.pop()
                    state.step = ParseStateStep.SelectField
                }
                ParseStateStep.SelectFromTable -> {
                    val table = state.peek()
                    if (table.isEmpty()) {

                    }
                    state.query.table = table
                    state.pop()
                    state.step = ParseStateStep.Where
                }
                ParseStateStep.InsertTable -> {
                    val table = state.peek()
                    state.query.table = table
                    state.pop()
                    state.step = ParseStateStep.InsertFieldsOpeningParentheses
                }
                ParseStateStep.InsertFieldsOpeningParentheses -> {
                    val opening = state.peek()
                    if (opening != "(") {

                    }
                    state.query.inserts = mutableListOf(mutableListOf())
                    state.pop()
                    state.step = ParseStateStep.InsertFields
                }
                ParseStateStep.InsertFields -> {
                    val identifier = state.peek()
                    if (!state.isIdentifier(identifier)) {

                    }
                    state.query.fields.add(identifier)
                    state.pop()
                    state.step = ParseStateStep.InsertFieldsCommaOrClosingParentheses
                }
                ParseStateStep.InsertFieldsCommaOrClosingParentheses -> {
                    val commaOrClosing = state.peek()
                    if (commaOrClosing != "," && commaOrClosing != ")") {

                    }
                    state.pop()
                    if (commaOrClosing == ",") {
                        state.step = ParseStateStep.InsertFields
                        continue
                    }
                    state.step = ParseStateStep.InsertValuesWord
                }
                ParseStateStep.InsertValuesWord -> {
                    val insertWord = state.peek()
                    if (insertWord.uppercase() != "VALUES") {

                    }
                    state.pop()
                    state.step = ParseStateStep.InsertValuesOpeningParentheses
                }
                ParseStateStep.InsertValuesOpeningParentheses -> {
                    val opening = state.peek()
                    if (opening != "(") {

                    }
                    state.query.inserts.add(mutableListOf())
                    state.pop()
                    state.step = ParseStateStep.InsertValues
                }
                ParseStateStep.InsertValues -> {
                    val value = state.peek()
                    state.query.inserts[state.query.inserts.size - 1].add(value)
                    state.pop()
                    state.step = ParseStateStep.InsertValuesCommaOrClosingParens
                }
                ParseStateStep.InsertValuesCommaOrClosingParens -> {
                    val commaOrClosing = state.peek()
                    if (commaOrClosing != "," && commaOrClosing != ")") {

                    }
                    state.pop()
                    if (commaOrClosing == ",") {
                        state.step = ParseStateStep.InsertValues
                        continue
                    }
                    val currentInsertRow = state.query.inserts[state.query.inserts.size - 1]
                    if (currentInsertRow.size != state.query.fields.size) {

                    }
                    state.step = ParseStateStep.InsertValuesCommaBeforeOpeningParens
                }
                ParseStateStep.InsertValuesCommaBeforeOpeningParens -> {
                    val comma = state.peek()
                    if (comma != ",") {

                    }
                    state.pop()
                    state.step = ParseStateStep.InsertValuesOpeningParentheses
                }
                ParseStateStep.UpdateTable -> {
                    val tableName = state.peek()
                    state.query.table = tableName
                    Log.d("TEST", "UpdateTable: ${tableName}")
                    state.pop()
                    state.step = ParseStateStep.UpdateSet
                }
                ParseStateStep.UpdateSet -> {
                    val setWord = state.peek()
                    if (setWord != "SET") {

                    }
                    state.pop()
                    Log.d("TEST", "UpdateSet: SET")
                    state.step = ParseStateStep.UpdateField
                }
                ParseStateStep.UpdateField -> {
                    val identifier = state.peek()
                    if (!state.isIdentifier(identifier)) {

                    }
                    state.currentUpdateField = identifier
                    state.pop()
                    Log.d("TEST", "UpdateField: ${identifier}")
                    state.step = ParseStateStep.UpdateEquals
                }
                ParseStateStep.UpdateEquals -> {
                    val equalsWord = state.peek()
                    if (equalsWord != "=") {

                    }
                    Log.d("TEST", "UpdateEquals: ${equalsWord}")
                    state.pop()
                    state.step = ParseStateStep.UpdateValue
                }
                ParseStateStep.UpdateValue -> {
                    val quoted = state.peekQuotedStringWithLength()
                    Log.d("TEST", "UpdateValue: ${quoted}")
                    if (quoted.length == 0) {

                    }
                    state.query.updates[state.currentUpdateField] = quoted.content
                    state.currentUpdateField = ""
                    state.pop()
                    val maybeWhereWord = state.peek()
                    Log.d("TEST", "UpdateValue: maybeWhereWord: ${maybeWhereWord}")
                    if (maybeWhereWord == "WHERE") {
                        Log.d("TEST", "UpdateValue: maybeWhereWord: WHERE")
                        state.step = ParseStateStep.Where
                        continue
                    }
                    state.step = ParseStateStep.UpdateComma
                }
                ParseStateStep.UpdateComma -> {
                    val commaWord = state.peek()
                    if (commaWord != ",") {

                    }
                    Log.d("TEST", "UpdateComma: commaWord: ${commaWord}")
                    state.pop()
                    state.step = ParseStateStep.UpdateField
                }
                ParseStateStep.DeleteFromTable -> {
                    val table = state.peek()
                    state.query.table = table
                    state.pop()
                    state.step = ParseStateStep.Where
                }
                ParseStateStep.Where -> {
                    val where = state.peek()
                    if (where.uppercase() != "WHERE") {

                    }
                    state.pop()
                    state.step = ParseStateStep.WhereField
                }
                ParseStateStep.WhereField -> {
                    when (val word = state.peek()) {
                        "NOT" -> {
                            state.pop()
                            val op = WhereClauseType.LogicalOperation(
                                LogicalOperator.NOT
                            )
                            if (state.query.isParsingSubExpr) {
                                if (state.query.currentSubExprOperations != null) {
                                    state.query.currentSubExprOperations!!.add(op)
                                } else {
                                    state.query.currentSubExprOperations = mutableListOf()
                                    state.query.currentSubExprOperations!!.add(op)
                                }
                            } else {
                                state.query.operations.add(
                                    op
                                )
                            }
                            state.step = ParseStateStep.WhereField
                        }
                        "(" -> {
                            state.pop()
                            state.query.isParsingSubExpr = true
                            state.step = ParseStateStep.WhereField
                        }
                        else -> {  // identifier
                            state.pop()
                            val cond = WhereClauseType.Condition(word, ConditionType.FIELD)
                            state.query.whereFields.add(word)
                            state.query.currentCond = cond
                            state.step = ParseStateStep.WhereOperator
                        }
                    }
                }
                ParseStateStep.WhereOperator -> {
                    val currentCond = state.query.currentCond
                    when (state.peek()) {
                        "==" -> currentCond.operator = Operator.Eq
                        ">" -> currentCond.operator = Operator.Gt
                        ">=" -> currentCond.operator = Operator.Gte
                        "<" -> currentCond.operator = Operator.Lt
                        "<=" -> currentCond.operator = Operator.Lte
                        "!=" -> currentCond.operator = Operator.Ne
                        else -> {}
                    }
                    state.query.currentCond = currentCond
                    state.pop()
                    state.step = ParseStateStep.WhereValue
                }
                ParseStateStep.WhereValue -> {
                    val currentCond = state.query.currentCond
                    val whereValue = state.peek()
                    if (state.checkIfQuoted()) { // for now, that's how we distinguish between fields and literals
                        val peeked = state.peekQuotedStringWithLength()
                        Log.d("TEST", "PEEKED: $peeked")
                        if (peeked.length != 0) {
                            currentCond.operand2 = peeked.content
                            currentCond.operand2Type = ConditionType.LITERAL
                        }
                    } else if (state.isIdentifier(whereValue)) {
                        currentCond.operand2 = whereValue
                        currentCond.operand2Type = ConditionType.FIELD
                        state.query.whereFields.add(whereValue)
                    }
                    state.query.currentCond = currentCond
                    state.pop()

                    if (state.query.isParsingSubExpr) {
                        if (state.query.currentSubExprOperations != null) {
                            val operation =
                                state.query.currentSubExprOperations!![state.query.currentSubExprOperations!!.size - 1]
                            operation.rightNode = state.query.currentCond
                            state.query.currentSubExprOperations!![state.query.currentSubExprOperations!!.size - 1] =
                                operation

                            if (state.query.operations.isNotEmpty()) {
                                val op =
                                    state.query.operations[state.query.operations.size - 1]
                                op.rightSubExpr = state.query.currentSubExprOperations
                                state.query.operations[state.query.operations.size - 1] = op
                            }
                        }
                    } else {
                        if (state.query.operations.isNotEmpty()) {
                            if (state.query.currentSubExprOperations != null) {
                                val op =
                                    state.query.operations[state.query.operations.size - 1]
                                op.rightSubExpr = state.query.currentSubExprOperations
                                state.query.operations[state.query.operations.size - 1] = op
                                state.query.currentSubExprOperations = null
                            } else {
                                val operation =
                                    state.query.operations[state.query.operations.size - 1]
                                operation.rightNode = state.query.currentCond
                                state.query.operations[state.query.operations.size - 1] =
                                    operation
                            }
                        }
                    }

                    when (state.peek()) {
                        "AND" -> {
                            logicalOperationInit(
                                WhereClauseType.LogicalOperation(
                                    LogicalOperator.AND
                                )
                            )
                        }
                        "OR" -> {
                            logicalOperationInit(
                                WhereClauseType.LogicalOperation(
                                    LogicalOperator.OR
                                )
                            )
                        }
                        ")" -> {
                            state.query.isParsingSubExpr = false
                            val nextLogicalSubExpr = state.peek()
                            if (nextLogicalSubExpr == "AND" || nextLogicalSubExpr == "OR") {
                                state.pop()
                                state.step = ParseStateStep.WhereField
                            }
                        }
                        "ORDER BY" -> {
                            state.step = ParseStateStep.WhereOrderByField
                            state.pop()
                        }
                        else -> {
                            state.step = ParseStateStep.WhereField
                            state.pop()
                        }
                    }
                }
                ParseStateStep.WhereOrderByField -> {
                    val field = state.peek()
                    state.query.orderByFields.add(field)
                    state.pop()
                    state.step = ParseStateStep.WhereOrderByComma
                }
                ParseStateStep.WhereOrderByComma -> {
                    val commaWord = state.peek()
                    if (commaWord != ",") {

                    }
                    state.pop()
                    state.step = ParseStateStep.WhereOrderByField
                }
                ParseStateStep.CreateTableName -> {
                    val table = state.peek()
                    Log.d("TEST", "TABLENAME $table")
                    state.query.table = table
                    state.pop()
                    state.step = ParseStateStep.CreateTableOpeningParens
                }
                ParseStateStep.CreateTableOpeningParens -> {
                    val opening = state.peek()
                    if (opening != "(") {

                    }
                    state.pop()
                    state.step = ParseStateStep.CreateTableFieldName
                }
                ParseStateStep.CreateTableFieldName -> {
                    val field = state.peek()
                    state.pop()
                    Log.d("TEST", "FIELD: $field")
                    state.query.schema.add(FieldSchemaDefinition(field))
                    state.step = ParseStateStep.CreateTableColon
                }
                ParseStateStep.CreateTableColon -> {
                    val colon = state.peek()
                    if (colon != ":") {

                    }
                    state.pop()
                    state.step = ParseStateStep.CreateTableFieldType
                }
                ParseStateStep.CreateTableFieldType -> {
                    val type = state.peek()
                    Log.d("TEST", "TYPE: ${type}")
                    val currentField = state.query.schema[state.query.schema.size - 1]
                    currentField.type = type

                    val assign = {
                        state.query.schema[state.query.schema.size - 1] = currentField
                    }

                    state.pop()
                    val optionalDefault = state.peek()
                    if (optionalDefault == "default") {
                        state.pop()
                        val defaultVal = state.peekQuotedStringWithLength()
                        currentField.default = defaultVal.content
                        assign()
                        state.pop()
                        state.step = ParseStateStep.CreateTableFieldCommaOrClosingParentheses
                        continue
                    }
                    assign()
                    state.pop()
                    state.step = ParseStateStep.CreateTableFieldName
                }
                ParseStateStep.CreateTableFieldCommaOrClosingParentheses -> {
                    val commaOrClosing = state.peek()
                    if (commaOrClosing != "," && commaOrClosing != ")") {

                    }
                    state.pop()
                    if (commaOrClosing == ",") {
                        state.step = ParseStateStep.CreateTableFieldName
                        continue
                    }
                }
                ParseStateStep.DropTableName -> {
                    val table = state.peek()
                    state.pop()
                    state.query.table = table
                }
                ParseStateStep.CreateIndexName -> {
                    val indexName = state.peek()
                    state.pop()
                    state.query.indexName = indexName
                    state.step = ParseStateStep.CreateIndexOnWord
                }
                ParseStateStep.CreateIndexOnWord -> {
                    val onWord = state.peek()
                    if (onWord != "ON") {

                    }
                    state.pop()
                    state.step = ParseStateStep.CreateIndexTableName
                }
                ParseStateStep.CreateIndexTableName -> {
                    val tableName = state.peek()
                    state.pop()
                    state.query.table = tableName
                    state.step = ParseStateStep.CreateIndexOpeningParenthesis
                }
                ParseStateStep.CreateIndexOpeningParenthesis -> {
                    val opening = state.peek()
                    if (opening != "(") {

                    }
                    state.pop()
                    state.step = ParseStateStep.CreateIndexFieldName
                }
                ParseStateStep.CreateIndexFieldName -> {
                    val fieldName = state.peek()
                    state.query.fields.add(fieldName)
                    state.pop()
                    state.step = ParseStateStep.CreateIndexClosingParenthesis
                }
                ParseStateStep.CreateIndexClosingParenthesis -> {
                    val closing = state.peek()
                    if (closing != ")") {

                    }
                    state.pop()
                }
                ParseStateStep.DropIndexTableName -> {
                    val tableName = state.peek()
                    state.query.table = tableName
                    state.pop()
                    state.step = ParseStateStep.DropIndexIndexName
                }
                ParseStateStep.DropIndexIndexName -> {
                    val indexName = state.peek()
                    state.query.indexName = indexName
                    state.pop()
                }
                ParseStateStep.TableInfoTableName -> {
                    val tableName = state.peek()
                    state.query.table = tableName
                    state.pop()
                }
            }
        }
    }

    private fun logicalOperationInit(op: WhereClauseType.LogicalOperation) {
        state.pop()
        if (state.query.isParsingSubExpr) {
            if (state.query.currentSubExprOperations == null) {
                state.query.currentSubExprOperations = mutableListOf()
                val leftNode = state.query.currentCond
                op.leftNode = leftNode
            }
            state.query.currentSubExprOperations!!.add(op)
        } else {
            if (state.query.operations.isEmpty()) {
                if (state.query.currentSubExprOperations != null) { // subexpr performed
                    op.leftSubExpr = state.query.currentSubExprOperations
                    state.query.currentSubExprOperations = null
                } else {
                    val leftNode = state.query.currentCond
                    op.leftNode = leftNode
                }
            } else {
                if (state.query.currentSubExprOperations != null) { // subexpr performed
                    op.rightSubExpr = state.query.currentSubExprOperations
                    state.query.currentSubExprOperations = null
                } else {
                    val rightNode = state.query.currentCond
                    op.rightNode = rightNode
                }
            }
            state.query.operations.add(op)
        }
        state.step = ParseStateStep.WhereField
    }
}