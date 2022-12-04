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
    DropTableName,
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
}

class StatementParser {
    private lateinit var state: ParseStmtState

    fun parseQuery(sql: String): Query {
        state = ParseStmtState(0, sql, ParseStateStep.Type, Query(), "")
        while (true) {
            Log.d("TEST", "STEP: ${state.step}")
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
                        "DropTable" -> {
                            state.query.type = QueryType.DropTable
                            state.pop()
                            state.step = ParseStateStep.DropTableName
                        }
                        "ListTables" -> {
                            state.query.type = QueryType.ListTables
                            state.pop()
                        }
                        "Select" -> {
                            state.query.type = QueryType.Select
                            state.pop()
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
                            state.step = ParseStateStep.UpdateField
                        }
                        "Delete FROM" -> {
                            state.query.type = QueryType.Delete
                            state.pop()
                            state.step = ParseStateStep.DeleteFromTable
                        }
                        else -> {}
                    }
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
                ParseStateStep.UpdateTable -> TODO()
                ParseStateStep.UpdateSet -> TODO()
                ParseStateStep.UpdateField -> TODO()
                ParseStateStep.UpdateEquals -> TODO()
                ParseStateStep.UpdateValue -> TODO()
                ParseStateStep.UpdateComma -> TODO()
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
                            state.query.operations.add(
                                WhereClauseType.LogicalOperation(
                                    LogicalOperator.NOT
                                )
                            )
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
                    if (state.isIdentifier(whereValue)) {
                        currentCond.operand2 = whereValue
                        currentCond.operand2Type = ConditionType.FIELD
                    } else {
                        val peeked = state.peekQuotedStringWithLength()
                        currentCond.operand2 = peeked.content
                        currentCond.operand2Type = ConditionType.LITERAL
                    }
                    state.query.currentCond = currentCond
                    state.pop()

                    if (state.query.operations.isNotEmpty()) {
                        val op = state.query.operations[state.query.operations.size - 1]
                        op.rightNode = currentCond
                    }

                    when (state.peek()) {
                        "AND" -> {
                            state.pop()
                            val op = WhereClauseType.LogicalOperation(LogicalOperator.AND)
                            if (state.query.operations.isEmpty()) {
                                val leftNode = state.query.currentCond
                                op.leftNode = leftNode
                                // assign leftnode or leftsubexpr depending on whether we're parsing subExpr
                            }
                            state.query.currentLogicalSubExpr = op
                            state.query.operations.add(op)
                            state.step = ParseStateStep.WhereField
                        }
                        "OR" -> {
                            state.pop()
                            val op = WhereClauseType.LogicalOperation(LogicalOperator.OR)
                            if (state.query.operations.isEmpty()) {
                                val leftNode = state.query.currentCond
                                op.leftNode = leftNode
                            }
                            state.query.currentLogicalSubExpr = op
                            state.query.operations.add(op)
                            state.step = ParseStateStep.WhereField
                        }
                        ")" -> {
                            state.query.isParsingSubExpr = false
                            // add current sub expr
                        }
                        else -> {
                            state.pop()
                        }
                    }
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
            }
        }
    }
}