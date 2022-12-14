package com.example.saa_project_db_engine.services.models

import com.example.saa_project_db_engine.parsers.models.WhereClauseType

sealed class WhereClause {
    data class LogicalOperations(val ops: List<WhereClauseType.LogicalOperation>) : WhereClause()
    data class SingleCondition(val cond: WhereClauseType.Condition) : WhereClause()
}