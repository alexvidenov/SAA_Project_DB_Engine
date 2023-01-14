package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.parsers.models.Operator

fun invertOperator(op: Operator): Operator {
    return when (op) {
        Operator.Eq -> Operator.Ne
        Operator.Ne -> Operator.Eq
        Operator.Gt -> Operator.Lt
        Operator.Lt -> Operator.Gt
        Operator.Gte -> Operator.Lte
        Operator.Lte -> Operator.Gte
        else -> Operator.Undefined
    }
}