package com.example.saa_project_db_engine.parsers.models

import android.util.Log
import com.example.saa_project_db_engine.parsers.ParseStateStep
import kotlin.math.min

// NOT AND OR -> PRECEDENCE

// WHERE NOT (Id == 5 OR Name == "test" AND ) AND BirthDate == "21.12.02"

/*
    WHERE
        NOT (SUBEXPR)
        SUBEXPR: (thing AND thing). (thing OR thing), (NOT thing AND NOT thing). (thing OR NOT thing)

 */


data class ParseStmtState(
    var pos: Int,
    var sql: String,
    var step: ParseStateStep,
    var query: Query,
    var currentUpdateField: String,
    val isSubExpression: Boolean = false,
    var err: String? = null
) {
    private val reservedSymbols = listOf(
        "(",
        ")",
        ">=",
        "<=",
        "!=",
        ",",
        "==",
        "=",
        ">",
        "<",
        ":",
        "Select",
        "Insert INTO",
        "VALUES",
        "Update",
        "Delete FROM",
        "WHERE",
        "FROM",
        "SET",
        "ORDER BY",
        "DISTINCT",
        "ON",
        "NOT",
        "default"
    )

    fun peek(): String {
        val token = peekWithLength()
        return token.content
    }

    private fun peekWithLength(): PeekWithLengthReturn {
        if (pos >= sql.length) {
            return PeekWithLengthReturn("", 0)
        }
        reservedSymbols.forEach {
            val token = sql.subSequence(IntRange(pos, min(sql.length - 1, pos + it.length - 1)))
            if (token == it) {
                return PeekWithLengthReturn(token.toString(), token.length)
            }
        }
        if (sql[pos] == '\'') {
            return peekQuotedStringWithLength()
        }
        return peekIdentifierWithLength()
    }

    fun checkIfQuoted(): Boolean {
        return !(sql.length < pos || sql[pos] != '\'' && sql[pos] == ')')
    }

    fun peekQuotedStringWithLength(): PeekWithLengthReturn {
        Log.d("TEST", "sql[pos] ${sql[pos]}")
        if (sql.length < pos || sql[pos] != '\'' && sql[pos] == ')') {
            Log.d("TEST", "DA GO TAKOVAM")
            return PeekWithLengthReturn("", 0)
        }
        for (i in (pos + 1) until sql.length) {
            if (sql[i] == '\'' && sql[i - 1] != '\\') {
                val subString = sql.substring(IntRange(pos + 1, i - 1))
                return PeekWithLengthReturn(subString, subString.length + 2)
            }
        }
        return PeekWithLengthReturn("", 0)
    }

    private fun peekIdentifierWithLength(): PeekWithLengthReturn {
        for (i in pos until sql.length) {
            val regex = """[.a-zA-Z0-9_*]""".toRegex()
            if (!regex.matches(sql[i].toString())) {
                val subString = sql.substring(IntRange(pos, i - 1))
                Log.d("TEST", "substring $subString")
                return PeekWithLengthReturn(subString, subString.length)
            }
        }
        val subString = sql.substring(pos)
        return PeekWithLengthReturn(subString, subString.length)
    }

    fun pop() {
        val peeked = peekWithLength()
        pos += peeked.length // moving the cursor with the read token
        removeWhitespace()
    }

    fun isIdentifier(s: String): Boolean {
        reservedSymbols.forEach {
            if (s.uppercase() == it) {
                return false
            }
        }
        val regex = """[.a-zA-Z_][.a-zA-Z_0-9]*""".toRegex()
        return regex.matches(s)
    }

    private fun removeWhitespace() {
        while (pos < sql.length && sql[pos] == ' ') {
            pos++
        }
    }
}

data class PeekWithLengthReturn(val content: String, val length: Int)