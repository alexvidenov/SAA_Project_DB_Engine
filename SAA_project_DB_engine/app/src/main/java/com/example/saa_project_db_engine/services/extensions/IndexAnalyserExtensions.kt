package com.example.saa_project_db_engine.services.extensions

import android.util.Log
import com.example.saa_project_db_engine.services.TableService

fun TableService.analysePossibleIndexScan(fields: List<String>, tableName: String): Boolean {
    val data = managerPool[tableName]!!
    fields.forEach {
        data.indexes[it] ?: return false
    }
    return true
}