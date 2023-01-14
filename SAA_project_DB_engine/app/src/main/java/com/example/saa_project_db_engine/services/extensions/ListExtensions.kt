package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.services.TableService
import java.io.File

fun TableService.listTables(): List<String> {
    return files.filter {
        val file = File(dir, it)
        file.extension == "db"
    }.map {
        it.replace(".db", "")
    }
}