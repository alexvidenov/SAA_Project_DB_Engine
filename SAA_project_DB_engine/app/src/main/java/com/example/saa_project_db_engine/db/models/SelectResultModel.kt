package com.example.saa_project_db_engine.db.models

data class SelectResultModel(
    val fields: List<String> = mutableListOf(),
    val values: List<List<String>> = mutableListOf()
)