package com.example.saa_project_db_engine.db

abstract class PageData {
    abstract val id: Int
    abstract var previousId: Int?
    abstract var nextId: Int?
}