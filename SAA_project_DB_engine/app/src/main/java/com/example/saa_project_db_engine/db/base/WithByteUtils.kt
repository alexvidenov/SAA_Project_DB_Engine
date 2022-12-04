package com.example.saa_project_db_engine.db.base

interface WithByteUtils {
    fun toAvroBytesSize(): Int

    fun empty(): WithByteUtils
}