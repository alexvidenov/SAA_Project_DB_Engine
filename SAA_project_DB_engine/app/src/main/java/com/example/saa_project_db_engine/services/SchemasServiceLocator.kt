package com.example.saa_project_db_engine.services

import android.annotation.SuppressLint
import android.content.Context
import org.apache.avro.Schema
import java.io.File

@SuppressLint("StaticFieldLeak")
object SchemasServiceLocator {
    lateinit var ctx: Context

    fun getSchemaFor(className: String): Schema {
        val file = File(ctx.filesDir, "${className}.avsc")
        return Schema.Parser().parse(file)
    }
}