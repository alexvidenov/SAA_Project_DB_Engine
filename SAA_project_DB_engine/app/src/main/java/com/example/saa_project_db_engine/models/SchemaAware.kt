package com.example.saa_project_db_engine.models

import com.example.saa_project_db_engine.services.SchemasServiceLocator

open class SchemaAware {
    val fileSchema = SchemasServiceLocator.getSchemaFor(this.javaClass.simpleName)
}