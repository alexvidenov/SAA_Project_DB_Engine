package com.example.saa_project_db_engine.db.base

import com.example.saa_project_db_engine.services.SchemasServiceLocator

open class SchemaAware {
    protected val fileSchema = SchemasServiceLocator.getSchemaFor(this.javaClass.simpleName)
}