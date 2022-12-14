package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.parsers.models.FieldSchemaDefinition
import org.apache.avro.Schema

fun buildTableSchemaRepresentation(tableName: String, fields: List<FieldSchemaDefinition>): String {
    val avroSchema = StringBuilder()
    var builder = avroSchema
        .append("{")
        .append("\"name\":")
        .append("\"${tableName}\"")
        .append(",")
        .append("\"type\": \"record\"")
        .append(",")
        .append("\"fields\": [")
    fields.forEachIndexed { i, it ->
        builder = builder
            .append("{\"name\":")
            .append("\"${it.name}\",")
            .append("\"type\": \"${it.type}\"")
        if (it.default != null) {
            var valueToAppend = it.default
            if (it.type == "string") {
                valueToAppend = "\"${valueToAppend}\""
            }
            builder = builder.append(",\"default\": $valueToAppend")
        }
        builder = builder.append("}")
        if (i != fields.size - 1) {
            builder = builder.append(",")
        }
    }
    builder = builder.append("]}")
    return builder.toString()
}

fun buildIndexSchemaRepresentation(
    tableName: String,
    fieldName: String,
    tableSchema: Schema,
): String {
    val fieldType =
        tableSchema.fields.find { it.name() == fieldName }?.schema()?.type?.getName()

    var builder = StringBuilder()
        .append("{")
        .append("\"name\":")
        .append("\"${tableName}\"")
        .append(",")
        .append("\"type\": \"record\"")
        .append(",")
        .append("\"fields\": [")
    builder = builder
        .append("{\"name\":")
        .append("\"${fieldName}\",")
        .append("\"type\": \"${fieldType}\",")
        .append("\"order\":")
        .append("\"ascending\"")
        .append("}")
        .append("]}")
    return builder.toString()
}