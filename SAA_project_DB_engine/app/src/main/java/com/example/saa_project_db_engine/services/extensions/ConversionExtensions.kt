package com.example.saa_project_db_engine.services.extensions

import com.example.saa_project_db_engine.serialization.GenericRecord
import org.apache.avro.Schema

fun convertOperandToNativeType(
    value: String,
    operand: String,
    record: GenericRecord
): Any {
    return convertStringToNativeType(
        value,
        fieldType(record, operand)
    )
}

fun convertStringToNativeType(value: String, type: Schema.Type): Any {
    return when (type) {
        Schema.Type.STRING -> {
            value
        }
        Schema.Type.INT -> {
            value.toInt()
        }
        Schema.Type.LONG -> {
            value.toLong()
        }
        Schema.Type.FLOAT -> {
            value.toFloat()
        }
        Schema.Type.DOUBLE -> {
            value.toDouble()
        }
        Schema.Type.BOOLEAN -> {
            value.toBoolean()
        }
        else -> {}
    }
}

fun fieldType(record: GenericRecord, field: String): Schema.Type {
    return record.schema.fields.find {
        it.name() == field
    }!!.schema().type
}