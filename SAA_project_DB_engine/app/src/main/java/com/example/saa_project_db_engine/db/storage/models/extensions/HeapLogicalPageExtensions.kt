package com.example.saa_project_db_engine.db.storage.models.extensions

import com.example.saa_project_db_engine.db.storage.models.HeapPageData
import com.example.saa_project_db_engine.safeCapitalize
import kotlin.reflect.KProperty

enum class PageDataProperties {
    Id, PreviousId, NextId
}

inline operator fun <reified T> HeapPageData.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (PageDataProperties.valueOf(property.name.safeCapitalize())) {
        PageDataProperties.Id -> id as T
        PageDataProperties.PreviousId -> previousPageId as T
        PageDataProperties.NextId -> nextPageId as T
    }
}

operator fun <T> HeapPageData.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (PageDataProperties.valueOf(property.name.safeCapitalize())) {
        PageDataProperties.Id -> (value as Int?)?.let { id = it }
        PageDataProperties.PreviousId -> (value as Int?)?.let { previousPageId = it }
        PageDataProperties.NextId -> (value as Int?)?.let { nextPageId = it }
    }
}