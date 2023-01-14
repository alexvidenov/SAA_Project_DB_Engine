package com.example.saa_project_db_engine.db.managers.file.extensions

import com.example.saa_project_db_engine.db.managers.file.models.PageMetadata
import com.example.saa_project_db_engine.safeCapitalize
import kotlin.reflect.KProperty

enum class PageMetadataProperties {
    NextLogicalPageId,
    NextRowId,
    EntriesCount
}

inline operator fun <reified T> PageMetadata.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (PageMetadataProperties.valueOf(property.name.safeCapitalize())) {
        PageMetadataProperties.NextLogicalPageId -> nextLogicalPageId as T
        PageMetadataProperties.NextRowId -> nextRowId as T
        PageMetadataProperties.EntriesCount -> entriesCount as T
    }
}

operator fun <T> PageMetadata.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (PageMetadataProperties.valueOf(property.name.safeCapitalize())) {
        PageMetadataProperties.NextLogicalPageId -> (value as Int?)?.let {
            nextLogicalPageId = it
        }
        PageMetadataProperties.NextRowId -> (value as Int?)?.let {
            nextRowId = it
        }
        PageMetadataProperties.EntriesCount -> (value as Int?)?.let {
            entriesCount = it
        }
    }
}