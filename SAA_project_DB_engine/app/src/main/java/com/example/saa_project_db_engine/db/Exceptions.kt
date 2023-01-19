package com.example.saa_project_db_engine.db

class PageFullException(message: String) : Exception(message)
class NullPersistenceModelException(message: String) : Exception(message)

class CRC32CheckFailedException(message: String) : Exception(message)