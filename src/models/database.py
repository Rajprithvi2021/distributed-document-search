"""
DEPRECATED: This file is no longer used.

The Distributed Document Search System uses Elasticsearch as its primary data store.
Database models have been replaced with Pydantic schemas for request/response validation.

MIGRATION NOTES:
- SQLAlchemy ORM removed (PostgreSQL layer removed)
- All data now stored in Elasticsearch (9200)
- Tenant isolation enforced via index-per-tenant pattern
- Metadata stored in Elasticsearch document fields

SCHEMA LOCATION:
All request/response validation schemas are now defined in:
  → src/api/schemas.py

This includes:
  - DocumentCreate: Used for creating new documents
  - DocumentResponse: Document data returned from API
  - SearchRequest: Search query parameters
  - SearchResponse: Search results
  - And other related schemas

For details on data structures, see src/api/schemas.py
For architecture overview, see docs/ARCHITECTURE_SIMPLE.md
"""


class DatabaseManager:
    """Compatibility shim for older imports.

    The running prototype stores documents in Elasticsearch and validates data
    with Pydantic schemas in src/api/schemas.py. No SQL database connection is
    opened by this module.
    """
    
    def create_tables(self):
        """No-op retained for compatibility."""
        return None
    
    def drop_tables(self):
        """No-op retained for compatibility."""
        return None
    
    def get_session(self):
        """Raise because SQL storage is not configured in this prototype."""
        raise RuntimeError("No SQL database is configured for this prototype")


# Global database manager instance
db_manager = DatabaseManager()
