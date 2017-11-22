from django.db.backends.postgresql import base
from postgres_schema.schema import DatabaseSchemaEditor


class DatabaseWrapper(base.DatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor
