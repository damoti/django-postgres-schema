import sys
from django.conf import settings
from .models import get_schema_model

Schema = get_schema_model()


class ALL_SCHEMAS:
    pass


class RunInSchemas:

    def __init__(self, operation, schemas=ALL_SCHEMAS, public=False, template=False):
        self.operation = operation
        self.public = public
        self.template = template
        if schemas is ALL_SCHEMAS:
            self.schemas = Schema.objects.all()
        else:
            self.schemas = schemas

    def _wrap_database_migration(self, method, app_label, schema_editor, from_state, to_state):

        sys.stdout.write('\n    {0:<42}'.format(self.operation.describe()))

        if self.public:
            sys.stdout.write(' ')
            sys.stdout.write(settings.POSTGRES_PUBLIC_SCHEMA)
            sys.stdout.flush()
            schema_editor.activate_schema(settings.POSTGRES_PUBLIC_SCHEMA)
            method(app_label, schema_editor, from_state, to_state)

        if self.template:
            sys.stdout.write(' ')
            sys.stdout.write(settings.POSTGRES_TEMPLATE_SCHEMA)
            sys.stdout.flush()
            schema_editor.activate_schema(settings.POSTGRES_TEMPLATE_SCHEMA)
            method(app_label, schema_editor, from_state, to_state)

        for schema in self.schemas:
            schema_editor.activate_schema(schema)
            sys.stdout.write(' ')
            sys.stdout.write(schema.schema)
            sys.stdout.flush()
            method(app_label, schema_editor, from_state, to_state)

        schema_editor.deactivate_schema()

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        self._wrap_database_migration(
            self.operation.database_forwards, app_label, schema_editor, from_state, to_state
        )

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        self._wrap_database_migration(
            self.operation.database_backwards, app_label, schema_editor, from_state, to_state
        )

    def __getattr__(self, attr):
        return getattr(self.operation, attr)


class RunInPublic(RunInSchemas):
    def __init__(self, operation):
        super().__init__(operation, schemas=[], public=True)


class RunInTemplate(RunInSchemas):
    def __init__(self, operation):
        super().__init__(operation, schemas=[], template=True)
