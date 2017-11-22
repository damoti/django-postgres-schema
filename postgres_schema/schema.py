import sys
from django.conf import settings
from django.db import connection
from django.db.backends.postgresql.schema import DatabaseSchemaEditor as PostgreSQLSchemaEditor


def is_tenant_model(model):
    if model._meta.app_label in settings.POSTGRES_SCHEMA_TENANTS:
        return True
    if model._meta.label in settings.POSTGRES_SCHEMA_TENANTS:
        return True
    return False


def create_schema(schema_name):
    with connection.cursor() as cursor:
        cursor.execute("SELECT clone_schema(%s, %s)", (
            settings.POSTGRES_TEMPLATE_SCHEMA, schema_name,
        ))


def schema_exists(schema_name):
    with connection.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema_name,))
        return bool(cursor.fetchone())


def activate_schema(schema_name, exclude_public=False):
    with connection.cursor() as cursor:
        if schema_name == settings.POSTGRES_PUBLIC_SCHEMA or exclude_public:
            cursor.execute("SET search_path TO %s", (schema_name,))
        else:
            cursor.execute("SET search_path TO %s, %s", (
                schema_name, settings.POSTGRES_PUBLIC_SCHEMA,
            ))


def deactivate_schema():
    activate_schema(settings.POSTGRES_PUBLIC_SCHEMA)


def get_active_schema_name():
    with connection.cursor() as cursor:
        cursor.execute('SELECT current_schema()')
        return cursor.fetchone()


def wrap(name):

    def _apply_to_all(self, model, *args, **kwargs):
        from .models import get_schema_model

        verbosity = kwargs.pop('verbosity', 1)
        if model._meta.label == 'migrations.Migration':
            # there is no otherway to silence Migration creation
            verbosity = 0

        if not self.wrapped:
            return getattr(super(DatabaseSchemaEditor, self), name)(model, *args, **kwargs)

        method = getattr(self, name)

        if verbosity >= 1:
            sys.stdout.write('\n    {a:<16} {m._meta.label:<25}'.format(a=name, m=model))

        if not is_tenant_model(model):
            self.wrapped = False
            if verbosity >= 1:
                sys.stdout.write(' ')
                sys.stdout.write(settings.POSTGRES_PUBLIC_SCHEMA)
                sys.stdout.flush()
            result = method(model, *args, **kwargs)
            self.wrapped = True
            return result

        schema_names = [settings.POSTGRES_TEMPLATE_SCHEMA]
        schema_names.extend(get_schema_model().objects.values_list('schema', flat=True))
        result = None
        for schema in schema_names:
            self.activate_schema(schema)
            self.wrapped = False
            if verbosity >= 1:
                sys.stdout.write(' ')
                sys.stdout.write(schema)
                sys.stdout.flush()
            result = method(model, *args, **kwargs)
            self.wrapped = True
        self.deactivate_schema()
        return result

    return _apply_to_all


class DatabaseSchemaEditor(PostgreSQLSchemaEditor):

    column_sql = wrap('column_sql')
    create_model = wrap('create_model')
    delete_model = wrap('delete_model')
    alter_unique_together = wrap('alter_unique_together')
    alter_index_together = wrap('alter_index_together')
    alter_db_table = wrap('alter_db_table')
    add_field = wrap('add_field')
    remove_field = wrap('remove_field')
    alter_field = wrap('alter_field')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wrapped = True

    def __enter__(self):
        super().__enter__()
        self.schema_deferred_sql = {}
        self.activate_schema(settings.POSTGRES_PUBLIC_SCHEMA)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.deferred_sql = self.schema_deferred_sql.pop(settings.POSTGRES_PUBLIC_SCHEMA, [])
        if exc_type is None:
            for schema_name, sql in self.schema_deferred_sql.items():
                activate_schema(schema_name)
                for statement in sql:
                    self.execute(statement)
            deactivate_schema()
        super().__exit__(exc_type, exc_value, traceback)

    def activate_schema(self, schema):
        if isinstance(schema, str):
            self.schema = None
            self.schema_name = schema
        else:
            self.schema = schema
            self.schema_name = schema.schema
        activate_schema(self.schema_name)
        self.deferred_sql = self.schema_deferred_sql.setdefault(self.schema_name, [])

    def deactivate_schema(self):
        self.activate_schema(settings.POSTGRES_PUBLIC_SCHEMA)

    def _constraint_names(self, model, column_names=None, unique=None,
                          primary_key=None, index=None, foreign_key=None,
                          check=None):
        """
        Returns all constraint names matching the columns and conditions
        """
        column_names = list(column_names) if column_names else None
        with self.connection.cursor() as cursor:
            constraints = get_constraints(cursor, model._meta.db_table)
        result = []
        for name, infodict in constraints.items():
            if column_names is None or column_names == infodict['columns']:
                if unique is not None and infodict['unique'] != unique:
                    continue
                if primary_key is not None and infodict['primary_key'] != primary_key:
                    continue
                if index is not None and infodict['index'] != index:
                    continue
                if check is not None and infodict['check'] != check:
                    continue
                if foreign_key is not None and not infodict['foreign_key']:
                    continue
                result.append(name)

        return result


def get_constraints(cursor, table_name):
    """
    Retrieves any constraints or keys (unique, pk, fk, check, index) across one or more columns.
    """
    constraints = {}
    # Loop over the key table, collecting things as constraints
    # This will get PKs, FKs, and uniques, but not CHECK
    cursor.execute("""
        SELECT
            kc.constraint_name,
            kc.column_name,
            c.constraint_type,
            array(SELECT table_name::text || '.' || column_name::text
                  FROM information_schema.constraint_column_usage
                  WHERE constraint_name = kc.constraint_name)
        FROM information_schema.key_column_usage AS kc
        JOIN information_schema.table_constraints AS c ON
            kc.table_schema = c.table_schema AND
            kc.table_name = c.table_name AND
            kc.constraint_name = c.constraint_name
        WHERE
            kc.table_schema = current_schema() AND
            kc.table_name = %s
        ORDER BY kc.ordinal_position ASC
    """, [table_name])
    for constraint, column, kind, used_cols in cursor.fetchall():
        # If we're the first column, make the record
        if constraint not in constraints:
            constraints[constraint] = {
                "columns": [],
                "primary_key": kind.lower() == "primary key",
                "unique": kind.lower() in ["primary key", "unique"],
                "foreign_key": tuple(used_cols[0].split(".", 1)) if kind.lower() == "foreign key" else None,
                "check": False,
                "index": False,
            }
        # Record the details
        constraints[constraint]['columns'].append(column)
    # Now get CHECK constraint columns
    cursor.execute("""
        SELECT kc.constraint_name, kc.column_name
        FROM information_schema.constraint_column_usage AS kc
        JOIN information_schema.table_constraints AS c ON
            kc.table_schema = c.table_schema AND
            kc.table_name = c.table_name AND
            kc.constraint_name = c.constraint_name
        WHERE
            c.constraint_type = 'CHECK' AND
            kc.table_schema = current_schema() AND
            kc.table_name = %s
    """, [table_name])
    for constraint, column in cursor.fetchall():
        # If we're the first column, make the record
        if constraint not in constraints:
            constraints[constraint] = {
                "columns": [],
                "primary_key": False,
                "unique": False,
                "foreign_key": None,
                "check": True,
                "index": False,
            }
        # Record the details
        constraints[constraint]['columns'].append(column)
    # Now get indexes
    cursor.execute("""
        SELECT
            c2.relname,
            ARRAY(
                SELECT (SELECT attname FROM pg_catalog.pg_attribute WHERE attnum = i AND attrelid = c.oid)
                FROM unnest(idx.indkey) i
            ),
            idx.indisunique,
            idx.indisprimary
        FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,
            pg_catalog.pg_index idx, pg_catalog.pg_namespace n
        WHERE c.oid = idx.indrelid
            AND idx.indexrelid = c2.oid
            AND n.oid = c.relnamespace
            AND n.nspname = current_schema()
            AND c.relname = %s
    """, [table_name])
    for index, columns, unique, primary in cursor.fetchall():
        if index not in constraints:
            constraints[index] = {
                "columns": list(columns),
                "primary_key": primary,
                "unique": unique,
                "foreign_key": None,
                "check": False,
                "index": True,
            }
    return constraints
