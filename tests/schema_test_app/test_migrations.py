from django.db import connection, models, migrations
from django.db.migrations.migration import Migration
from django.db.migrations.state import ProjectState
from django.test import TestCase
from django.test.utils import isolate_apps
from django.utils import six

from postgres_schema.models import get_schema_model
from postgres_schema.schema import activate_schema, deactivate_schema

Schema = get_schema_model()


@isolate_apps('schema_test_app', attr_name='apps')
class MigrationTest(TestCase):

    def get_table_description(self, table):
        with connection.cursor() as cursor:
            return connection.introspection.get_table_description(cursor, table)

    def get_table_list(self):
        with connection.cursor() as cursor:
            table_list = connection.introspection.get_table_list(cursor)
        if table_list and not isinstance(table_list[0], six.string_types):
            table_list = [table.name for table in table_list]
        return table_list

    def assertTableExists(self, table):
        self.assertIn(table, self.get_table_list())

    def assertTableNotExists(self, table):
        self.assertNotIn(table, self.get_table_list())

    def assertColumnExists(self, table, column):
        self.assertIn(column, [c.name for c in self.get_table_description(table)])

    def assertColumnNotExists(self, table, column):
        self.assertNotIn(column, [c.name for c in self.get_table_description(table)])

    def assertColumnNull(self, table, column):
        self.assertEqual([c.null_ok for c in self.get_table_description(table) if c.name == column][0], True)

    def assertColumnNotNull(self, table, column):
        self.assertEqual([c.null_ok for c in self.get_table_description(table) if c.name == column][0], False)

    def assertIndexExists(self, table, columns, value=True):
        with connection.cursor() as cursor:
            self.assertEqual(
                value,
                any(
                    c["index"]
                    for c in connection.introspection.get_constraints(cursor, table).values()
                    if c['columns'] == list(columns)
                ),
            )

    def assertIndexNotExists(self, table, columns):
        return self.assertIndexExists(table, columns, False)

    def assertFKExists(self, table, columns, to, value=True):
        with connection.cursor() as cursor:
            self.assertEqual(
                value,
                any(
                    c["foreign_key"] == to
                    for c in connection.introspection.get_constraints(cursor, table).values()
                    if c['columns'] == list(columns)
                ),
            )

    def assertFKNotExists(self, table, columns, to, value=True):
        return self.assertFKExists(table, columns, to, False)

    def test_create_shared_model(self):
        migration = Migration('name', 'tests')
        migration.operations = [migrations.CreateModel("Address", [
            ('id', models.AutoField(primary_key=True)),
            ('street', models.TextField()),
        ])]
        with connection.schema_editor() as editor:
            migration.apply(ProjectState(), editor)

        activate_schema('public')
        self.assertTableExists('tests_address')
        activate_schema('__template__', exclude_public=True)
        self.assertTableNotExists('tests_address')

    def test_create_tenant_model(self):
        migration = Migration('name', 'tests')
        migration.operations = [migrations.CreateModel("Address", [
            ('id', models.AutoField(primary_key=True)),
            ('street', models.TextField()),
        ])]
        with self.settings(POSTGRES_SCHEMA_TENANTS=['tests']):
            with connection.schema_editor() as editor:
                migration.apply(ProjectState(), editor)

        activate_schema('public')
        self.assertTableNotExists('tests_address')
        activate_schema('__template__', exclude_public=True)
        self.assertTableExists('tests_address')
