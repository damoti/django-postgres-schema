from django.db import connection, migrations, models
from django.db.utils import ProgrammingError
from django.db.migrations.state import ProjectState
from django.test import TestCase, override_settings
from django.test.utils import isolate_apps

from postgres_schema.operations import RunInSchemas
from .models import Company


class MigrationTestCase(TestCase):

    def migrate(self, ops, state=None):
        class Migration(migrations.Migration):
            operations = ops
        migration = Migration('name', 'schema_test_app')
        with connection.schema_editor() as schema_editor:
            return migration.apply(state or ProjectState.from_apps(self.apps), schema_editor)


@isolate_apps('schema_test_app', attr_name='apps')
@override_settings(POSTGRES_SCHEMA_TENANTS='schema_test_app.Person',)
class RunInSchemaTests(MigrationTestCase):

    create_model = migrations.CreateModel(
        'person', [
            ('name', models.TextField()),
        ]
    )

    run_sql = migrations.RunSQL(
        "INSERT INTO schema_test_app_person (name) VALUES ('Ludwig von Mises')"
    )

    def test_not_in_schema_has_exception(self):
        expected_message = 'relation "schema_test_app_person" does not exist'
        with self.assertRaisesMessage(ProgrammingError, expected_message):
            self.migrate([
                self.create_model,
                self.run_sql
            ])

    def test_in_schema_no_exception(self):
        # test that no exception is thrown
        self.migrate([
            self.create_model,
            RunInSchemas(self.run_sql)
        ])

    def test_run_in_specific_schema(self):
        good = Company.objects.create(schema='good', name='good')
        bad = Company.objects.create(schema='bad', name='bad')
        state = self.migrate([
            self.create_model,
            RunInSchemas(self.run_sql, Company.objects.filter(name='good'))
        ])
        Person = state.apps.get_model('schema_test_app.person')
        bad.activate()
        self.assertFalse(Person.objects.exists())
        good.activate()
        self.assertTrue(Person.objects.exists())
