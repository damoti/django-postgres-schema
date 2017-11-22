import os
from collections import OrderedDict
from django.apps import apps
from django.conf import settings
from django.core.management.commands.migrate import Command as MigrateCommand
from django.db import router, transaction

from postgres_schema.schema import activate_schema, deactivate_schema, is_tenant_model


with open(os.path.join(os.path.dirname(__file__), '..', '..', 'sql', 'clone_schema.001.sql')) as fp:
    CLONE_SCHEMA = fp.read()


class Command(MigrateCommand):

    def sync_apps(self, connection, app_labels):
        "Runs the old syncdb-style operation on a list of app_labels."
        cursor = connection.cursor()

        try:
            # Get a list of already installed *models* so that references work right.
            tables = connection.introspection.table_names(cursor)
            created_models = set()

            # Build the manifest of apps and models that are to be synchronized
            all_models = [
                (app_config.label,
                 router.get_migratable_models(app_config, connection.alias, include_auto_created=False))
                for app_config in apps.get_app_configs()
                if app_config.models_module is not None and app_config.label in app_labels
                ]

            def model_installed(model):
                opts = model._meta
                converter = connection.introspection.table_name_converter
                # Note that if a model is unmanaged we short-circuit and never try to install it
                return not (
                    (converter(opts.db_table) in tables) or
                    (opts.auto_created and converter(opts.auto_created._meta.db_table) in tables)
                )

            manifest = OrderedDict(
                (app_name, list(filter(model_installed, model_list)))
                for app_name, model_list in all_models
            )

            # Create the tables for each model
            if self.verbosity >= 1:
                self.stdout.write("  Creating tables...\n")

            with transaction.atomic(using=connection.alias, savepoint=connection.features.can_rollback_ddl):

                with connection.schema_editor() as editor:
                    editor.execute("CREATE SCHEMA {}".format(settings.POSTGRES_TEMPLATE_SCHEMA))
                    statements = editor.connection.ops.prepare_sql_script(CLONE_SCHEMA)
                    for statement in statements:
                        editor.execute(statement, params=None)

                schema_deferred_sql = {}

                with connection.schema_editor() as editor:
                    schema_model = apps.get_model(settings.POSTGRES_SCHEMA_MODEL)
                    editor.create_model(schema_model, verbosity=self.verbosity)
                    schema_deferred_sql.update(editor.schema_deferred_sql)
                    editor.schema_deferred_sql = {}
                    created_models.add(schema_model)

                for app_name, model_list in manifest.items():
                    for model in model_list:
                        if not model._meta.can_migrate(connection):
                            continue
                        if model in created_models:
                            continue  # probably schema model
                        if self.verbosity >= 3:
                            self.stdout.write(
                                "    Processing %s.%s model\n" % (app_name, model._meta.object_name)
                            )
                        with connection.schema_editor() as editor:
                            editor.schema_deferred_sql.update(schema_deferred_sql)
                            editor.create_model(model, verbosity=self.verbosity)
                            schema_deferred_sql.update(editor.schema_deferred_sql)
                            editor.schema_deferred_sql = {}
                            created_models.add(model)

                if self.verbosity >= 1:
                    self.stdout.write("\n    Running deferred SQL...\n")
                with connection.schema_editor() as editor:
                    editor.schema_deferred_sql = schema_deferred_sql

        finally:
            cursor.close()

        return created_models
