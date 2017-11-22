from django.apps import AppConfig


class PostgresSchemaConfig(AppConfig):
    name = 'postgres_schema'

    def ready(self):
        from django.conf import settings
        for key in dir(DefaultSettings):
            if not hasattr(settings, key):
                setattr(settings, key,
                        getattr(DefaultSettings, key))


class DefaultSettings:
    POSTGRES_PUBLIC_SCHEMA = 'public'
    POSTGRES_TEMPLATE_SCHEMA = '__template__'
    POSTGRES_SCHEMA_MODEL = None
    POSTGRES_SCHEMA_TENANTS = []
