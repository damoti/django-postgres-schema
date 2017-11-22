from threading import local

from django.apps import apps as django_apps
from django.conf import settings
from django.core.validators import RegexValidator
from django.db import models
from django.db.models import query, manager
from django.forms import ValidationError
from django.utils.translation import ugettext_lazy as _
from django.core.exceptions import ImproperlyConfigured

from .schema import (
    create_schema, schema_exists,
    activate_schema, deactivate_schema,
)


def get_schema_model():
    """
    Returns the schema model that is active in this project.
    """
    try:
        return django_apps.get_model(settings.POSTGRES_SCHEMA_MODEL, require_ready=False)
    except ValueError:
        raise ImproperlyConfigured("POSTGRES_SCHEMA_MODEL must be of the form 'app_label.model_name'")
    except LookupError:
        raise ImproperlyConfigured(
            "POSTGRES_SCHEMA_MODEL refers to model '%s' that has not been installed" % settings.POSTGRES_SCHEMA_MODEL
        )


class SchemaQuerySet(models.query.QuerySet):

    def active(self):
        return self.filter(is_active=True)

    def inactive(self):
        return self.filter(is_active=False)

    def delete(self):
        self.update(is_active=False)

    def activate(self, pk):
        self.get(pk=pk).activate()


_active = local()


class AbstractSchema(models.Model):
    """
    The Schema model provides an abstraction for a Postgres schema.

    It will take care of creating a cloned copy of the template schema
    when it is created, and also has the ability to activate and deactivate
    itself.
    """

    SCHEMA_NAME_VALIDATOR_MESSAGE = (
        'May only contain lowercase letters, digits, underscores and dashes. '
        'Must start with a letter.'
    )

    schema = models.CharField(max_length=36, primary_key=True, unique=True,
        validators=[RegexValidator(
            regex='^[a-z][a-z0-9_\-]*$',
            message=_(SCHEMA_NAME_VALIDATOR_MESSAGE)
        )],
        help_text='<br>'.join([
            'The internal name of the schema.',
            SCHEMA_NAME_VALIDATOR_MESSAGE,
            'May not be changed after creation.',
        ]),
    )

    name = models.CharField(max_length=128, unique=True,
        help_text=_('The display name of the schema.')
    )

    is_active = models.BooleanField(default=True,
        help_text=_('Use this instead of deleting schema.')
    )

    objects = SchemaQuerySet.as_manager()

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initial_schema = self.schema

    def __repr__(self):
        return '%s (%s)' % (self.name, self.schema)

    def save(self, *args, **kwargs):

        if self.schema in (settings.POSTGRES_PUBLIC_SCHEMA, settings.POSTGRES_TEMPLATE_SCHEMA):
            raise ValidationError(_('Schema %s is not editable') % self.schema)

        self._meta.get_field('schema').run_validators(self.schema)

        if self._state.adding:
            if self.schema_exists():
                raise ValidationError(_('Schema %s already in use') % self.schema)
            else:
                self.create_schema()

        elif self.schema != self._initial_schema:
            raise ValidationError(_('may not change schema after creation.'))

        return super().save(*args, **kwargs)

    def delete(self, using=None, keep_parents=False):
        self.is_active = False
        self.save()

    def create_schema(self):
        create_schema(self.schema)

    def schema_exists(self):
        return schema_exists(self.schema)

    def activate(self):
        activate_schema(self.schema)
        _active.schema = self

    @staticmethod
    def deactivate():
        deactivate_schema()
        _active.schema = None

    @staticmethod
    def active():
        return getattr(_active, "schema", None)


class SchemaAwareModel(models.Model):
    class Meta:
        abstract = True

    def __eq__(self, other):
        if not hasattr(self, '_schema'):
            raise ImproperlyConfigured(
                "SchemaAwareModel must be created by a SchemaAwareManager."
                .format(self.__class__.__name__)
            )
        return super().__eq__(other) and self._schema == other._schema


class SchemaAwareBaseManager(manager.BaseManager):
    def get_queryset(self):
        return super().get_queryset().annotate(_schema='current_schema()')


class SchemaAwareManager(SchemaAwareBaseManager.from_queryset(query.QuerySet)):
    pass
