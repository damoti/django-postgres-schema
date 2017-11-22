INSTALLED_APPS = (
    'postgres_schema',
    'schema_test_app'
)
DATABASES = {
    'default': {
        'ENGINE': 'postgres_schema.engine',
        'NAME': 'postgres_schema',
        'TEST': {
            'SERIALIZE': False
        }
    }
}
POSTGRES_SCHEMA_MODEL = 'schema_test_app.Company'
SECRET_KEY = 'test-key'
