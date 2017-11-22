.PHONY: test clean release

test:
	tox

clean:
	rm -rf build dist django_postgres_schema.egg-info

release:
	python setup.py sdist bdist_wheel upload
