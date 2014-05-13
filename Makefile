# pbs python-based PBS job submission and management

VERS="develop"
ID=$$(git rev-parse HEAD)
URL=$$(git config --get remote.origin.url)



all: setup.py

install: setup.py
ifeq "$(PYINSTALL)" ""
	python setup.py install
else
	python setup.py install --prefix=$(PYINSTALL)
endif

uninstall:
	pip uninstall pbs

clean:
	rm -f setup.py pbs/__init__.py
	rm -rf build

setup.py:
	@sed "s/VERSION_ID/$(VERS)/" pbs/__init__.template.py > pbs/__init__.tmp.0
	@sed "s/COMMIT_ID/$(ID)/" pbs/__init__.tmp.0 > pbs/__init__.py; rm pbs/__init__.tmp.0 
	@sed "s/VERSION_ID/$(VERS)/" setup.template.py > setup.tmp.0
	@sed "s/COMMIT_ID/$(ID)/" setup.tmp.0 > setup.tmp.1; rm setup.tmp.0
	@sed "s|REPO_URL|$(URL)|" setup.tmp.1 > setup.py; rm setup.tmp.1	
