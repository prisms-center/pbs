# pbs python-based PBS job submission and management

VERS=$$(git rev-parse --abbrev-ref HEAD)
ID=$$(git rev-parse HEAD)
URL=$$(git config --get remote.origin.url)
ifeq "$(BIN)" ""
	INSTALL = /usr/local/bin
else
	INSTALL = $(BIN)
endif


all: setup.py

install: setup.py
ifeq "$(PYINSTALL)" ""
	python setup.py install
else
	python setup.py install --prefix=$(PYINSTALL)
endif
	install scripts/pstat $(INSTALL)
	install scripts/psub $(INSTALL)
	install scripts/taskmaster $(INSTALL)

uninstall:
	pip uninstall pbs
	@echo "Finish uninstalling by removing the pbs package directory from the above path"
	rm -f $(INSTALL)/pstat
	rm -f $(INSTALL)/psub
	rm -f $(INSTALL)/taskmaster

clean:
	rm -f setup.py pbs/__init__.py
	rm -rf build

setup.py:
	@sed "s/VERSION_ID/$(VERS)/" pbs/__init__.template.py > pbs/__init__.tmp.0
	@sed "s/COMMIT_ID/$(ID)/" pbs/__init__.tmp.0 > pbs/__init__.py; rm pbs/__init__.tmp.0 
	@sed "s/VERSION_ID/$(VERS)/" setup.template.py > setup.tmp.0
	@sed "s/COMMIT_ID/$(ID)/" setup.tmp.0 > setup.tmp.1; rm setup.tmp.0
	@sed "s|REPO_URL|$(URL)|" setup.tmp.1 > setup.py; rm setup.tmp.1	
