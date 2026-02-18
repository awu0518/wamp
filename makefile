include common.mk

PYTHONPATH := $(CURDIR)
export PYTHONPATH

# Our directories
API_DIR = server
DB_DIR = data
SEC_DIR = security
REQ_DIR = .
CITIES_DIR = cities
COUNTRIES_DIR = countries
STATES_DIR = states
USERS_DIR = users

FORCE:

prod: all_tests github

github: FORCE
	- git commit -a
	git push origin master

all_tests: FORCE
	cd $(API_DIR); make tests
	cd $(SEC_DIR); make tests
	cd $(CITIES_DIR); make tests
	cd $(COUNTRIES_DIR); make tests
	cd $(STATES_DIR); make tests
	cd $(USERS_DIR); make tests
	# cd $(DB_DIR); make tests

dev_env: FORCE
	pip install -r $(REQ_DIR)/requirements-dev.txt
	@echo "You should set PYTHONPATH to: "
	@echo $(shell pwd)

docs: FORCE
	cd $(API_DIR); make docs
