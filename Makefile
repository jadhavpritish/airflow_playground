SHELL := /bin/bash

.PHONY = init clean

LABS_LAUNCH_DIRECTORY?=$$(pwd)

clean: ## Clean

	# git gc is really just minor tidying - https://git-scm.com/docs/git-gc
	git gc --aggressive


init: clean
	poetry run pip install --upgrade pip

	@# If you are having problems running this on Big Sur, try prepending the following:  SYSTEM_VERSION_COMPAT=1
	poetry install 