SHELL = /bin/sh
.DEFAULT_GOAL=all

DB_CONTAINER = omnisci

-include .env

# see docs/source/contributing.rst
init:
	conda env create -f environment.yml
.PHONY: init

init.gpu:
	conda env create -f environment_gpu.yml
.PHONY: init.gpu

develop:
	pip install -e '.[dev]'
	pre-commit install
.PHONY: develop

start:
	docker run -d --rm --name ${DB_CONTAINER} \
		--ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cpu \
		/omnisci/startomnisci --non-interactive \
		--data /omnisci-storage/data --config /omnisci-storage/omnisci.conf \
		--enable-runtime-udf --enable-table-functions
.PHONY: start

start.gpu:
	docker run -d --rm --name ${DB_CONTAINER} \
		--ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cuda \
		/omnisci/startomnisci --non-interactive \
		--data /omnisci-storage/data --config /omnisci-storage/omnisci.conf \
		--enable-runtime-udf --enable-table-functions
.PHONY: start.gpu

stop:
	docker stop ${DB_CONTAINER}
.PHONY: stop

down:
	docker rm -f ${DB_CONTAINER}
.PHONY: down

install:
	pip install -e .
.PHONY: install

build:
	python setup.py build
	# pip install -e .
.PHONY: build

check:
	flake8
 .PHONY: check

test:
	pytest
.PHONY: test

clean:
	python setup.py clean
.PHONY: clean

test_all: install_conda start check test down clean
.PHONY: test_all

all: build
.PHONY: all
