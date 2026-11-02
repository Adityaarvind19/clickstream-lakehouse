PY ?= python3
EVENTS ?= 10000000

.PHONY: install data pipeline warehouse benchmark report test all clean

install:
	$(PY) -m pip install -r requirements.txt

data:            ## generate synthetic raw events (landing zone)
	$(PY) -m generator.generate --events $(EVENTS)

pipeline:        ## bronze -> silver -> gold -> data-quality gate
	$(PY) -m pipeline.run all

warehouse:       ## DuckDB views over gold + analytics SQL
	$(PY) -m pipeline.warehouse

benchmark:       ## storage + query benchmarks
	$(PY) -m pipeline.benchmark

report:          ## HTML dashboard -> data/reports/dashboard.html
	$(PY) -m pipeline.report

test:
	$(PY) -m pytest -q tests

all: data pipeline warehouse benchmark report

clean:
	rm -rf data spark-warehouse metastore_db derby.log
