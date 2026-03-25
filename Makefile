PYTHON ?= python3

ISO_CONFIGS = config/data_sources.yml config/caiso.yml config/pjm.yml config/miso.yml config/spp.yml config/nyiso.yml config/isone.yml

.PHONY: all all-regions ingest ingest-real ingest-hybrid transform forecast queue markets finance charts dashboard qa status status-all clean test

# Default: run ERCOT only then build combined dashboard
all: ingest transform forecast queue markets finance charts dashboard qa

# Run full pipeline for all ISOs + build combined dashboard
all-regions:
	@for cfg in $(ISO_CONFIGS); do \
		echo "\n=== Pipeline: $$cfg ==="; \
		$(PYTHON) -m energy_analytics ingest --config $$cfg; \
		$(PYTHON) -m energy_analytics transform --config $$cfg; \
		$(PYTHON) -m energy_analytics forecast --config $$cfg; \
		$(PYTHON) -m energy_analytics queue --config $$cfg; \
		$(PYTHON) -m energy_analytics markets --config $$cfg; \
		$(PYTHON) -m energy_analytics finance --config $$cfg; \
		$(PYTHON) -m energy_analytics charts --config $$cfg; \
		$(PYTHON) -m energy_analytics qa --config $$cfg; \
	done
	$(PYTHON) -m energy_analytics dashboard

# Per-ISO pipeline targets (ERCOT default)
ingest:
	$(PYTHON) -m energy_analytics ingest

ingest-real:
	$(PYTHON) -m energy_analytics ingest-real

ingest-hybrid:
	$(PYTHON) -m energy_analytics ingest-hybrid

transform:
	$(PYTHON) -m energy_analytics transform

forecast:
	$(PYTHON) -m energy_analytics forecast

queue:
	$(PYTHON) -m energy_analytics queue

markets:
	$(PYTHON) -m energy_analytics markets

finance:
	$(PYTHON) -m energy_analytics finance

charts:
	$(PYTHON) -m energy_analytics charts

dashboard:
	$(PYTHON) -m energy_analytics dashboard

qa:
	$(PYTHON) -m energy_analytics qa

status:
	$(PYTHON) -m energy_analytics status

status-all:
	$(PYTHON) -m energy_analytics status-all

test:
	$(PYTHON) -m unittest discover -s tests -q

clean:
	rm -f data/raw/*.csv data/staged/*.csv data/curated/*.csv data/curated/*.parquet
	rm -f data/marts/*.csv
	rm -f reports/charts/*.svg reports/qa_report.md reports/ingestion_metadata.log
	rm -f reports/market_findings.md reports/*_market_findings.md reports/*_qa_report.md reports/*_ingestion_metadata.log
	rm -f reports/dashboard/*.html reports/*.json
