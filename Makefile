.PHONY: clean dev docs-install docs-build

clean:
	rm -fr build .databricks databricks_labs_sdp_meta.egg-info

dev:
	python3 -m venv .databricks
	.databricks/bin/python -m pip install -e .

docs-install:
	cd docs && npm ci --no-audit --no-fund
	test -x docs/node_modules/.bin/docusaurus

docs-build:
	cd docs && npm run build
