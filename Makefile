clean:
	rm -fr build .databricks databricks_labs_sdp_meta.egg-info

dev:
	python3 -m venv .databricks
	.databricks/bin/python -m pip install -e .