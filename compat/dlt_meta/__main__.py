"""Entry point for dlt_meta package (compatibility wrapper).

This redirects to databricks.labs.sdp_meta.__main__ with a deprecation warning.
"""
import warnings

warnings.warn(
    "Running 'python -m dlt_meta' is deprecated. "
    "Please use 'python -m databricks.labs.sdp_meta' or migrate to databricks-labs-sdp-meta package.",
    DeprecationWarning,
    stacklevel=2
)

# Import and run the main module from databricks.labs.sdp_meta
from databricks.labs.sdp_meta.__main__ import main

if __name__ == "__main__":
    main()
