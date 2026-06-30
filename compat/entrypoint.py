"""CLI entrypoint for dlt-meta compatibility wrapper.

This redirects all commands to databricks.labs.sdp_meta with deprecation warnings.
"""
import sys
import warnings


def main(raw):
    """Main CLI entry point with deprecation warning."""
    # Show deprecation warning
    print("\n" + "=" * 60)
    print("DEPRECATION NOTICE: 'dlt-meta' CLI is deprecated.")
    print("Please use 'databricks labs sdp-meta' instead.")
    print("=" * 60 + "\n")

    warnings.warn(
        "'databricks labs dlt-meta' is deprecated. "
        "Please use 'databricks labs sdp-meta' instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # Forward to the actual CLI implementation
    from databricks.labs.sdp_meta.cli import main as sdp_meta_main
    return sdp_meta_main(raw)


if __name__ == "__main__":
    main(*sys.argv[1:])
