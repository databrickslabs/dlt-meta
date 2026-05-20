"""Installation script for dlt-meta compatibility wrapper.

This redirects to the databricks-labs-sdp-meta installation with a deprecation notice.
"""
import logging
import warnings

logger = logging.getLogger("databricks.labs.dlt-meta.install")


def main():
    """Main installation entry point."""
    warnings.warn(
        "\n" + "=" * 60 + "\n"
        "DEPRECATION NOTICE: 'dlt-meta' is deprecated.\n"
        "Please migrate to 'sdp-meta':\n"
        "  databricks labs install sdp-meta\n"
        "=" * 60,
        DeprecationWarning,
        stacklevel=2
    )

    print("\n" + "=" * 60)
    print("DEPRECATION NOTICE")
    print("=" * 60)
    print("The 'dlt-meta' package is deprecated.")
    print("Please migrate to 'sdp-meta' for new features and support.")
    print("")
    print("To install the new package:")
    print("  databricks labs install sdp-meta")
    print("")
    print("Your existing configurations will continue to work.")
    print("=" * 60 + "\n")

    # Proceed with installation by calling sdp_meta install
    try:
        from databricks.labs.sdp_meta.install import WorkspaceInstaller
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient(product="dlt-meta")  # Keep old product name for backwards compat
        installer = WorkspaceInstaller(ws)
        installer.run()
    except Exception as e:
        logger.error(f"Installation failed: {e}")
        raise


if __name__ == "__main__":
    main()
