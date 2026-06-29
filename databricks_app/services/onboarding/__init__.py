"""Onboarding-template support helpers.

Pure-Python modules used by ``routes/onboarding.py`` (and re-exported
by ``app.py`` for back-compat with tests):

  * ``bundled_specs``     \u2014 the curated demo registry + ``_list_bundled_specs``
  * ``path_resolver``     \u2014 ``_resolve_local_onboarding_path`` + ``_preflight_parse_onboarding``
  * ``env_validation``    \u2014 ``_detect_env_suffixes`` + ``_verify_env_matches_template``
  * ``required_files``    \u2014 ``_extract_required_files`` + ``_check_required_files_existence``
"""
