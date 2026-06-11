# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Connect Database Demo
# MAGIC
# MAGIC ## Create LFC pipeline(s):
# MAGIC 1. Select a serverless or shared cluster for the notebook
# MAGIC 2. Select source database connection name
# MAGIC 3. Select cdc_qbc pipeline type 
# MAGIC    - CDC (change data capture where gateway and ingestion run as two separate pipelines) 
# MAGIC    - CDC_SINGLE_PIPELINE (CDC where gateway and ingestion are in a single pipeline)
# MAGIC    - QBC (query based)
# MAGIC 4. Set trigger_interval_min
# MAGIC    - 0 = continuous (only supported for CDC and CDC_SINGLE_PIPELINE)
# MAGIC    - 5 = 5 min job trigger
# MAGIC 5. Click `Run all`
# MAGIC
# MAGIC ## Once the pipelines are created:
# MAGIC 1. Click Connection, Schema, Jobs, Pipelines URLs at the end of the notebook.
# MAGIC 2. Auto clean up after 1 hour.
# MAGIC    - Auto delete successfully created schema, pipeline(s), job(s)
# MAGIC    - Run execute_queued_functions() to cleanup NOW instead of waiting
# MAGIC    - **run disable_cleanup() to NOT CLEANUP**
# MAGIC
# MAGIC **Remember to delete/check schema(s) pipeline(s) and jobs(s) at the end of the day**
# MAGIC
# MAGIC **QBC will see update every two minutes.** DML updates one table per minute. lfcddemo schema has two tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup 

# COMMAND ----------

%pip install --quiet lfcdemolib==0.0.13

# COMMAND ----------

dbutils.widgets.dropdown("connection", choices=[
    'lfcddemo-azure-sqlserver',
    'lfcddemo-azure-mysql',
    'lfcddemo-azure-pg'
    ],
    defaultValue="lfcddemo-azure-sqlserver")

dbutils.widgets.dropdown("cdc_qbc", choices=[
    'cdc', 'qbc', 'cdc_single_pipeline'],
    defaultValue="cdc")

dbutils.widgets.text("trigger_interval_min",
    defaultValue="5")

dbutils.widgets.text("target_catalog", defaultValue="", label="target_catalog")
dbutils.widgets.text("source_schema", defaultValue="lfcddemo", label="source_schema")
dbutils.widgets.text("run_id", defaultValue="", label="run_id")
dbutils.widgets.text("sequence_by_pk", defaultValue="false", label="sequence_by_pk")
dbutils.widgets.text("downstream_job_id", defaultValue="", label="downstream_job_id")

# COMMAND ----------

# will result in config after verification
_target_catalog = dbutils.widgets.get("target_catalog").strip() or None
_source_schema = dbutils.widgets.get("source_schema").strip() or None
config_dict={
    # required
    "source_connection_name": dbutils.widgets.get("connection"),
    "cdc_qbc": dbutils.widgets.get("cdc_qbc"),
    "trigger_interval_min": dbutils.widgets.get("trigger_interval_min"),

    # optional — overridable via job base_parameters or notebook widgets
    "target_catalog": _target_catalog,  # defaults to main. catalog must exist.
    "source_schema": _source_schema,    # defaults to lfcddemo. schema and tables will be created if does not exist.
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper code.  DO NOT CHANGE
# MAGIC - cleanup all created objects after 1 hour
# MAGIC - dml every 1 minute for 1 hour, 10 delete, 10 update, 10 insert a table

# COMMAND ----------

import lfcdemolib, json, pandas, random, sqlalchemy as sa
# Default: reinitialize on each rerun (development workflow)
d, config, dbxs, dmls, dbx_key, dml_key, scheduler = lfcdemolib.unpack_demo_instance(config_dict, dbutils, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo Sequence

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQLAlchemy to set pg slots and publication, display tables, column, sample data

# COMMAND ----------

print(f"{dml_key=}")
dml_generator = dmls[dml_key]
sqlalchemy_engine = dml_generator.engine
schema = dml_generator.schema


with sqlalchemy_engine.connect() as conn:
    # pg requires slot creation for now starting 1/31/2026
    if d.source_type.startswith("postgres") and config.cdc_qbc == "cdc":

        print(f"cleaning unused postgres replication slots")
        conn.execute(sa.text(f"""
            SELECT pg_drop_replication_slot(slot_name)
            FROM pg_replication_slots
            WHERE active = false 
            AND inactive_since IS NOT NULL
            AND inactive_since < NOW() - INTERVAL '24 hours';
        """))

        print(f"cleaning orphaned postgres publications")
        conn.execute(sa.text(f"""
            DO $$
            DECLARE
                pub_record RECORD;
                has_active_slots BOOLEAN;
            BEGIN
                -- Check if there are any active slots at all
                SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE active = true) INTO has_active_slots;
                
                -- Only drop publications if no active slots exist
                IF NOT has_active_slots THEN
                    FOR pub_record IN 
                        SELECT pubname
                        FROM pg_publication
                        WHERE pubname LIKE 'dbx_pub_%' OR pubname LIKE '%_pub'
                    LOOP
                        RAISE NOTICE 'Dropping publication: %', pub_record.pubname;
                        EXECUTE format('DROP PUBLICATION IF EXISTS %I', pub_record.pubname);
                    END LOOP;
                END IF;
            END $$;
        """))

        print(f"creating postgres replication slot and publication")
        
        conn.execute(sa.text(f"CREATE PUBLICATION {d.target_schema}_pub FOR table {schema}.intpk, {schema}.dtix"))
        conn.execute(sa.text(f"SELECT 'init' FROM pg_create_logical_replication_slot('{d.target_schema}', 'pgoutput')"))

        replication_slots_query = sa.text(f"SELECT * FROM pg_replication_slots order by slot_name")
        replication_slots_result = conn.execute(replication_slots_query)
        replication_slots = pandas.DataFrame(replication_slots_result.fetchall(), columns=replication_slots_result.keys())

        publication_query = sa.text(f"SELECT * FROM pg_publication order by pubname")
        publication_result = conn.execute(publication_query)
        publication_slots = pandas.DataFrame(publication_result.fetchall(), columns=publication_result.keys())
        display(replication_slots)
        display(publication_slots)

        def cleanup_pg_publication_and_slot(sqlalchemy_engine, d):
            """Cleanup function to drop PostgreSQL publication and replication slot"""
            try:
                with sqlalchemy_engine.connect() as conn:
                    # Drop publication
                    conn.execute(sa.text(f"DROP PUBLICATION IF EXISTS {d.target_schema}_pub CASCADE"))
                    # Drop replication slot
                    conn.execute(sa.text(f"SELECT pg_drop_replication_slot('{d.target_schema}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{d.target_schema}')"))
                    conn.commit()
                print(f"✅ Cleaned up PostgreSQL publication and slot for {d.target_schema}")
            except Exception as e:
                print(f"⚠️  Error cleaning up PostgreSQL resources: {e}")
        dbxs[dbx_key].cleanup_queue.put((cleanup_pg_publication_and_slot, (sqlalchemy_engine, d,), {}))
         
    # Query tables using SQLAlchemy
    tables_query = sa.text(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}'")
    tables_result = conn.execute(tables_query)
    tables = pandas.DataFrame(tables_result.fetchall(), columns=[key.upper() for key in tables_result.keys()])
    
    if not tables.empty:
        first_table_name = tables["TABLE_NAME"].iloc[0]
        
        # Query columns using SQLAlchemy
        try:
            columns_query = sa.text(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{first_table_name}'")
            columns_result = conn.execute(columns_query)
            columns = pandas.DataFrame(columns_result.fetchall(), columns=columns_result.keys())
        except Exception:
            columns = None
        
        # Query sample data using SQLAlchemy
        try:
            sample_query = sa.text(f"SELECT * FROM {schema}.{first_table_name} WHERE DT = (SELECT MIN(DT) FROM {schema}.{first_table_name})")
            sample_result = conn.execute(sample_query)
            sample_data = pandas.DataFrame(sample_result.fetchall(), columns=sample_result.keys())
        except Exception:
            sample_data = None
    else:
        columns = None
        sample_data = None

display(tables)
display(columns)
display(sample_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema holding target streaming tables

# COMMAND ----------

# create schema and tag if does not exist
schema_response=d.schema_create(d.target_catalog, d.target_schema, print_response=False) 
schema_tags_response=d.schema_tags(d.target_schema_path, print_response=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gateway pipeline for CDC only

# COMMAND ----------

# ── Name prefix (mirrors launch_lfc_demo.py; change here for a full rename) ──
_DEMO_SLUG   = "sdp-meta-lfc"  # hyphenated  → job/pipeline names
_DEMO_PREFIX = "sdp_meta"      # underscored → UC schema names, volume paths

# If lfc_created.json exists for this run, pipelines (and scheduler job) are already created; reuse and do not create again.
import json
_lfc_reuse = False
_lfc_created = {}
_run_id = (dbutils.widgets.get("run_id") or "").strip()
_catalog = getattr(d, "target_catalog", None) or ""
if _run_id and _catalog:
    try:
        _vol_path = f"/Volumes/{_catalog}/{_DEMO_PREFIX}_dataflowspecs_lfc_{_run_id}/{_catalog}_lfc_volume_{_run_id}/conf/lfc_created.json"
        _content = dbutils.fs.head(_vol_path)
        _lfc_created = json.loads(_content)
        if _lfc_created.get("gw_pipeline_id") and _lfc_created.get("ig_pipeline_id"):
            _lfc_reuse = True
            gw_response_json = {"pipeline_id": _lfc_created["gw_pipeline_id"]}
            ig_response_json = {"pipeline_id": _lfc_created["ig_pipeline_id"]}
            ig_jobs_response_json = {"job_id": _lfc_created.get("lfc_scheduler_job_id")} if _lfc_created.get("lfc_scheduler_job_id") else {}
            print("Reusing existing LFC pipelines and job from lfc_created.json; skipping gateway/ingestion/job creation.")
    except Exception:
        pass

# gw pipeline spec
gw_pipeline_spec = {
    "name": d.gw_pipeline_name,
    "gateway_definition": {
        "connection_name": d.connection_name,
        "gateway_storage_catalog": d.target_catalog,
        "gateway_storage_schema": d.target_schema,
    },
    "tags": {"RemoveAfter": d.remove_after_yyyymmdd, "Connector": d.source_type},
}

if not _lfc_reuse and config.cdc_qbc == 'cdc':
    gw_response=d.create_pipeline(json.dumps(gw_pipeline_spec))
    gw_response_json=gw_response.json()
elif not _lfc_reuse:
    gw_response=""
    gw_response_json={'pipeline_id':None}    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion pipeline for CDC, CDC_SINGLE_PIPELINE and QBC
# MAGIC - Oracle uppercase by default
# MAGIC - Postgres lowercase by default
# MAGIC - SQL Server case sensitivity
# MAGIC - MySQL case sensitivity usually.  no catalog.

# COMMAND ----------

# ig pipeline spec
ig_pipeline_spec = {
    "name": d.ig_pipeline_name,
    'catalog': d.target_catalog,
    'schema': d.target_schema,
    "pipeline_type": 
        'MANAGED_INGESTION' if config.cdc_qbc in ['cdc_single_pipeline'] 
        else None,    
    "configuration": {
        "pipelines.directCdc.minimumRunDurationMinutes": "1",
        "pipelines.directCdc.enableBoundedContinuousGraphExecution": True
    } if config.cdc_qbc in ['cdc_single_pipeline'] 
        else None,    
  'development': True,
  'serverless': 
    # cdc_single_pipeline needs to be classic compute for now
    True if config.cdc_qbc in ['cdc', 'qbc'] 
    else False,
  'continuous': 
    True if config.trigger_interval_min in ['0']  
    else False,       
    "ingestion_definition": {
        "ingestion_gateway_id": 
            gw_response_json["pipeline_id"] if config.cdc_qbc in ["cdc"]
            else None, 
        "connection_name": 
            d.connection_name if config.cdc_qbc in ["qbc", "cdc_single_pipeline"]  
            else None,
        "connector_type": 
            "CDC" if config.cdc_qbc in ["cdc_single_pipeline"] 
            else None,
        "source_type": d.source_type.upper(),
        "source_configurations":     
            [ {
                "catalog": {
                    "source_catalog": d.source_catalog,
                    "postgres": {
                        "slot_config": {
                        "slot_name": f"{d.target_schema}",
                        "publication_name": f"{d.target_schema}_pub",
                        }
                    }
                }
            }] if d.source_type.startswith("postgres") and config.cdc_qbc in ["cdc_single_pipeline", "cdc"]
            else None,
        "objects": [
            {
                "table": {
                    "source_catalog": 
                        None if d.source_type.startswith("mysql") 
                        else d.source_catalog.upper() if d.source_type.startswith("ora") 
                        else d.source_catalog, 
                    "source_schema": 
                        d.source_schema.upper() if d.source_type.startswith("ora") 
                        else d.source_schema,  
                    "source_table": 
                        "intpk".upper() if d.source_type.startswith("ora") 
                        else "intpk", 
                    "destination_catalog": d.target_catalog,
                    "destination_schema": d.target_schema,
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_1",
                        "query_based_connector_config": {
                            "cursor_columns": [
                                "dt".upper() if d.source_type.startswith('ora') 
                                else 'dt',
                            ]
                        } if config.cdc_qbc == 'qbc' else None,
                    },
                },
            },
            {
                "table": {
                    "source_catalog": 
                        None if d.source_type.startswith("mysql") 
                        else d.source_catalog.upper() if d.source_type.startswith("ora") 
                        else d.source_catalog, 
                    "source_schema": 
                        d.source_schema.upper() if d.source_type.startswith("ora") 
                        else d.source_schema, 
                    "source_table": 
                        "dtix".upper() if d.source_type.startswith("ora") 
                        else "dtix", 
                    "destination_catalog": d.target_catalog,
                    "destination_schema": d.target_schema,
                    "table_configuration": {
                        "scd_type": "SCD_TYPE_2",
                    },
                },
            } if config.cdc_qbc in ['cdc','cdc_single_pipeline'] and d.secrets_json.get('replication_mode') != 'ct' 
              else None
        ],
    },
}

if not _lfc_reuse:
    ig_response=d.create_pipeline(json.dumps(ig_pipeline_spec))
    ig_response_json=ig_response.json()
    
    # Check if slot_config is not allowed and retry without it
    if 'error_code' in ig_response_json:
        error_reason = ig_response_json.get('details', [{}])[0].get('reason', '') if isinstance(ig_response_json.get('details'), list) else ''
        
        if 'POSTGRES_SLOT_CONFIG_NOT_ALLOWED' in error_reason:
            print("⚠️  Slot config not allowed, retrying without slot_config...")
            
            # Remove slot_config from source_configurations
            if ig_pipeline_spec.get("ingestion_definition", {}).get("source_configurations"):
                for src_config in ig_pipeline_spec["ingestion_definition"]["source_configurations"]:
                    if "catalog" in src_config and "postgres" in src_config["catalog"]:
                        del src_config["catalog"]["postgres"]["slot_config"]
            
            # Retry pipeline creation
            ig_response = d.create_pipeline(json.dumps(ig_pipeline_spec))
            ig_response_json = ig_response.json()
            print("✅ Pipeline created without slot_config")
else:
    pass  # ig_response_json set in gateway cell when reusing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job to trigger ingestion pipeline

# COMMAND ----------

# run starting on random minute {random.randint(1, 5)}/ every 5 min
if config.trigger_interval_min == "0":
    pass
    # continuous will autostart and do not need a separate method
    #try:
    #    d.start_pipeline(ig_response_json['pipeline_id'],full_refresh=False)
    #except Exception as e:
    #    print("Manually start the pipeline from the UI.", e)
elif _lfc_reuse:
    pass  # ig_jobs_response_json already set in gateway cell when reusing
else:
    ig_job_spec={
        "name": f"{d.ig_pipeline_name}_{ig_response_json['pipeline_id']}",
        "performance_target": "standard",
        "schedule": {
            "timezone_id":"UTC", 
            "quartz_cron_expression": f"0 {random.randint(1, 5)}/{config.trigger_interval_min} * * * ?"},
            "tasks": [ {
                "task_key":"run_dlt", 
                "pipeline_task":{"pipeline_id": ig_response_json['pipeline_id']} 
            } ],
        "tags": {"RemoveAfter": d.remove_after_yyyymmdd, "Connector": d.source_type},
    }

    ig_jobs_response_json = {}
    try:
        ig_jobs_response=d.jobs_create(json.dumps(ig_job_spec))
        ig_jobs_response_json=ig_jobs_response.json()

        ig_jobs_runnow_response=d.jobs_runnow(ig_jobs_response_json['job_id'])
        ig_jobs_runnow_response_json=ig_jobs_runnow_response.json()

    except Exception as e_job_create:
        print("Trying manual start as job creation failed.", e_job_create)
        ig_jobs_response_json.update({'job_id': None})
        try:
            d.start_pipeline(ig_response_json['pipeline_id'],full_refresh=False)
        except Exception as e_start_pipeline:
            print("Manual start failed.  Please start the pipeline from the UI.", e_start_pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse connection, schema, pipeline(s), job(s)

# COMMAND ----------

print(f"""
connection: {d.workspace_url}/explore/connections/{d.connection_name}
target_schema: {d.workspace_url}/explore/data/{d.target_catalog}/{d.target_schema}
ingestion pipeline: {d.workspace_url}/pipelines/{ig_response_json["pipeline_id"]}
""")

print(f"""
ingestion job: {d.workspace_url}/jobs/{ig_jobs_response_json["job_id"]}
""") if config.trigger_interval_min != "0" and ig_jobs_response_json["job_id"] is not None else print()

print(f"""
gateway pipeline: {d.workspace_url}/pipelines/{gw_response_json["pipeline_id"]}
gateway_volume: {d.workspace_url}/explore/data/volumes/{d.target_catalog}/{d.target_schema}/__databricks_ingestion_gateway_staging_data-{gw_response_json["pipeline_id"]}
""") if config.cdc_qbc == 'cdc' else print()

# COMMAND ----------

# Write LFC-created resources to the run's volume so cleanup_lfc_demo.py can scope deletion to this run.
# Also overwrite onboarding.json with the correct source_database = d.target_schema (the schema where
# LFC actually created intpk/dtix), so onboarding_job and bronze pipeline read from the right place.
# Write before waiting — cleanup can then delete pipelines/schema even while they are still coming up.
import json
_run_id = (dbutils.widgets.get("run_id") or "").strip()
_catalog = (dbutils.widgets.get("target_catalog") or "").strip()
_LFC_TABLES = ["intpk", "dtix"]
if _run_id and _catalog:
    _job_id = None
    try:
        _job_id = ig_jobs_response_json.get("job_id")
    except NameError:
        pass
    _vol_prefix = f"/Volumes/{_catalog}/{_DEMO_PREFIX}_dataflowspecs_lfc_{_run_id}/{_catalog}_lfc_volume_{_run_id}"
    _vol_conf = f"{_vol_prefix}/conf"
    # When reusing pipelines, keep same schema/catalog as when created (don't overwrite with new d.target_schema).
    _lfc_catalog = (_lfc_created.get("target_catalog") if (_lfc_reuse and _lfc_created) else None) or _catalog
    _lfc_schema = (_lfc_created.get("lfc_schema") if (_lfc_reuse and _lfc_created) else None) or d.target_schema
    dbutils.fs.put(f"{_vol_conf}/lfc_created.json", json.dumps({
        "target_catalog": _lfc_catalog,
        "lfc_schema": _lfc_schema,
        "gw_pipeline_id": gw_response_json.get("pipeline_id"),
        "ig_pipeline_id": ig_response_json.get("pipeline_id"),
        "lfc_scheduler_job_id": _job_id,
    }, indent=2), overwrite=True)
    print(f"Wrote {_vol_conf}/lfc_created.json for run-scoped cleanup.")
    # Overwrite onboarding.json so source_database = d.target_schema (LFC-created schema), not source_schema widget
    # Demo: intpk = readChangeFeed + CDC SCD1; dtix = readChangeFeed + CDC SCD2 (accurate __END_AT)
    _bronze_schema = f"{_DEMO_PREFIX}_bronze_lfc_{_run_id}"
    _silver_schema = f"{_DEMO_PREFIX}_silver_lfc_{_run_id}"
    _intpk_cdc = {
        "keys": ["pk"],
        "sequence_by": "_commit_version",
        "scd_type": "1",
        "apply_as_deletes": "_change_type = 'delete'",
        "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
    }
    # ── No-PK SCD Type 2 helpers ─────────────────────────────────────────────────
    # When a source table has NO primary key, Lakeflow Connect uses ALL source
    # columns as the implicit composite PK and tracks row history with
    # __start_at / __end_at.  The correct DLT-Meta CDC config is:
    #
    #   keys       = all source columns + "__start_at"
    #                  → one unique key per LFC row-version; __start_at
    #                    distinguishes versions of the same logical row.
    #   scd_type   = "1"
    #                  → DLT does NOT add its own __START_AT/__END_AT on top of
    #                    LFC's columns; versioning is already encoded in the key.
    #   sequence_by (bronze) = "_commit_version"
    #                  → Always non-null from readChangeFeed; correctly sequences
    #                    the INSERT (end_at=NULL) and the later UPDATE that sets
    #                    __end_at when LFC closes a version.  Using __end_at
    #                    directly would fail because DLT rejects NULL sequence
    #                    values, and active rows always have __end_at = NULL.
    #   sequence_by (silver) = "__start_at"
    #                  → Always non-null; unique per version; equivalent ordering
    #                    to "most recent __end_at" (newer versions always have a
    #                    later __start_at).

    def _get_no_pk_scd2_keys(engine, schema, table_name):
        """Return ordered source column names from INFORMATION_SCHEMA for a no-PK SCD2 table."""
        try:
            with engine.connect() as _conn:
                _result = _conn.execute(sa.text(
                    f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table_name}' "
                    f"ORDER BY ORDINAL_POSITION"
                ))
                return [row[0] for row in _result.fetchall()]
        except Exception as _e:
            print(f"Warning: could not fetch columns for {schema}.{table_name}: {_e}")
            return []

    def _no_pk_scd2_bronze_cdc(source_cols):
        """Bronze CDC config for LFC SCD Type 2 with no primary key.

        keys = source_cols + ["__start_at"]: uniquely identifies each row-version.
        scd_type = "1": DLT applies UPDATEs/INSERTs in-place; LFC's __end_at is
          the authoritative history column — DLT does not add duplicate SCD2 cols.
        sequence_by = "_commit_version": non-null CDF field; the UPDATE event that
          sets __end_at always has a higher commit_version than the original INSERT,
          so the final __end_at value is correctly preserved (most-recent wins).
        """
        return {
            "keys": source_cols + ["__start_at"],
            "sequence_by": "_commit_version",
            "scd_type": "1",
            "apply_as_deletes": "_change_type = 'delete'",
            "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
        }

    def _no_pk_scd2_silver_cdc(source_cols):
        """Silver CDC config for LFC SCD Type 2 with no primary key.

        keys = source_cols + ["__start_at"]: same composite key as bronze.
        scd_type = "1": LFC's __end_at is already authoritative.
        sequence_by = "__start_at": always non-null; each row-version has a unique
          __start_at that is monotonically increasing per logical row — equivalent
          to ordering by "most recent __end_at" since newer versions always start
          later.  Using __end_at here would fail because active rows have
          __end_at = NULL and DLT rejects NULL sequence values.
        """
        return {
            "keys": source_cols + ["__start_at"],
            "sequence_by": "__start_at",
            "scd_type": "1",
        }

    # dtix: LFC SCD2 table has __START_AT/__END_AT which DLT reserves globally as system
    # column names.  For apply_changes (CDF) DLT raises DLTAnalysisException; for
    # apply_changes_from_snapshot it silently strips them, making them unresolvable as keys.
    # Solution: use apply_changes_from_snapshot (batch snapshot, not CDF) AND rename the
    # reserved columns via a bronze_custom_transform in init_sdp_meta_pipeline.py:
    #   __START_AT → lfc_start_at,  __END_AT → lfc_end_at
    # Key choice — why (dt, lfc_end_at) and not (dt, lfc_start_at):
    #   LFC assigns __END_AT a unique __cdc_internal_value for every row, including
    #   initial-load rows whose __START_AT is null.  No-PK tables can have many rows
    #   with the same dt and null __START_AT, so (dt, __START_AT) is non-unique.
    #   __END_AT is null only for the single currently-active version per dt value,
    #   so (dt, __END_AT) is globally unique across both historical and active rows.
    _dtix_bronze_acfs = {"keys": ["dt", "lfc_end_at"], "scd_type": "1"}
    _dtix_silver_acfs = {"keys": ["dt", "lfc_end_at"], "scd_type": "1"}
    # Silver reads from bronze intpk via CDF (readChangeFeed=true) so that MERGE operations
    # written by bronze CDC (apply_changes) do not trigger DELTA_SOURCE_TABLE_IGNORE_CHANGES.
    # With CDF the correct sequence_by is always _commit_version (not pk or dt).
    _intpk_silver_reader_options = {"readChangeFeed": "true"}
    _intpk_silver_cdc = {
        "keys": ["pk"],
        "sequence_by": "_commit_version",
        "scd_type": "1",
        "apply_as_deletes": "_change_type = 'delete'",
        "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
    }
    _onboarding = []
    for i, tbl in enumerate(_LFC_TABLES):
        if tbl == "dtix":
            entry = {
                "data_flow_id": str(i + 1),
                "data_flow_group": "A1",
                "source_format": "snapshot",
                "source_details": {
                    "source_catalog_prod": _lfc_catalog,
                    "source_database": _lfc_schema,
                    "source_table": tbl,
                    "snapshot_format": "delta",
                },
                "bronze_database_prod": f"{_catalog}.{_bronze_schema}",
                "bronze_table": tbl,
                "bronze_apply_changes_from_snapshot": _dtix_bronze_acfs,
                "silver_database_prod": f"{_catalog}.{_silver_schema}",
                "silver_table": tbl,
                "silver_transformation_json_prod": f"{_vol_prefix}/conf/silver_transformations.json",
                "silver_apply_changes_from_snapshot": _dtix_silver_acfs,
            }
        else:
            entry = {
                "data_flow_id": str(i + 1),
                "data_flow_group": "A1",
                "source_format": "delta",
                "source_details": {
                    "source_catalog_prod": _lfc_catalog,
                    "source_database": _lfc_schema,
                    "source_table": tbl,
                },
                "bronze_database_prod": f"{_catalog}.{_bronze_schema}",
                "bronze_table": tbl,
                "bronze_reader_options": {"readChangeFeed": "true"},
                "bronze_database_quarantine_prod": f"{_catalog}.{_bronze_schema}",
                "bronze_quarantine_table": f"{tbl}_quarantine",
                "silver_database_prod": f"{_catalog}.{_silver_schema}",
                "silver_table": tbl,
                "silver_transformation_json_prod": f"{_vol_prefix}/conf/silver_transformations.json",
                "silver_data_quality_expectations_json_prod": f"{_vol_prefix}/conf/dqe/silver_dqe.json",
                "bronze_cdc_apply_changes": _intpk_cdc,
                "bronze_data_quality_expectations_json_prod": f"{_vol_prefix}/conf/dqe/bronze_dqe.json",
                "silver_reader_options": _intpk_silver_reader_options,
                "silver_cdc_apply_changes": _intpk_silver_cdc,
            }
        _onboarding.append(entry)
    dbutils.fs.put(f"{_vol_conf}/onboarding.json", json.dumps(_onboarding, indent=2), overwrite=True)
    print(f"Wrote {_vol_conf}/onboarding.json with source_database={_lfc_schema} (LFC-created schema).")
else:
    print("run_id or target_catalog not set; skipping lfc_created.json and onboarding.json write.")

# COMMAND ----------

# Wait for LFC pipelines before onboarding/bronze.
# Gateway is always continuous → RUNNING is sufficient (hardcoded).
# Ingestion: continuous (trigger_interval_min=0) → RUNNING; trigger mode → latest update COMPLETED.

import time
from databricks.sdk import WorkspaceClient as _WorkspaceClient

_ws = _WorkspaceClient()
_TIMEOUT_SEC = 1200
_POLL_SEC = 30
_continuous = (config.trigger_interval_min or "").strip() == "0"

def _is_completed_state(state):
    if not state:
        return False
    s = str(state).upper()
    return s == "COMPLETED" or s.split(".")[-1] == "COMPLETED"

def _is_running_state(state):
    if not state:
        return False
    s = str(state).upper()
    return s == "RUNNING" or s.split(".")[-1] == "RUNNING"

def _latest_update_completed(p):
    updates = p.latest_updates or []
    if not updates:
        return False
    return _is_completed_state(updates[0].state)

def _pipeline_or_update_running(p):
    """True if pipeline is RUNNING or latest update is RUNNING (for continuous mode)."""
    if "RUNNING" in str(p.state).upper():
        return True
    updates = p.latest_updates or []
    if not updates:
        return False
    return _is_running_state(updates[0].state)

def _wait_for_pipeline(pipeline_id, label, runnings_sufficient):
    """runnings_sufficient=True: exit when RUNNING (gateway is always continuous). False: exit when latest COMPLETED (ingestion trigger mode) or RUNNING (ingestion continuous)."""
    if not pipeline_id:
        print(f"  {label}: skipped (no pipeline ID)")
        return
    start = time.time()
    while True:
        elapsed = int(time.time() - start)
        p = _ws.pipelines.get(pipeline_id=pipeline_id)
        state = str(p.state)
        updates_state = [str(u.state) for u in (p.latest_updates or [])[:2]]
        print(f"  [{elapsed:>5}s] {label}: pipeline={state}  updates={updates_state}")
        if runnings_sufficient and _pipeline_or_update_running(p):
            print(f"  ✓ {label} RUNNING")
            return
        if not runnings_sufficient and _latest_update_completed(p):
            print(f"  ✓ {label} latest update COMPLETED")
            return
        _upds = p.latest_updates or []
        _latest_state = str(_upds[0].state) if _upds else None
        _terminal_ok = runnings_sufficient and any(s in state for s in ("STOPPED", "CANCELED", "DELETED"))
        if _terminal_ok:
            print(f"  ✓ {label} {state} (gateway stopped/canceled is OK)")
            return
        if any(s in state for s in ("FAILED", "STOPPED", "DELETED")) and not _is_completed_state(_latest_state):
            raise RuntimeError(f"{label} pipeline state={state}, latest update={_latest_state}")
        if elapsed >= _TIMEOUT_SEC:
            raise TimeoutError(
                f"{label} did not reach {'RUNNING' if runnings_sufficient else 'COMPLETED'} within {_TIMEOUT_SEC // 60} min"
            )
        time.sleep(_POLL_SEC)

print("Waiting for LFC pipelines (gateway: RUNNING; ingestion: RUNNING or latest COMPLETED per mode)...")
_wait_for_pipeline(gw_response_json.get("pipeline_id"), "Gateway pipeline", runnings_sufficient=True)
_wait_for_pipeline(ig_response_json.get("pipeline_id"), "Ingestion pipeline", runnings_sufficient=_continuous)
print("\nlfc_setup task complete.")

# COMMAND ----------

# Enable change data feed on intpk so DLT-Meta bronze can read CDC (readChangeFeed + bronze_cdc_apply_changes).
# When ingestion has completed (same logic as wait cell), the table exists; then run ALTER only if not already set.
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient as _WorkspaceClient

def _ingestion_ready(ws, ig_pipeline_id, continuous):
    if not ig_pipeline_id:
        return False
    p = ws.pipelines.get(pipeline_id=ig_pipeline_id)
    updates = p.latest_updates or []
    if not updates:
        return False
    state = str(updates[0].state).upper()
    if "COMPLETED" in state or state.split(".")[-1] == "COMPLETED":
        return True
    if continuous and ("RUNNING" in str(p.state).upper() or "RUNNING" in state):
        return True
    return False

# Use catalog/schema from lfc_created.json when present (same location as when pipelines were created).
# On re-run or if the file was written earlier in this run, this avoids mismatch with d.target_schema.
_catalog = getattr(d, "target_catalog", None)
_schema = getattr(d, "target_schema", None)
_run_id = (dbutils.widgets.get("run_id") or "").strip()
if _run_id and _catalog:
    try:
        _vol_path = f"/Volumes/{_catalog}/{_DEMO_PREFIX}_dataflowspecs_lfc_{_run_id}/{_catalog}_lfc_volume_{_run_id}/conf/lfc_created.json"
        _meta = json.loads(dbutils.fs.head(_vol_path))
        if _meta.get("target_catalog"):
            _catalog = _meta["target_catalog"]
        if _meta.get("lfc_schema"):
            _schema = _meta["lfc_schema"]
    except Exception:
        pass
if _catalog and _schema:
    _ig_id = ig_response_json.get("pipeline_id")
    _continuous = (config.trigger_interval_min or "").strip() == "0"
    if not _ingestion_ready(_WorkspaceClient(), _ig_id, _continuous):
        print("Ingestion pipeline not ready (need latest COMPLETED or RUNNING). Continuing to table check to capture any error.")
    _table_name = f"{_catalog}.{_schema}.intpk"
    _timeout_sec = 600
    _poll_sec = 10
    _start = time.time()
    _exists = False
    while time.time() - _start < _timeout_sec:
        try:
            spark.sql(f"SELECT 1 FROM {_table_name} LIMIT 0").collect()
            _exists = True
            break
        except Exception as _e:
            _elapsed = int(time.time() - _start)
            print(f"  {datetime.now().isoformat()} Waiting for table {_table_name} to exist ({_elapsed}s / {_timeout_sec}s)... Last error: {_e}")
            time.sleep(_poll_sec)
    if not _exists:
        _tables_in_schema = []
        try:
            _rows = spark.sql(f"SHOW TABLES IN `{_catalog}`.`{_schema}`").collect()
            _tables_in_schema = [r.tableName for r in _rows] if _rows else []
        except Exception as _e2:
            print(f"SHOW TABLES error: {_e2}")
        print(f"Table {_table_name} not found after {_timeout_sec}s. Tables in {_catalog}.{_schema}: {_tables_in_schema or '(none or schema not visible)'}. Not raising; table/properties known good.")
    else:
        try:
            _already = spark.sql(f"SHOW TBLPROPERTIES {_table_name}").filter("key = 'delta.enableChangeDataFeed'").collect()
            if _already and str(_already[0].value).lower() == "true":
                print(f"Change data feed already enabled on {_table_name}")
            else:
                try:
                    spark.sql(f"ALTER TABLE {_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
                    print(f"Enabled change data feed on {_table_name}")
                except Exception as _e3:
                    _err = str(_e3).upper()
                    if "SET_TBLPROPERTIES_NOT_ALLOWED_FOR_PIPELINE_TABLE" in _err or "INVALID_TARGET_FOR_SET_TBLPROPERTIES" in _err:
                        print(f"LFC streaming table: ALTER not allowed; delta.enableChangeDataFeed is already true by default on {_table_name}. Error: {_e3}")
                    else:
                        print(f"ALTER TBLPROPERTIES failed (not raising): {_e3}")
        except Exception as _e4:
            print(f"SHOW TBLPROPERTIES failed (not raising): {_e4}")
else:
    print("d.target_catalog and d.target_schema not set; skipping CDF check. Not raising.")

# When downstream_job_id is set (parallel_downstream mode), trigger onboarding -> bronze -> silver now;
# the notebook continues running (e.g. 1h cleanup) while that job runs in parallel.
_downstream_id = (dbutils.widgets.get("downstream_job_id") or "").strip()
if _downstream_id:
    from databricks.sdk import WorkspaceClient as _W
    _run = _W().jobs.run_now(job_id=int(_downstream_id))
    print(f"Triggered downstream job run_id={_run.run_id} (onboarding -> bronze -> silver). Notebook continues.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete schema, pipeline(s) after one hour
# MAGIC
# MAGIC Clean up control:
# MAGIC - will auto delete successfully created schema, pipeline(s), job(s)
# MAGIC - run execute_queued_functions() to cleanup NOW instead of waiting
# MAGIC - **run disable_cleanup() to NOT CLEANUP**

# COMMAND ----------

print("Currently active cleanup task(s):")
for dbx_key,dbx_val in dbxs.items():
    print(f"queue for {dbx_key=}")
    for q in list(dbx_val.cleanup_queue.queue):
        method, args, kwargs = q
        # Handle both bound methods and regular functions
        if hasattr(method, '__self__'):
            class_name = method.__self__.__class__.__name__
            method_name = method.__name__
            print(f"  {class_name}.{method_name} {args}, {kwargs}")
        else:
            # Regular function (not a bound method)
            print(f"  {method.__name__} {args}, {kwargs}")

print("\nCurrently active scheduler(s):")
scheduler.scheduler.print_jobs()

# COMMAND ----------

# uncomment to delete now instead of waiting till the end
#for dbx_key,dbx_val in dbxs.items(): dbx_val.execute_queued_functions()

# COMMAND ----------

# When parallel_downstream: do not exit while the scheduler still has jobs (e.g. 1h cleanup).
# Poll every 1 minute and exit only when there are no more jobs in the queue.
import time
_downstream_id = (dbutils.widgets.get("downstream_job_id") or "").strip()
if _downstream_id:
    while True:
        try:
            _jobs = scheduler.scheduler.get_jobs()
        except Exception:
            _jobs = []
        if not _jobs:
            print("No jobs left in scheduler queue; exiting notebook.")
            dbutils.notebook.exit("Scheduler queue empty; exiting.")
        print(f"Scheduler has {len(_jobs)} job(s) in queue; waiting 60s before recheck...")
        time.sleep(60)
