"""Tests for reusable governance tagging in the pipeline integration runner."""

import json
from types import SimpleNamespace

import pytest
import yaml

from integration_tests.governance_tagging_helper import (
    INTEGRATION_TAG_KEY,
    INTEGRATION_TAG_VALUE,
    generate_integration_tags_file,
)
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
)


def onboarding_row():
    return {
        "bronze_catalog_it": "main",
        "bronze_database_it": "bronze",
        "bronze_table": "customers",
        "silver_catalog_it": "main",
        "silver_database_it": "silver",
        "silver_table": "customers",
    }


def test_generate_integration_tags_materializes_discovered_targets(tmp_path):
    onboarding = tmp_path / "onboarding.json"
    output = tmp_path / "tags.yml"
    onboarding.write_text(json.dumps([onboarding_row()]), encoding="utf-8")

    targets = generate_integration_tags_file(
        [str(onboarding)],
        str(output),
        environment="it",
        catalog="main",
        default_schema="bronze",
        source_id="pipeline-it-tags",
    )

    assert targets == ["customers", "silver.customers"]
    document = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert document["source_id"] == "pipeline-it-tags"
    assert document["tables"] == {
        "customers": {
            "table": {INTEGRATION_TAG_KEY: INTEGRATION_TAG_VALUE}
        },
        "silver.customers": {
            "table": {INTEGRATION_TAG_KEY: INTEGRATION_TAG_VALUE}
        },
    }
    assert all(node for node in document["tables"].values())


def test_generate_integration_tags_requires_rendered_onboarding(tmp_path):
    with pytest.raises(ValueError, match="no rendered onboarding rows"):
        generate_integration_tags_file(
            [str(tmp_path / "missing.json")],
            str(tmp_path / "tags.yml"),
            environment="it",
            catalog="main",
            default_schema="bronze",
            source_id="pipeline-it-tags",
        )


def test_workflow_applies_tags_between_pipeline_and_validation():
    captured = {}

    class FakeJobs:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(job_id=1)

    runner = SDPMETARunner.__new__(SDPMETARunner)
    runner.ws = SimpleNamespace(jobs=FakeJobs())
    conf = SDPMetaRunnerConf(
        run_id="abc",
        source="multi_source_cdc",
        uc_catalog_name="main",
        sdp_meta_schema="sdp_meta_it_abc",
        bronze_schema="bronze_it_abc",
        silver_schema="silver_it_abc",
        runners_nb_path="/Workspace/runners",
        test_output_file_path="/Workspace/results.csv",
        remote_whl_path="/Volumes/main/sdp_meta/wheels/sdp_meta.whl",
        bronze_pipeline_id="pipeline-id",
        enable_governance_tagging=True,
        warehouse_id="warehouse-id",
        governance_tags_volume_path="/Volumes/main/sdp_meta/tags.yml",
    )

    runner.create_workflow_spec(conf)

    tasks = {task.task_key: task for task in captured["tasks"]}
    apply_task = tasks["apply_governance_tags"]
    assert [dependency.task_key for dependency in apply_task.depends_on] == [
        "sdp-meta-pipeline"
    ]
    assert [
        dependency.task_key
        for dependency in tasks["validate_results"].depends_on
    ] == ["apply_governance_tags"]
    assert apply_task.python_wheel_task.named_parameters == {
        "tags-file": "/Volumes/main/sdp_meta/tags.yml",
        "state-table": (
            "main.sdp_meta_it_abc.uc_governance_tag_assignments"
        ),
        "warehouse-id": "warehouse-id",
    }
