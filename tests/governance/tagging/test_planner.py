"""Tests for ownership-aware reconciliation planning."""

from databricks.labs.sdp_meta.governance.tagging.models import (
    OWN_EXTERNAL,
    OWN_SCRIPT,
    Desired,
    Key,
)
from databricks.labs.sdp_meta.governance.tagging.planner import plan


def contributor(target="main.protected.customers"):
    return ("target", target)


def key(tag_key="sensitivity"):
    return Key("main", "protected", "customers", None, tag_key)


def state(value, contributors, ownership=OWN_SCRIPT):
    return {
        "last_applied_value": value,
        "ownership": ownership,
        "contributors": set(contributors),
    }


def test_partial_selection_does_not_touch_unselected_target_state():
    selected = contributor("main.protected.customers")
    unselected = contributor("main.protected.orders")
    other_key = key("owner")
    actions = plan(
        desired={},
        actual={other_key: "team-b"},
        state={other_key: state("team-b", {unselected})},
        transfer=False,
        selected_contributors={selected},
    )
    assert actions == []


def test_removing_final_desired_tag_plans_unset():
    selected = contributor()
    tag_key = key()
    actions = plan(
        desired={},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected})},
        transfer=False,
        selected_contributors={selected},
    )
    assert [action.kind for action in actions] == ["unset"]


def test_removing_one_contributor_preserves_shared_assignment():
    selected = contributor()
    remaining = contributor("main.shared.customers")
    tag_key = key()
    actions = plan(
        desired={},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected, remaining})},
        transfer=False,
        selected_contributors={selected},
    )
    assert [action.kind for action in actions] == ["update_contributors"]
    assert actions[0].contributors == {remaining}


def test_one_source_cannot_unset_another_sources_shared_assignment():
    selected = ("target", "file-a", "main.protected.customers")
    remaining = ("target", "file-b", "main.protected.customers")
    tag_key = key()

    actions = plan(
        desired={},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected, remaining})},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["update_contributors"]
    assert actions[0].contributors == {remaining}


def test_pending_intent_does_not_overwrite_a_different_actual_value():
    selected = contributor()
    tag_key = key()
    pending = state("pii", {selected})
    pending["status"] = "pending"

    actions = plan(
        desired={tag_key: Desired("pii", {selected})},
        actual={tag_key: "confidential"},
        state={tag_key: pending},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["conflict"]


def test_absent_assignment_is_set_and_owned_by_script():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("pii", {selected})},
        actual={},
        state={},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["set"]
    assert actions[0].contributors == {selected}
    assert actions[0].ownership == OWN_SCRIPT
    assert actions[0].reason == "R1 absent"


def test_matching_preexisting_assignment_is_recorded_as_external():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("pii", {selected})},
        actual={tag_key: "pii"},
        state={},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["record_external"]
    assert actions[0].ownership == OWN_EXTERNAL
    assert actions[0].contributors == {selected}


def test_matching_tracked_assignment_is_noop_and_preserves_ownership():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("pii", {selected})},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected}, OWN_EXTERNAL)},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["noop"]
    assert actions[0].ownership == OWN_EXTERNAL


def test_script_owned_assignment_is_updated_when_actual_differs():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("restricted", {selected})},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected})},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["set"]
    assert actions[0].value == "restricted"
    assert actions[0].ownership == OWN_SCRIPT
    assert actions[0].reason == "R4 update (was 'pii')"


def test_external_assignment_conflicts_without_transfer():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("restricted", {selected})},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected}, OWN_EXTERNAL)},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["conflict"]
    assert actions[0].ownership == OWN_EXTERNAL
    assert actions[0].reason == "R3 externally owned, actual='pii'"


def test_untracked_assignment_conflicts_as_external_without_transfer():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("restricted", {selected})},
        actual={tag_key: "pii"},
        state={},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["conflict"]
    assert actions[0].ownership == OWN_EXTERNAL


def test_transfer_takes_ownership_of_external_assignment():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("restricted", {selected})},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected}, OWN_EXTERNAL)},
        transfer=True,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["set"]
    assert actions[0].ownership == OWN_SCRIPT
    assert actions[0].reason == "R3+transfer (external was 'pii')"


def test_selected_value_cannot_override_out_of_scope_contributor():
    selected = contributor()
    outside_scope = contributor("main.shared.customers")
    tag_key = key()

    actions = plan(
        desired={tag_key: Desired("restricted", {selected})},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected, outside_scope})},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["conflict"]
    assert actions[0].contributors == {selected, outside_scope}
    assert "outside this run" in actions[0].reason


def test_removing_final_external_contributor_forgets_observation():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={},
        actual={tag_key: "pii"},
        state={tag_key: state("pii", {selected}, OWN_EXTERNAL)},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["forget"]
    assert actions[0].ownership == OWN_EXTERNAL
    assert actions[0].reason == "R7 external, forget observation"


def test_removing_missing_script_owned_assignment_forgets_state():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={},
        actual={},
        state={tag_key: state("pii", {selected})},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["forget"]
    assert actions[0].ownership == OWN_SCRIPT
    assert actions[0].reason == "already gone"


def test_removing_externally_modified_script_assignment_conflicts():
    selected = contributor()
    tag_key = key()

    actions = plan(
        desired={},
        actual={tag_key: "restricted"},
        state={tag_key: state("pii", {selected})},
        transfer=False,
        selected_contributors={selected},
    )

    assert [action.kind for action in actions] == ["conflict"]
    assert actions[0].value is None
    assert "externally modified" in actions[0].reason
