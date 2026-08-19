"""Pure reconciliation planner implementing assignment ownership rules."""

from typing import Dict, List, Optional

from databricks.labs.sdp_meta.governance.tagging.models import (
    OWN_EXTERNAL,
    OWN_SCRIPT,
    Action,
    Desired,
    Key,
)


def plan(
    desired: Dict[Key, Desired],
    actual: Dict[Key, str],
    state: Dict[Key, dict],
    transfer: bool,
    selected_contributors: Optional[set] = None,
) -> List[Action]:
    selected_contributors = selected_contributors or set()
    actions: List[Action] = []
    for key, wanted in sorted(desired.items(), key=lambda item: item[0].label()):
        current_value = actual.get(key)
        current_state = state.get(key)
        owned = (
            current_state is not None
            and current_state["ownership"] == OWN_SCRIPT
            and current_state.get("status", "applied") == "applied"
        )
        prior = (
            set(current_state.get("contributors", set())) if current_state else set()
        )
        outside_scope = prior - selected_contributors
        contributors = outside_scope | wanted.contributors
        if (
            owned
            and outside_scope
            and current_state["last_applied_value"] != wanted.value
        ):
            actions.append(
                Action(
                    "conflict",
                    key,
                    wanted.value,
                    "selected value conflicts with contributors outside this run",
                    contributors=contributors,
                    ownership=OWN_SCRIPT,
                )
            )
        elif current_value is None:
            actions.append(
                Action(
                    "set",
                    key,
                    wanted.value,
                    "R1 absent",
                    contributors=contributors,
                    ownership=OWN_SCRIPT,
                )
            )
        elif current_value == wanted.value:
            if current_state is None:
                actions.append(
                    Action(
                        "record_external",
                        key,
                        wanted.value,
                        "R2 pre-existing, recorded external",
                        contributors=contributors,
                        ownership=OWN_EXTERNAL,
                    )
                )
            else:
                actions.append(
                    Action(
                        "noop",
                        key,
                        wanted.value,
                        "already desired",
                        contributors=contributors,
                        ownership=current_state["ownership"],
                    )
                )
        elif owned:
            actions.append(
                Action(
                    "set",
                    key,
                    wanted.value,
                    f"R4 update (was {current_value!r})",
                    contributors=contributors,
                    ownership=OWN_SCRIPT,
                )
            )
        elif transfer:
            actions.append(
                Action(
                    "set",
                    key,
                    wanted.value,
                    f"R3+transfer (external was {current_value!r})",
                    contributors=contributors,
                    ownership=OWN_SCRIPT,
                )
            )
        else:
            actions.append(
                Action(
                    "conflict",
                    key,
                    wanted.value,
                    f"R3 externally owned, actual={current_value!r}",
                    contributors=contributors,
                    ownership=(
                        current_state["ownership"] if current_state else OWN_EXTERNAL
                    ),
                )
            )

    for key, current_state in sorted(state.items(), key=lambda item: item[0].label()):
        if key in desired:
            continue
        prior = set(current_state.get("contributors", set()))
        if not prior & selected_contributors:
            continue
        remaining = prior - selected_contributors
        if remaining:
            actions.append(
                Action(
                    "update_contributors",
                    key,
                    current_state["last_applied_value"],
                    "selected contributors removed; assignment still shared",
                    contributors=remaining,
                    ownership=current_state["ownership"],
                )
            )
            continue
        current_value = actual.get(key)
        if current_state["ownership"] != OWN_SCRIPT:
            actions.append(
                Action(
                    "forget",
                    key,
                    None,
                    "R7 external, forget observation",
                    ownership=OWN_EXTERNAL,
                )
            )
        elif current_value is None:
            actions.append(
                Action(
                    "forget",
                    key,
                    None,
                    "already gone",
                    ownership=OWN_SCRIPT,
                )
            )
        elif current_value == current_state["last_applied_value"]:
            actions.append(
                Action(
                    "unset",
                    key,
                    None,
                    "R5 stale, unchanged",
                    ownership=OWN_SCRIPT,
                )
            )
        else:
            actions.append(
                Action(
                    "conflict",
                    key,
                    None,
                    f"R6 externally modified to {current_value!r}, preserved",
                    ownership=OWN_SCRIPT,
                )
            )
    return actions
