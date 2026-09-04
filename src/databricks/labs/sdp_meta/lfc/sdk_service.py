"""High-level, ownership-safe SDK reconciliation for Lakeflow Connect."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from databricks.labs.sdp_meta.lfc.deployer import (
    CREATE,
    DRIFT,
    NOOP,
    PRUNE,
    SKIP,
    UPDATE,
    DeploymentPlan,
    DeploymentResult,
    DeploymentState,
    IngestionDeployer,
    PartialDeploymentError,
    PlanAction,
)
from databricks.labs.sdp_meta.lfc.renderer import IngestionRenderer
from databricks.labs.sdp_meta.lfc.state import IngestionStateRepository


def _is_not_found(err: Exception) -> bool:
    """Match SDK not-found errors so prune retries converge on deletion."""
    return getattr(err, "status_code", None) == 404 or getattr(
        err, "error_code", None
    ) in {"NOT_FOUND", "RESOURCE_DOES_NOT_EXIST"}


@dataclass
class IngestionReconcileResult:
    """Safe aggregate result for CLI output."""

    actions: List[PlanAction]
    dry_run: bool

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "actions": [action.as_dict() for action in self.actions],
        }


class IngestionSdkService:
    """Coordinate SQL desired state, SDK reconciliation, and ownership state."""

    def __init__(
        self,
        workspace_client: Any,
        repository: IngestionStateRepository,
        *,
        renderer: Optional[IngestionRenderer] = None,
        deployer: Optional[IngestionDeployer] = None,
    ):
        self.workspace_client = workspace_client
        self.repository = repository
        self.renderer = renderer or IngestionRenderer()
        self.deployer = deployer or IngestionDeployer(workspace_client)

    @staticmethod
    def _data_flow_id(spec: Dict[str, Any]) -> str:
        value = spec.get("dataFlowId")
        if value is None or str(value) == "":
            raise ValueError("ingestion spec is missing dataFlowId")
        return str(value)

    @staticmethod
    def _has_stateful_action(plan: DeploymentPlan) -> bool:
        return any(
            action.action in (CREATE, UPDATE)
            for action in plan.actions
        )

    def _plan_selected(
        self,
        specs: Sequence[Dict[str, Any]],
        states_by_id: Dict[str, Any],
        *,
        dry_run: bool,
    ) -> List[Tuple[str, Any, Any, DeploymentPlan]]:
        planned = []
        drift = []
        rendered_specs = [
            (self._data_flow_id(spec), self.renderer.render(spec))
            for spec in specs
        ]
        names: Dict[str, Tuple[str, str]] = {}
        collisions = []
        for data_flow_id, rendered in rendered_specs:
            for kind, definition in (
                ("gateway", rendered.gateway),
                ("ingestion", rendered.ingestion),
            ):
                name = str(definition["name"])
                owner = names.get(name)
                if owner is not None:
                    collisions.append(
                        f"{name!r} is used by "
                        f"{owner[0]}:{owner[1]} and {data_flow_id}:{kind}"
                    )
                else:
                    names[name] = (data_flow_id, kind)
        if collisions:
            raise ValueError(
                "ingestion pipeline names must be globally unique: "
                + "; ".join(collisions)
            )

        existing_by_name = (
            self.deployer.existing_by_name() if rendered_specs else {}
        )
        for data_flow_id, rendered in rendered_specs:
            stored = states_by_id.get(data_flow_id)
            prior = stored.state if stored is not None else None
            plan = self.deployer.plan(
                rendered,
                state=prior,
                prune=False,
                dry_run=dry_run,
                existing_by_name=existing_by_name,
            )
            planned.append((data_flow_id, rendered, prior, plan))
            drift.extend(
                action for action in plan.actions if action.action == DRIFT
            )
        if drift:
            summary = ", ".join(
                f"{action.kind}:{action.name} ({action.reason})"
                for action in drift
            )
            raise RuntimeError(
                "ingestion deployment refused due to ownership drift: "
                f"{summary}"
            )
        return planned

    @staticmethod
    def _prune_actions(
        states_by_id: Dict[str, Any], desired_ids: Sequence[str]
    ) -> List[Tuple[str, PlanAction]]:
        desired = set(desired_ids)
        actions = []
        for data_flow_id, stored in sorted(states_by_id.items()):
            if data_flow_id in desired:
                continue
            for kind, pipeline_id in (
                ("ingestion", stored.state.ingestion_pipeline_id),
                ("gateway", stored.state.gateway_pipeline_id),
            ):
                if pipeline_id:
                    actions.append(
                        (
                            data_flow_id,
                            PlanAction(
                                PRUNE,
                                kind,
                                f"data-flow-{data_flow_id}-{kind}",
                                str(pipeline_id),
                                "owned state is absent from desired specs",
                            ),
                        )
                    )
        return actions

    def reconcile(
        self,
        *,
        data_flow_group: Optional[str] = None,
        data_flow_ids: Optional[Sequence[str]] = None,
        dry_run: bool = False,
        prune: bool = False,
    ) -> IngestionReconcileResult:
        """Preflight all selected specs, then reconcile and optionally prune."""
        specs = self.repository.read_specs(data_flow_group, data_flow_ids)
        stored_states = self.repository.read_states(allow_missing=True)
        states_by_id = {
            stored.data_flow_id: stored for stored in stored_states
        }
        planned = self._plan_selected(
            specs, states_by_id, dry_run=dry_run
        )
        prune_items = []
        if prune:
            desired_ids = self.repository.read_all_spec_ids()
            prune_items = self._prune_actions(states_by_id, desired_ids)

        all_actions = [
            action
            for _, _, _, plan in planned
            for action in plan.actions
        ]
        all_actions.extend(action for _, action in prune_items)
        if dry_run:
            return IngestionReconcileResult(all_actions, True)

        has_apply_actions = any(
            action.action in (CREATE, UPDATE)
            for action in all_actions
        )
        if not has_apply_actions and not prune_items:
            return IngestionReconcileResult(all_actions, False)
        self.repository.ensure_state_table()
        lock_token = uuid.uuid4().hex
        lock_ids = sorted({
            *(
                data_flow_id
                for data_flow_id, _, _, plan in planned
                if self._has_stateful_action(plan)
            ),
            *(data_flow_id for data_flow_id, _ in prune_items),
        })
        acquired = []
        try:
            for data_flow_id in lock_ids:
                if not self.repository.acquire_lock(
                    data_flow_id, lock_token
                ):
                    raise RuntimeError(
                        "another ingestion reconciliation is active for "
                        f"data_flow_id={data_flow_id}"
                    )
                acquired.append(data_flow_id)

            # A durable pending row is written before any Workspace mutation.
            # If final state persistence fails after a successful create, the
            # next run can adopt only the same-name pipelines carrying the
            # renderer's matching ownership tags.
            for data_flow_id, _, prior, plan in planned:
                if not self._has_stateful_action(plan):
                    continue
                self.repository.save_state(
                    data_flow_id,
                    DeploymentState(
                        prior.gateway_pipeline_id if prior else None,
                        prior.ingestion_pipeline_id if prior else None,
                        plan.spec_version,
                        "",
                        "pending",
                    ),
                )

            completed_actions = []
            for data_flow_id, rendered, prior, plan in planned:
                if all(
                    action.action in (SKIP, NOOP)
                    for action in plan.actions
                ):
                    completed_actions.extend(plan.actions)
                    continue
                try:
                    result: DeploymentResult = self.deployer.deploy(
                        rendered,
                        state=prior,
                        prune=False,
                        dry_run=False,
                        deployment_plan=plan,
                    )
                except PartialDeploymentError as err:
                    self.repository.save_state(data_flow_id, err.state)
                    raise
                if any(
                    action.action == DRIFT
                    for action in result.plan.actions
                ):
                    raise RuntimeError(
                        "ingestion deployment encountered ownership drift "
                        f"for data_flow_id={data_flow_id}"
                    )
                if self._has_stateful_action(result.plan):
                    self.repository.save_state(data_flow_id, result.state)
                completed_actions.extend(result.plan.actions)

            prune_by_id: Dict[str, List[PlanAction]] = {}
            for data_flow_id, action in prune_items:
                prune_by_id.setdefault(data_flow_id, []).append(action)
            for data_flow_id in sorted(prune_by_id):
                for action in prune_by_id[data_flow_id]:
                    try:
                        self.workspace_client.pipelines.delete(
                            action.pipeline_id
                        )
                    except Exception as err:
                        if not _is_not_found(err):
                            raise
                    completed_actions.append(action)
                self.repository.delete_states([data_flow_id])
            return IngestionReconcileResult(completed_actions, False)
        finally:
            for data_flow_id in reversed(acquired):
                self.repository.release_lock(data_flow_id, lock_token)
