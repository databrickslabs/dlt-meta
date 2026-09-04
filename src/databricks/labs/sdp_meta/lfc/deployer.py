"""Planning and reconciliation for Lakeflow Connect ingestion pipelines."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional

from databricks.labs.sdp_meta.lfc.renderer import (
    GATEWAY_ID_PLACEHOLDER,
    RenderedIngestionDefinitions,
)


CREATE = "create"
UPDATE = "update"
NOOP = "noop"
DRIFT = "drift"
PRUNE = "prune"
SKIP = "skip"


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict") and callable(value.as_dict):
        result = value.as_dict()
        if isinstance(result, Mapping):
            return result
    if hasattr(value, "__dict__"):
        return vars(value)
    raise TypeError("expected a mapping-like value")


def _value(value: Any, *names: str, default: Any = None) -> Any:
    mapping = _mapping(value)
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return default


def desired_fingerprint(
    gateway: Mapping[str, Any], ingestion: Mapping[str, Any]
) -> str:
    """Return a stable fingerprint without exposing configuration values."""
    payload = {"gateway": gateway, "ingestion": ingestion}
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _desired_is_subset(actual: Any, desired: Any) -> bool:
    """Compare desired fields while ignoring server-populated live defaults."""
    if isinstance(desired, Mapping):
        try:
            actual_mapping = _mapping(actual)
        except TypeError:
            return False
        return all(
            (
                key in actual_mapping
                and _desired_is_subset(actual_mapping[key], value)
            )
            or (
                key not in actual_mapping
                and value in (None, [], {})
            )
            for key, value in desired.items()
        )
    if isinstance(desired, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(desired)
            and all(
                _desired_is_subset(actual_item, desired_item)
                for actual_item, desired_item in zip(actual, desired)
            )
        )
    actual_value = getattr(actual, "value", actual)
    return actual_value == desired


@dataclass
class DeploymentState:
    """Serializable reconciliation state; persistence is deliberately external."""

    gateway_pipeline_id: Optional[str]
    ingestion_pipeline_id: Optional[str]
    spec_version: str
    fingerprint: str
    status: str

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_value(cls, value: Any) -> "DeploymentState":
        if isinstance(value, cls):
            return value
        raw = _mapping(value)
        return cls(
            gateway_pipeline_id=raw.get("gateway_pipeline_id"),
            ingestion_pipeline_id=raw.get("ingestion_pipeline_id"),
            spec_version=str(raw.get("spec_version", "1")),
            fingerprint=str(raw.get("fingerprint", "")),
            status=str(raw.get("status", "unknown")),
        )


@dataclass
class PlanAction:
    """One pipeline reconciliation action."""

    action: str
    kind: str
    name: str
    pipeline_id: Optional[str] = None
    reason: str = ""
    desired: Optional[Dict[str, Any]] = field(default=None, repr=False)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "kind": self.kind,
            "name": self.name,
            "pipeline_id": self.pipeline_id,
            "reason": self.reason,
        }


@dataclass
class DeploymentPlan:
    actions: List[PlanAction]
    fingerprint: str
    spec_version: str
    dry_run: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "actions": [action.as_dict() for action in self.actions],
            "fingerprint": self.fingerprint,
            "spec_version": self.spec_version,
            "dry_run": self.dry_run,
        }


@dataclass
class DeploymentResult:
    plan: DeploymentPlan
    state: DeploymentState


class PartialDeploymentError(RuntimeError):
    """Deployment failed after ownership-relevant work may have completed."""

    def __init__(self, message: str, state: DeploymentState):
        self.state = state
        super().__init__(message)


def schedule_plan(
    name: str,
    ingestion_pipeline_id: str,
    schedule: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Render Jobs metadata that schedules the ingestion pipeline directly."""
    result: Dict[str, Any] = {
        "name": name,
        "tasks": [{
            "task_key": "run_ingestion_pipeline",
            "pipeline_task": {
                "pipeline_id": ingestion_pipeline_id,
                "full_refresh": False,
            },
        }],
    }
    if schedule:
        result["schedule"] = deepcopy(dict(schedule))
    return result


class IngestionDeployer:
    """Testable create/update/reconcile service for a WorkspaceClient-like object."""

    def __init__(self, workspace_client: Any):
        self.workspace_client = workspace_client

    def _rendered(self, desired: Any) -> RenderedIngestionDefinitions:
        if isinstance(desired, RenderedIngestionDefinitions):
            return desired
        raw = _mapping(desired)
        gateway = raw.get("gateway", raw.get("gateway_definition"))
        ingestion = raw.get("ingestion", raw.get("ingestion_definition"))
        if not isinstance(gateway, Mapping) or not isinstance(
            ingestion, Mapping
        ):
            raise TypeError(
                "desired definitions must contain gateway and ingestion mappings"
            )
        return RenderedIngestionDefinitions(
            gateway=deepcopy(dict(gateway)),
            ingestion=deepcopy(dict(ingestion)),
            deploy=bool(raw.get("deploy", True)),
            spec_version=str(raw.get("spec_version", "1")),
        )

    def _list_pipelines(self) -> List[Any]:
        pipelines = self.workspace_client.pipelines
        if hasattr(pipelines, "list_pipelines"):
            return list(pipelines.list_pipelines())
        if hasattr(pipelines, "list"):
            return list(pipelines.list())
        return []

    @staticmethod
    def _pipeline_id(pipeline: Any) -> Optional[str]:
        return _value(pipeline, "pipeline_id", "id")

    @staticmethod
    def _pipeline_name(pipeline: Any) -> Optional[str]:
        return _value(pipeline, "name")

    def existing_by_name(self) -> Dict[str, Any]:
        """Return one reusable snapshot of workspace pipelines by name."""
        return {
            name: pipeline
            for pipeline in self._list_pipelines()
            for name in [self._pipeline_name(pipeline)]
            if name
        }

    def _live_matches(
        self,
        pipeline_id: str,
        desired: Mapping[str, Any],
    ) -> Optional[bool]:
        """Return whether visible live settings contain the desired payload."""
        get_pipeline = getattr(
            self.workspace_client.pipelines, "get", None
        )
        if not callable(get_pipeline):
            return None
        response = get_pipeline(pipeline_id)
        spec = _value(response, "spec", default=response)
        if (
            spec is response
            and not isinstance(response, Mapping)
            and "spec" not in vars(response)
        ):
            return None
        try:
            _mapping(spec)
        except TypeError:
            # Lightweight test doubles and older SDK facades may not expose a
            # readable spec. Fingerprint behavior remains the fallback there.
            return None
        return _desired_is_subset(spec, desired)

    def plan(
        self,
        desired: Any,
        state: Optional[Any] = None,
        prune: bool = False,
        dry_run: bool = False,
        *,
        existing_by_name: Optional[Mapping[str, Any]] = None,
    ) -> DeploymentPlan:
        """Plan reconciliation without adopting same-name unmanaged pipelines."""
        rendered = self._rendered(desired)
        fingerprint = desired_fingerprint(
            rendered.gateway, rendered.ingestion
        )
        prior = (
            DeploymentState.from_value(state) if state is not None else None
        )
        existing = (
            dict(existing_by_name)
            if existing_by_name is not None
            else self.existing_by_name()
        )
        actions: List[PlanAction] = []

        if not rendered.deploy:
            for kind, definition in (
                ("gateway", rendered.gateway),
                ("ingestion", rendered.ingestion),
            ):
                actions.append(
                    PlanAction(
                        SKIP,
                        kind,
                        definition["name"],
                        reason="deploy is false",
                    )
                )
            return DeploymentPlan(
                actions, fingerprint, rendered.spec_version, dry_run
            )

        desired_pairs = (
            (
                "gateway",
                rendered.gateway,
                prior.gateway_pipeline_id if prior else None,
            ),
            (
                "ingestion",
                rendered.ingestion,
                prior.ingestion_pipeline_id if prior else None,
            ),
        )
        existing_ids = {
            self._pipeline_id(pipeline): pipeline
            for pipeline in existing.values()
            if self._pipeline_id(pipeline)
        }
        for kind, definition, managed_id in desired_pairs:
            name = str(definition["name"])
            named = existing.get(name)
            named_id = (
                self._pipeline_id(named) if named is not None else None
            )
            if managed_id:
                if managed_id not in existing_ids and named_id != managed_id:
                    actions.append(
                        PlanAction(
                            DRIFT,
                            kind,
                            name,
                            managed_id,
                            "managed pipeline is missing",
                            deepcopy(definition),
                        )
                    )
                elif prior and prior.fingerprint == fingerprint:
                    live_definition = definition
                    if (
                        kind == "ingestion"
                        and prior.gateway_pipeline_id
                    ):
                        live_definition = self._wire_gateway(
                            definition, prior.gateway_pipeline_id
                        )
                    live_matches = self._live_matches(
                        managed_id, live_definition
                    )
                    if live_matches is False:
                        actions.append(
                            PlanAction(
                                UPDATE,
                                kind,
                                name,
                                managed_id,
                                "live pipeline configuration drifted",
                                deepcopy(definition),
                            )
                        )
                    else:
                        actions.append(
                            PlanAction(
                                NOOP,
                                kind,
                                name,
                                managed_id,
                                (
                                    "fingerprint and live definition match"
                                    if live_matches
                                    else "fingerprint matches"
                                ),
                                deepcopy(definition),
                            )
                        )
                else:
                    actions.append(
                        PlanAction(
                            UPDATE,
                            kind,
                            name,
                            managed_id,
                            "desired spec changed",
                            deepcopy(definition),
                        )
                    )
            elif named is not None:
                ownership_tags = definition.get("tags")
                recoverable = (
                    prior is not None
                    and prior.status == "pending"
                    and named_id is not None
                    and isinstance(ownership_tags, Mapping)
                    and self._live_matches(
                        named_id, {"tags": ownership_tags}
                    ) is True
                )
                if recoverable:
                    actions.append(
                        PlanAction(
                            UPDATE,
                            kind,
                            name,
                            named_id,
                            "recovering a tagged pending deployment",
                            deepcopy(definition),
                        )
                    )
                else:
                    actions.append(
                        PlanAction(
                            DRIFT,
                            kind,
                            name,
                            named_id,
                            "same-name pipeline is not managed",
                            deepcopy(definition),
                        )
                    )
            else:
                actions.append(
                    PlanAction(
                        CREATE, kind, name, desired=deepcopy(definition)
                    )
                )

        if prune and prior:
            desired_ids = {
                action.pipeline_id
                for action in actions
                if action.pipeline_id and action.action != DRIFT
            }
            for kind, pipeline_id in (
                ("gateway", prior.gateway_pipeline_id),
                ("ingestion", prior.ingestion_pipeline_id),
            ):
                if (
                    pipeline_id
                    and pipeline_id not in desired_ids
                    and pipeline_id in existing_ids
                ):
                    old = existing_ids[pipeline_id]
                    actions.append(
                        PlanAction(
                            PRUNE,
                            kind,
                            self._pipeline_name(old) or pipeline_id,
                            pipeline_id,
                            "explicit prune",
                        )
                    )
        return DeploymentPlan(
            actions, fingerprint, rendered.spec_version, dry_run
        )

    @staticmethod
    def _wire_gateway(
        definition: Mapping[str, Any], gateway_id: str
    ) -> Dict[str, Any]:
        result = deepcopy(dict(definition))
        ingestion_definition = result.get("ingestion_definition", {})
        if (
            ingestion_definition.get("ingestion_gateway_id")
            == GATEWAY_ID_PLACEHOLDER
        ):
            ingestion_definition["ingestion_gateway_id"] = gateway_id
        result["ingestion_definition"] = ingestion_definition
        return result

    @staticmethod
    def _sdk_payload(definition: Mapping[str, Any]) -> Dict[str, Any]:
        """Convert nested dictionaries to models expected by PipelinesAPI."""
        from databricks.sdk.service import pipelines as pipeline_models

        result = deepcopy(dict(definition))
        converters = {
            "gateway_definition": "IngestionGatewayPipelineDefinition",
            "ingestion_definition": "IngestionPipelineDefinition",
        }
        for field_name, model_name in converters.items():
            value = result.get(field_name)
            if isinstance(value, Mapping):
                model = getattr(pipeline_models, model_name, None)
                if model is None:
                    raise RuntimeError(
                        "installed databricks-sdk does not support Lakeflow "
                        "Connect managed ingestion; upgrade databricks-sdk"
                    )
                result[field_name] = model.from_dict(dict(value))
        if isinstance(result.get("clusters"), list):
            result["clusters"] = [
                pipeline_models.PipelineCluster.from_dict(dict(cluster))
                if isinstance(cluster, Mapping)
                else cluster
                for cluster in result["clusters"]
            ]
        return result

    def _create(self, definition: Mapping[str, Any]) -> str:
        response = self.workspace_client.pipelines.create(
            **self._sdk_payload(definition)
        )
        pipeline_id = self._pipeline_id(response)
        if not pipeline_id:
            raise RuntimeError(
                "pipeline create response did not include an id"
            )
        return pipeline_id

    def _update(
        self, pipeline_id: str, definition: Mapping[str, Any]
    ) -> str:
        self.workspace_client.pipelines.update(
            pipeline_id=pipeline_id,
            **self._sdk_payload(definition),
        )
        return pipeline_id

    def deploy(
        self,
        desired: Any,
        state: Optional[Any] = None,
        prune: bool = False,
        dry_run: bool = False,
        *,
        deployment_plan: Optional[DeploymentPlan] = None,
    ) -> DeploymentResult:
        """Plan and, unless dry-run, reconcile gateway before ingestion."""
        rendered = self._rendered(desired)
        if deployment_plan is None:
            plan = self.plan(
                rendered, state=state, prune=prune, dry_run=dry_run
            )
        else:
            expected_fingerprint = desired_fingerprint(
                rendered.gateway, rendered.ingestion
            )
            if (
                deployment_plan.fingerprint != expected_fingerprint
                or deployment_plan.spec_version != rendered.spec_version
            ):
                raise ValueError(
                    "deployment plan does not match desired ingestion state"
                )
            plan = deployment_plan
        prior = (
            DeploymentState.from_value(state) if state is not None else None
        )
        gateway_id = prior.gateway_pipeline_id if prior else None
        ingestion_id = prior.ingestion_pipeline_id if prior else None

        if dry_run:
            return DeploymentResult(
                plan,
                DeploymentState(
                    gateway_id,
                    ingestion_id,
                    plan.spec_version,
                    plan.fingerprint,
                    "planned",
                ),
            )

        actions = sorted(
            plan.actions,
            key=lambda action: (
                action.action == PRUNE,
                # Creates/updates wire the gateway first (the ingestion
                # pipeline needs its id); prunes delete in reverse order —
                # ingestion before the gateway it still references.
                (action.kind == "gateway")
                if action.action == PRUNE
                else (action.kind != "gateway"),
            ),
        )
        status = "deployed"
        for action in actions:
            if action.action in (SKIP, NOOP):
                if action.kind == "gateway":
                    gateway_id = action.pipeline_id or gateway_id
                else:
                    ingestion_id = action.pipeline_id or ingestion_id
                if action.action == SKIP:
                    status = "skipped"
                continue
            if action.action == DRIFT:
                status = "drift"
                continue
            try:
                if action.action == PRUNE:
                    self.workspace_client.pipelines.delete(action.pipeline_id)
                    continue

                definition = action.desired or {}
                if action.kind == "ingestion":
                    if not gateway_id:
                        raise RuntimeError(
                            "cannot deploy ingestion before its gateway has an id"
                        )
                    definition = self._wire_gateway(definition, gateway_id)
                pipeline_id = (
                    self._create(definition)
                    if action.action == CREATE
                    else self._update(str(action.pipeline_id), definition)
                )
                if action.kind == "gateway":
                    gateway_id = pipeline_id
                else:
                    ingestion_id = pipeline_id
            except Exception as err:
                # Preserve IDs acquired before a later action failed.  An empty
                # fingerprint deliberately forces every managed definition to
                # be retried instead of marking partially applied work NOOP.
                partial = DeploymentState(
                    gateway_id,
                    ingestion_id,
                    plan.spec_version,
                    "",
                    "failed",
                )
                raise PartialDeploymentError(
                    "ingestion deployment failed after partial progress",
                    partial,
                ) from err

        return DeploymentResult(
            plan,
            DeploymentState(
                gateway_id,
                ingestion_id,
                plan.spec_version,
                plan.fingerprint,
                status,
            ),
        )

    @staticmethod
    def schedule_plan(
        name: str,
        ingestion_pipeline_id: str,
        schedule: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        return schedule_plan(name, ingestion_pipeline_id, schedule)
