"""Shared Event Hubs Kafka option construction."""

from collections.abc import Callable


_SAS_SOURCE_FIELDS = (
    "eventhub.accessKeyName",
    "eventhub.accessKeySecretName",
    "eventhub.secretsScopeName",
)
EVENTHUB_CONFIGURATION_FIELDS = (
    "eventhub.namespace",
    "eventhub.port",
    "eventhub.name",
    *_SAS_SOURCE_FIELDS,
)
_SAS_KAFKA_OPTIONS = (
    "kafka.sasl.mechanism",
    "kafka.sasl.jaas.config",
    "kafka.security.protocol",
)
_CONNECTION_OPTIONS = (
    "kafka.bootstrap.servers",
    "subscribe",
    "subscribepattern",
    "assign",
    "topic",
)
_SAS_SUBSCRIPTION_CONFLICT_OPTIONS = ("subscribepattern", "assign")
_SERVICE_CREDENTIAL_OPTION = "databricks.serviceCredential"
_SERVICE_CREDENTIAL_OPTION_LOWER = _SERVICE_CREDENTIAL_OPTION.lower()


def build_eventhub_kafka_options(
    eventhub_details: dict,
    reader_options: dict,
    *,
    topic_option: str,
    get_secret: Callable[[str, str], str],
    allow_sas_connection_option_overrides: bool = True,
) -> dict:
    """Build Kafka options for Event Hubs using SAS or a service credential."""
    required = ("eventhub.namespace", "eventhub.port", "eventhub.name")
    missing = [key for key in required if not eventhub_details.get(key)]
    if missing:
        raise ValueError(
            "Event Hubs requires non-empty source details: " + ", ".join(missing)
        )

    namespace = eventhub_details["eventhub.namespace"]
    port = eventhub_details["eventhub.port"]
    name = eventhub_details["eventhub.name"]
    connection_options = {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:{port}",
        topic_option: name,
    }
    service_credential_keys = [
        key
        for key in reader_options
        if str(key).lower() == _SERVICE_CREDENTIAL_OPTION_LOWER
    ]
    if len(service_credential_keys) > 1:
        raise ValueError(
            "databricks.serviceCredential cannot be configured more than once "
            f"with different casing: {', '.join(map(str, service_credential_keys))}"
        )
    service_credential_key = (
        service_credential_keys[0] if service_credential_keys else None
    )
    service_credential = (
        reader_options[service_credential_key]
        if service_credential_key is not None
        else None
    )
    effective_options = {
        key: value
        for key, value in reader_options.items()
        if str(key).lower() != _SERVICE_CREDENTIAL_OPTION_LOWER
    }
    if service_credential is not None:
        if not isinstance(service_credential, str) or not service_credential.strip():
            raise ValueError(
                "databricks.serviceCredential must be a non-empty service credential name"
            )
        service_credential = service_credential.strip()
        sas_source_fields = sorted(
            key for key in _SAS_SOURCE_FIELDS if eventhub_details.get(key)
        )
        if sas_source_fields:
            raise ValueError(
                "databricks.serviceCredential cannot be combined with "
                "SAS Event Hubs source details: "
                f"{', '.join(sas_source_fields)}"
            )
        conflicting_options = sorted(
            key
            for key in reader_options
            if str(key).lower().startswith("kafka.sasl.")
            or str(key).lower() in _SAS_KAFKA_OPTIONS
            or str(key).lower() in _CONNECTION_OPTIONS
        )
        if conflicting_options:
            raise ValueError(
                "databricks.serviceCredential cannot be combined with "
                f"Kafka connection or SAS options: {', '.join(conflicting_options)}"
            )
        return {
            **connection_options,
            **effective_options,
            _SERVICE_CREDENTIAL_OPTION: service_credential,
        }

    sas_subscription_conflicts = sorted(
        key
        for key in reader_options
        if str(key).lower() in _SAS_SUBSCRIPTION_CONFLICT_OPTIONS
    )
    if sas_subscription_conflicts:
        raise ValueError(
            f"Event Hubs SAS authentication cannot combine {topic_option} with "
            f"{', '.join(sas_subscription_conflicts)}"
        )
    missing_sas_fields = [
        key for key in ("eventhub.accessKeyName", "eventhub.secretsScopeName")
        if not eventhub_details.get(key)
    ]
    if missing_sas_fields:
        raise ValueError(
            "Event Hubs SAS authentication requires source details: "
            + ", ".join(missing_sas_fields)
        )
    key_name = eventhub_details["eventhub.accessKeyName"]
    secret_name = eventhub_details.get("eventhub.accessKeySecretName") or key_name
    secret = get_secret(eventhub_details["eventhub.secretsScopeName"], secret_name)
    connection_string = (
        f"Endpoint=sb://{namespace}.servicebus.windows.net/;"
        f"SharedAccessKeyName={key_name};SharedAccessKey={secret}"
    )
    login_module = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    sasl_config = (
        f'{login_module} required username="$ConnectionString" '
        f'password="{connection_string}";'
    )
    sas_options = {
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": sasl_config,
    }
    if allow_sas_connection_option_overrides:
        return {**connection_options, **sas_options, **effective_options}
    generated_option_keys = {
        key.lower()
        for key in (*connection_options, *sas_options)
    }
    sink_options = {
        key: value
        for key, value in effective_options.items()
        if str(key).lower() not in generated_option_keys
    }
    return {**sink_options, **connection_options, **sas_options}
