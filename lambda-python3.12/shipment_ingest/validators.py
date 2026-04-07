from schema import ALL_FIELDS, REQUIRED_FIELDS


def validate_payload(payload: dict):
    missing_fields = sorted(
        field for field in REQUIRED_FIELDS
        if field not in payload or payload[field] in ("", None, [])
    )

    unexpected_fields = sorted(
        key for key in payload.keys()
        if key not in ALL_FIELDS
    )

    return missing_fields, unexpected_fields