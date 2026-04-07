import base64
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3

TABLE_NAME = os.environ["TABLE_NAME"]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

ALL_FIELDS = {
    "date",
    "project_name",
    "agreement_number",
    "quote_deadline",
    "decision_deadline",
    "shipping_budget",
    "decision_status",
    "official_3pl_outbound_decision",
    "official_3pl_inbound_decision",
    "offical_3pl_notified_y_n",
    "bol_status",
    "logistics_status",
    "project_address",
    "state_project_is_in",
    "meal_count",
    "meal_type",
    "shipping_quote_form_made",
    "total_miles_each_way",
    "total_miles_round_trip",
    "warehouse_packing_deadline",
    "outbound_delivery_date",
    "outbound_weekday_number",
    "outbound_weekend_delivery",
    "outbound_liftgate_yes_or_no",
    "outbound_volume",
    "outbound_ftl_10_or_more",
    "outbound_weight",
    "ob_quote_number_1_company",
    "ob_quote_number_1_cost",
    "ob_quote_number_2_company",
    "ob_quote_number_2_cost",
    "ob_quote_number_3_company",
    "ob_quote_number_3_cost",
    "outbound_quote_final",
    "inbound_warehouse",
    "inbound_pickup_date",
    "inbound_weekday_number",
    "inbound_weekend_delivery",
    "inbound_pickup_liftgate",
    "inbound_volume",
    "inbound_ftl_10_or_more",
    "inbound_weight",
    "inbound_quote_number_1_company",
    "inbound_quote_number_1_cost",
    "inbound_quote_number_2_company",
    "inbound_quote_number_2_cost",
    "inbound_quote_number_3_company",
    "inbound_quote_number_3_cost",
    "inbound_quote_final",
}

REQUIRED_FIELDS = {
    "date",
    "project_name",
    "agreement_number",
    "quote_deadline",
    "decision_deadline",
    "shipping_budget",
    "official_3pl_outbound_decision",
    "official_3pl_inbound_decision",
    "project_address",
    "state_project_is_in",
    "meal_count",
    "meal_type",
    "total_miles_each_way",
    "total_miles_round_trip",
    "outbound_delivery_date",
    "outbound_weekday_number",
    "outbound_weekend_delivery",
    "outbound_liftgate_yes_or_no",
    "outbound_volume",
    "outbound_ftl_10_or_more",
    "outbound_weight",
    "outbound_quote_final",
    "inbound_pickup_date",
    "inbound_weekday_number",
    "inbound_weekend_delivery",
    "inbound_pickup_liftgate",
    "inbound_volume",
    "inbound_ftl_10_or_more",
    "inbound_weight",
    "inbound_quote_final",
}

NUMBER_FIELDS = {
    "agreement_number",
    "meal_count",
    "total_miles_each_way",
    "total_miles_round_trip",
    "outbound_weekday_number",
    "outbound_volume",
    "outbound_weight",
    "ob_quote_number_1_cost",
    "inbound_weekday_number",
    "inbound_volume",
    "inbound_weight",
    "inbound_quote_number_2_cost",
}

BOOLEAN_FIELDS = {
    "offical_3pl_notified_y_n",
    "outbound_weekend_delivery",
    "outbound_ftl_10_or_more",
    "inbound_weekend_delivery",
    "inbound_ftl_10_or_more",
}

DATE_FIELDS = {
    "date",
    "quote_deadline",
    "decision_deadline",
    "outbound_delivery_date",
    "inbound_pickup_date",
}

CURRENCY_FIELDS = {
    "ob_quote_number_2_cost",
    "ob_quote_number_3_cost",
    "outbound_quote_final",
    "inbound_quote_final",
}


def build_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, default=str)
    }


def parse_request_body(event: dict) -> dict:
    body = event.get("body")

    if body is None:
        return {}

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    if isinstance(body, str):
        return json.loads(body)

    if isinstance(body, dict):
        return body

    raise ValueError("Request body must be valid JSON")


def to_decimal(value):
    if value is None or value == "":
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    cleaned = str(value).strip().replace(",", "").replace("$", "")
    if cleaned == "":
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid numeric value: {value}") from exc


def to_bool(value):
    if value is None or value == "":
        return None

    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()

    truthy = {"true", "t", "yes", "y", "1"}
    falsy = {"false", "f", "no", "n", "0"}

    if normalized in truthy:
        return True
    if normalized in falsy:
        return False

    raise ValueError(f"Invalid boolean value: {value}")


def to_iso_date(value):
    if value is None or value == "":
        return None

    if isinstance(value, str):
        value = value.strip()
    else:
        value = str(value).strip()

    supported_formats = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
    ]

    for fmt in supported_formats:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    raise ValueError(f"Invalid date value: {value}")


def normalize_value(field_name: str, value):
    if value is None:
        return None

    if field_name in NUMBER_FIELDS:
        return to_decimal(value)

    if field_name in CURRENCY_FIELDS:
        return to_decimal(value)

    if field_name in BOOLEAN_FIELDS:
        return to_bool(value)

    if field_name in DATE_FIELDS:
        return to_iso_date(value)

    if isinstance(value, str):
        return value.strip()

    return value


def normalize_payload(payload: dict) -> dict:
    normalized = {}

    for key, value in payload.items():
        if key not in ALL_FIELDS:
            continue

        normalized[key] = normalize_value(key, value)

    return normalized


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


def lambda_handler(event, context):
    try:
        raw_payload = parse_request_body(event)

        if not isinstance(raw_payload, dict):
            return build_response(400, {
                "message": "Payload must be a JSON object"
            })

        missing_fields, unexpected_fields = validate_payload(raw_payload)

        if missing_fields:
            return build_response(400, {
                "message": "Missing required fields",
                "missing_fields": missing_fields
            })

        if unexpected_fields:
            return build_response(400, {
                "message": "Payload contains unexpected fields",
                "unexpected_fields": unexpected_fields
            })

        normalized_payload = normalize_payload(raw_payload)

        now = datetime.now(timezone.utc).isoformat()

        item = {
            **normalized_payload,
            "shipment_id": str(uuid.uuid4()),
            "created_at": now,
            "updated_at": now
        }

        table.put_item(Item=item)

        return build_response(201, {
            "message": "Shipment record created successfully",
            "shipment_id": item["shipment_id"]
        })

    except json.JSONDecodeError:
        return build_response(400, {
            "message": "Invalid JSON body"
        })
    except ValueError as e:
        return build_response(400, {
            "message": str(e)
        })
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return build_response(500, {
            "message": "Internal server error",
            "details": str(e)
        })