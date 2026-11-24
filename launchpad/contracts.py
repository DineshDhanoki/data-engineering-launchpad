"""Compare and validate the repository's small teaching data-contract format."""

import argparse
import json
from pathlib import Path

FORMAT = "launchpad-data-contract"
TYPES = {"string": str, "integer": int, "boolean": bool}


class ContractError(ValueError):
    """A malformed contract or record input with an actionable diagnostic."""


def load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContractError(f"file not found: {path}; provide an existing JSON path") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read JSON {path}: {exc}; provide valid UTF-8 JSON") from exc


def parse_contract(document: object) -> dict[str, object]:
    if not isinstance(document, dict):
        raise ContractError("contract must be a JSON object; use the documented contract shape")
    version = document.get("version", "<missing>")
    if document.get("format") != FORMAT:
        raise ContractError(
            f"contract {version}: rule format; expected {FORMAT!r}; set the format explicitly"
        )
    if not isinstance(document.get("version"), str) or not document["version"].strip():
        raise ContractError("contract <missing>: rule version; expected a non-empty string; add version")
    fields = document.get("fields")
    if not isinstance(fields, dict) or not fields:
        raise ContractError(f"contract {version}: rule fields; expected a non-empty object; declare fields")
    normalized = {}
    for name, specification in fields.items():
        if not isinstance(name, str) or not name or not isinstance(specification, dict):
            raise ContractError(
                f"contract {version}, field {name!r}: rule field_shape; use a named field object"
            )
        field_type = specification.get("type")
        required = specification.get("required")
        if field_type not in TYPES:
            raise ContractError(
                f"contract {version}, field {name}: rule type; use one of {sorted(TYPES)}"
            )
        if type(required) is not bool:
            raise ContractError(
                f"contract {version}, field {name}: rule required; set required to true or false"
            )
        if not required and "default" not in specification:
            raise ContractError(
                f"contract {version}, field {name}: rule optional_default; add a stable default"
            )
        if required and "default" in specification:
            raise ContractError(
                f"contract {version}, field {name}: rule required_default; remove the unused default"
            )
        allowed = specification.get("allowed")
        if allowed is not None and (
            not isinstance(allowed, list) or not allowed or len(allowed) != len(set(map(repr, allowed)))
        ):
            raise ContractError(
                f"contract {version}, field {name}: rule allowed; use a non-empty unique list"
            )
        expected_type = TYPES[field_type]
        for label in ("default",):
            if label in specification and not _has_type(specification[label], expected_type):
                raise ContractError(
                    f"contract {version}, field {name}: rule {label}_type; use {field_type}"
                )
        if allowed is not None:
            if any(not _has_type(value, expected_type) for value in allowed):
                raise ContractError(
                    f"contract {version}, field {name}: rule allowed_type; use only {field_type} values"
                )
            if "default" in specification and specification["default"] not in allowed:
                raise ContractError(
                    f"contract {version}, field {name}: rule default_allowed; add the default to allowed"
                )
        normalized[name] = {
            "type": field_type,
            "required": required,
            **({"default": specification["default"]} if "default" in specification else {}),
            **({"allowed": allowed} if allowed is not None else {}),
        }
    return {"format": FORMAT, "version": document["version"], "fields": normalized}


def _has_type(value: object, expected: type) -> bool:
    return type(value) is expected


def diagnostic(version: str, field: str, rule: str, message: str, action: str) -> dict[str, str]:
    return {
        "contract_version": version,
        "field": field,
        "rule": rule,
        "message": message,
        "corrective_action": action,
    }


def compare(old_document: object, new_document: object) -> dict[str, object]:
    old = parse_contract(old_document)
    new = parse_contract(new_document)
    old_fields = old["fields"]
    new_fields = new["fields"]
    findings = []
    for name in sorted(old_fields):
        if name not in new_fields:
            findings.append(diagnostic(
                new["version"], name, "field_removed",
                f"new producer no longer guarantees consumed field {name}",
                "keep the field through migration or add an explicit consumer fallback",
            ))
            continue
        before, after = old_fields[name], new_fields[name]
        if before["type"] != after["type"]:
            findings.append(diagnostic(
                new["version"], name, "type_changed",
                f"type changed from {before['type']} to {after['type']}",
                "publish a new field and dual-read before retiring the old field",
            ))
        before_allowed, after_allowed = before.get("allowed"), after.get("allowed")
        if (
            (before_allowed is None) != (after_allowed is None)
            or before_allowed is not None and set(before_allowed) != set(after_allowed)
        ):
            findings.append(diagnostic(
                new["version"], name, "allowed_values_changed",
                "allowed values changed and an existing consumer may reject or misread data",
                "coordinate consumer support before changing the enum set",
            ))
        if before.get("default") != after.get("default"):
            findings.append(diagnostic(
                new["version"], name, "default_changed",
                "default changed and fallback behavior is no longer stable",
                "retain the old default until all consumers migrate",
            ))
        if before["required"] and not after["required"]:
            findings.append(diagnostic(
                new["version"], name, "required_guarantee_removed",
                "new producer may omit a field previously guaranteed",
                "keep the field required or migrate consumers to a fallback first",
            ))
        if not before["required"] and after["required"]:
            findings.append(diagnostic(
                new["version"], name, "optional_became_required",
                "existing records may omit a field that is now required",
                "backfill and verify all producers before making the field required",
            ))
    for name in sorted(set(new_fields) - set(old_fields)):
        field = new_fields[name]
        if field["required"]:
            findings.append(diagnostic(
                new["version"], name, "required_field_added",
                "existing records and producers do not supply the new required field",
                "add it as optional with a stable default, backfill, then require it later",
            ))
    return {
        "old_version": old["version"],
        "new_version": new["version"],
        "compatible": not findings,
        "findings": findings,
    }


def validate_records(
    contract_document: object, records_document: object, unknown_fields: str
) -> dict[str, object]:
    contract = parse_contract(contract_document)
    if unknown_fields not in {"strict", "permissive"}:
        raise ContractError("unknown-fields policy must be strict or permissive; choose one explicitly")
    if not isinstance(records_document, list):
        raise ContractError("records must be a JSON array; wrap representative objects in a list")
    errors = []
    invalid_indexes = set()
    normalized_records = []
    fields = contract["fields"]
    for index, record in enumerate(records_document):
        if not isinstance(record, dict):
            invalid_indexes.add(index)
            errors.append(diagnostic(
                contract["version"], "<record>", "record_shape",
                f"record {index} is not an object", "replace it with a field/value object",
            ))
            continue
        errors_before_record = len(errors)
        normalized = {}
        for name in sorted(fields):
            specification = fields[name]
            if name not in record:
                if specification["required"]:
                    invalid_indexes.add(index)
                    errors.append(diagnostic(
                        contract["version"], name, "required_missing",
                        f"record {index} is missing required field {name}",
                        "supply the field or select a compatible older consumer contract",
                    ))
                else:
                    normalized[name] = specification["default"]
                continue
            value = record[name]
            if not _has_type(value, TYPES[specification["type"]]):
                invalid_indexes.add(index)
                errors.append(diagnostic(
                    contract["version"], name, "record_type",
                    f"record {index} expected {specification['type']}",
                    "correct the producer value or migrate with a new field",
                ))
            elif "allowed" in specification and value not in specification["allowed"]:
                invalid_indexes.add(index)
                errors.append(diagnostic(
                    contract["version"], name, "record_allowed",
                    f"record {index} value is outside the allowed set",
                    "use an allowed value or coordinate an enum migration",
                ))
            else:
                normalized[name] = value
        for name in sorted(set(record) - set(fields)):
            if unknown_fields == "strict":
                invalid_indexes.add(index)
                errors.append(diagnostic(
                    contract["version"], name, "unknown_field",
                    f"record {index} contains unknown future field {name}",
                    "use permissive mode during rollout or upgrade the consumer contract",
                ))
        if len(errors) == errors_before_record:
            normalized_records.append(normalized)
    return {
        "contract_version": contract["version"],
        "unknown_fields": unknown_fields,
        "valid": not errors,
        "accepted_records": len(records_document) - len(invalid_indexes),
        "errors": errors,
        "records": normalized_records,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    compare_command = commands.add_parser("compare", help="classify producer contract evolution")
    compare_command.add_argument("--old", type=Path, required=True)
    compare_command.add_argument("--new", type=Path, required=True)
    validate_command = commands.add_parser("validate", help="run records through a consumer contract")
    validate_command.add_argument("--contract", type=Path, required=True)
    validate_command.add_argument("--records", type=Path, required=True)
    validate_command.add_argument("--unknown-fields", choices=("strict", "permissive"), required=True)
    args = parser.parse_args()
    try:
        if args.command == "compare":
            result = compare(load_json(args.old), load_json(args.new))
        else:
            result = validate_records(
                load_json(args.contract), load_json(args.records), args.unknown_fields
            )
    except ContractError as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["compatible" if args.command == "compare" else "valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
