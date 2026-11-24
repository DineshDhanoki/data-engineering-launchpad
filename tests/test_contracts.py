import contextlib
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from launchpad.contracts import (
    ContractError,
    compare,
    load_json,
    main,
    parse_contract,
    validate_records,
)
from launchpad.pipeline import read_orders

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "labs" / "03-contracts"
V1 = load_json(LAB / "contracts" / "orders-v1.json")
V2 = load_json(LAB / "contracts" / "orders-v2.json")
RECORDS_V1 = load_json(LAB / "records" / "orders-v1.json")
RECORDS_V2 = load_json(LAB / "records" / "orders-v2.json")


def contract_fixture(name):
    return load_json(LAB / "contracts" / name)


class ContractCompatibilityTests(unittest.TestCase):
    def changed(self, version, mutate):
        contract = copy.deepcopy(V1)
        contract["version"] = version
        mutate(contract["fields"])
        return contract

    def test_optional_field_with_stable_default_is_compatible(self):
        result = compare(V1, V2)
        self.assertTrue(result["compatible"])
        self.assertEqual(result["findings"], [])
        validation = validate_records(V2, RECORDS_V1, "strict")
        self.assertTrue(validation["valid"])
        self.assertEqual(validation["records"][0]["sales_channel"], "unknown")

    def test_required_field_without_migration_is_breaking_and_actionable(self):
        contract = contract_fixture("orders-v2-required-currency.json")
        result = compare(V1, contract)
        self.assertFalse(result["compatible"])
        self.assertEqual(result["findings"][0]["contract_version"], "2.required-currency")
        self.assertEqual(result["findings"][0]["field"], "currency")
        self.assertEqual(result["findings"][0]["rule"], "required_field_added")
        self.assertIn("backfill", result["findings"][0]["corrective_action"])

        required_existing = copy.deepcopy(V2)
        required_existing["version"] = "3.required-channel"
        required_existing["fields"]["sales_channel"] = {
            "type": "string", "required": True,
            "allowed": ["unknown", "web", "mobile", "store"],
        }
        findings = compare(V2, required_existing)["findings"]
        self.assertIn("optional_became_required", {item["rule"] for item in findings})

    def test_removing_or_renaming_consumed_field_is_breaking(self):
        removed = self.changed("2.removed", lambda fields: fields.pop("order_id"))
        renamed = contract_fixture("orders-v2-renamed-order-id.json")
        for contract in (removed, renamed):
            with self.subTest(version=contract["version"]):
                findings = compare(V1, contract)["findings"]
                self.assertIn("field_removed", {item["rule"] for item in findings})
                self.assertIn("order_id", {item["field"] for item in findings})

    def test_type_and_allowed_value_changes_are_breaking(self):
        changed_type = contract_fixture("orders-v2-amount-string.json")
        changed_enum = contract_fixture("orders-v2-narrow-status.json")
        self.assertEqual(compare(V1, changed_type)["findings"][0]["rule"], "type_changed")
        self.assertEqual(
            compare(V1, changed_enum)["findings"][0]["rule"], "allowed_values_changed"
        )

    def test_allowed_value_order_alone_is_not_a_contract_change(self):
        reordered = self.changed(
            "1.0.1", lambda fields: fields["status"].update({
                "allowed": list(reversed(fields["status"]["allowed"]))
            })
        )
        self.assertTrue(compare(V1, reordered)["compatible"])

    def test_unknown_future_field_is_explicitly_strict_or_permissive(self):
        strict = validate_records(V1, RECORDS_V2, "strict")
        permissive = validate_records(V1, RECORDS_V2, "permissive")
        self.assertFalse(strict["valid"])
        self.assertEqual(strict["errors"][0]["field"], "sales_channel")
        self.assertEqual(strict["errors"][0]["rule"], "unknown_field")
        self.assertIn("upgrade", strict["errors"][0]["corrective_action"])
        self.assertTrue(permissive["valid"])
        self.assertNotIn("sales_channel", permissive["records"][0])

    def test_record_errors_name_version_field_rule_and_action(self):
        record = copy.deepcopy(RECORDS_V1[0])
        record["amount_cents"] = "2599"
        result = validate_records(V1, [record, RECORDS_V1[0]], "strict")
        error = result["errors"][0]
        self.assertEqual(
            set(error), {"contract_version", "field", "rule", "message", "corrective_action"}
        )
        self.assertEqual((error["contract_version"], error["field"], error["rule"]),
                         ("1.0.0", "amount_cents", "record_type"))
        self.assertEqual(result["accepted_records"], 1)
        self.assertEqual(len(result["records"]), 1)

    def test_contract_shape_requires_optional_defaults(self):
        invalid = self.changed(
            "2.bad", lambda fields: fields.update({
                "source": {"type": "string", "required": False}
            })
        )
        with self.assertRaisesRegex(ContractError, "2.bad, field source: rule optional_default"):
            parse_contract(invalid)
        required_default = copy.deepcopy(V1)
        required_default["fields"]["order_id"]["default"] = "unknown"
        with self.assertRaisesRegex(ContractError, "rule required_default"):
            parse_contract(required_default)


class ContractCliAndPipelineTests(unittest.TestCase):
    def run_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch("sys.argv", ["launchpad.contracts", *args]),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            try:
                main()
                code = 0
            except SystemExit as exc:
                code = exc.code
        return code, stdout.getvalue(), stderr.getvalue()

    def test_cli_compare_and_strict_failure_have_machine_readable_results(self):
        code, output, error = self.run_cli(
            "compare", "--old", str(LAB / "contracts" / "orders-v1.json"),
            "--new", str(LAB / "contracts" / "orders-v2.json"),
        )
        self.assertEqual((code, error), (0, ""))
        self.assertTrue(json.loads(output)["compatible"])

        code, output, error = self.run_cli(
            "validate", "--contract", str(LAB / "contracts" / "orders-v1.json"),
            "--records", str(LAB / "records" / "orders-v2.json"),
            "--unknown-fields", "strict",
        )
        self.assertEqual((code, error), (1, ""))
        self.assertFalse(json.loads(output)["valid"])

    def test_lab_does_not_change_six_or_seven_column_pipeline_behavior(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            six = root / "six.csv"
            seven = root / "seven.csv"
            six.write_text(
                "order_id,customer_id,order_date,amount_cents,status,updated_at\n"
                "o1,c1,2026-01-01,100,paid,2026-01-01T12:00:00+00:00\n"
            )
            seven.write_text(
                "order_id,customer_id,order_date,amount_cents,status,updated_at,sales_channel\n"
                "o2,c2,2026-01-01,200,paid,2026-01-01T12:00:00+00:00,web\n"
            )
            self.assertEqual(read_orders(six)[0][-1], "unknown")
            self.assertEqual(read_orders(seven)[0][-1], "web")


if __name__ == "__main__":
    unittest.main()
