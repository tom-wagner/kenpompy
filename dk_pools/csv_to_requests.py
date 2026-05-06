#!/usr/bin/env python3

import argparse
import csv
import hashlib
import json
import random
import re
import sys
from pathlib import Path


PICK_COLUMN_RE = re.compile(r"^[A-Z0-9]+:(\d+)$")
RESOLVED_PICK_RE = re.compile(r"^(\d+):(.+):(\d+)$")
BETTER_PICK_RE = re.compile(r"^BETTER:(\d+)$")
HARDCODED_PICK_ANSWER = 148


def parse_args():
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Convert DraftKings bracket CSV rows into request payload JSON."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=str(here / "example_entry.csv"),
        help="CSV file to convert.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(here / "generated_requests.json"),
        help="Output JSON file.",
    )
    parser.add_argument(
        "-t",
        "--template-json",
        default=str(here / "example_entry.json"),
        help="Template JSON used to carry over non-bracket picks like the tiebreaker.",
    )
    parser.add_argument(
        "-d",
        "--dk-json",
        default=str(here / "dk.json"),
        help="DraftKings bracket metadata used to resolve directive picks.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any row is missing resolved picks instead of skipping it.",
    )
    return parser.parse_args()


def load_template_extras(template_path, csv_pick_item_ids):
    if not template_path.exists():
        return []

    with template_path.open() as handle:
        template = json.load(handle)

    extras = []
    for pick in template.get("picks", []):
        pick_item_id = str(pick.get("pickItemId"))
        if pick_item_id in csv_pick_item_ids:
            continue
        pick = dict(pick)
        if "pickAnswer" in pick:
            pick["pickAnswer"] = HARDCODED_PICK_ANSWER
        extras.append(pick)
    return extras


def load_bracket_metadata(dk_json_path):
    with dk_json_path.open() as handle:
        dk_data = json.load(handle)

    items = dk_data["poolPickItemDetails"]["matchBracketPickItems"]
    picks_by_id = {}
    parents_by_child = {}

    for item in items:
        pick_item_id = str(item["pickItemId"])
        outcomes = []
        outcome_by_id = {}
        for outcome in item.get("matchOutcomePickItemOutcomes", []):
            normalized = {
                "pickItemOutcomeId": int(outcome["pickItemOutcomeId"]),
                "seed": int(outcome["seed"]),
                "teamCode": outcome.get("teamCode"),
                "teamMarket": outcome.get("teamMarket"),
                "teamName": outcome.get("teamName"),
            }
            outcomes.append(normalized)
            outcome_by_id[normalized["pickItemOutcomeId"]] = normalized

        picks_by_id[pick_item_id] = {
            "pickItemId": pick_item_id,
            "roundNumber": int(item["roundNumber"]),
            "childPickItemId": (
                str(item["childPickItemId"]) if item.get("childPickItemId") is not None else None
            ),
            "outcomes": outcomes,
            "outcomeById": outcome_by_id,
        }

        child_pick_item_id = item.get("childPickItemId")
        if child_pick_item_id is not None:
            parents_by_child.setdefault(str(child_pick_item_id), []).append(pick_item_id)

    for parent_ids in parents_by_child.values():
        parent_ids.sort(key=int)

    return picks_by_id, parents_by_child


def pick_columns(fieldnames):
    columns = []
    for fieldname in fieldnames:
        match = PICK_COLUMN_RE.match(fieldname)
        if match:
            columns.append((fieldname, match.group(1)))
    return columns


def parse_resolved_pick(cell_value):
    if cell_value is None:
        return None
    match = RESOLVED_PICK_RE.match(cell_value.strip())
    if not match:
        return None
    return {
        "pickItemOutcomeId": int(match.group(3)),
        "seed": int(match.group(1)),
        "label": match.group(2),
    }


def parse_better_pick(cell_value):
    if cell_value is None:
        return None
    match = BETTER_PICK_RE.match(cell_value.strip())
    if not match:
        return None
    percentage = int(match.group(1))
    if 0 <= percentage <= 100:
        return percentage
    return None


def stable_percentage_roll(row, pick_item_id):
    basis = f"{row.get('entryGroupKey', '')}|{pick_item_id}"
    digest = hashlib.sha256(basis.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big")
    return (value / (1 << 64)) * 100


def available_outcomes_for_pick(pick_item_id, pick_metadata, parents_by_child, chosen_by_pick_item):
    metadata = pick_metadata[pick_item_id]
    if metadata["outcomes"]:
        return metadata["outcomes"]

    parent_pick_item_ids = parents_by_child.get(pick_item_id, [])
    outcomes = []
    for parent_pick_item_id in parent_pick_item_ids:
        chosen = chosen_by_pick_item.get(parent_pick_item_id)
        if chosen is None:
            return None
        outcomes.append(chosen)
    return outcomes if len(outcomes) == 2 else None


def feeder_order_by_child(pick_cols, pick_metadata):
    feeders = {}
    for _, pick_item_id in pick_cols:
        child_pick_item_id = pick_metadata.get(pick_item_id, {}).get("childPickItemId")
        if child_pick_item_id is None:
            continue
        feeders.setdefault(child_pick_item_id, []).append(pick_item_id)
    return feeders


def resolve_pick(row, pick_item_id, cell_value, pick_metadata, parents_by_child, chosen_by_pick_item):
    resolved = parse_resolved_pick(cell_value)
    if resolved is not None:
        metadata = pick_metadata.get(pick_item_id)
        if metadata is None:
            return resolved

        # Round 1 picks have explicit outcomes in the DK metadata.
        if metadata["outcomes"]:
            return metadata["outcomeById"].get(resolved["pickItemOutcomeId"])

        # Inner-round picks must be one of the two winners that fed this game.
        available_outcomes = available_outcomes_for_pick(
            pick_item_id, pick_metadata, parents_by_child, chosen_by_pick_item
        )
        if not available_outcomes:
            return None
        for outcome in available_outcomes:
            if outcome["pickItemOutcomeId"] == resolved["pickItemOutcomeId"]:
                return outcome
        return None

    better_percentage = parse_better_pick(cell_value)
    if better_percentage is None:
        return None

    available_outcomes = available_outcomes_for_pick(
        pick_item_id, pick_metadata, parents_by_child, chosen_by_pick_item
    )
    if not available_outcomes or len(available_outcomes) != 2:
        return None

    ordered = sorted(
        available_outcomes,
        key=lambda outcome: (int(outcome["seed"]), int(outcome["pickItemOutcomeId"])),
    )
    better_outcome = ordered[0]
    worse_outcome = ordered[-1]

    if int(better_outcome["seed"]) == int(worse_outcome["seed"]):
        return random.choice(available_outcomes)

    roll = stable_percentage_roll(row, pick_item_id)
    return better_outcome if roll < better_percentage else worse_outcome


def build_payload(row, pick_cols, template_extras, pick_metadata, parents_by_child):
    missing = []
    picks = []
    chosen_by_pick_item = {}
    ordered_parents_by_child = feeder_order_by_child(pick_cols, pick_metadata)

    for column_name, pick_item_id in pick_cols:
        chosen_outcome = resolve_pick(
            row=row,
            pick_item_id=pick_item_id,
            cell_value=row.get(column_name, ""),
            pick_metadata=pick_metadata,
            parents_by_child=ordered_parents_by_child,
            chosen_by_pick_item=chosen_by_pick_item,
        )
        if chosen_outcome is None:
            missing.append(column_name)
            continue
        chosen_by_pick_item[pick_item_id] = chosen_outcome
        picks.append(
            {
                "pickItemId": pick_item_id,
                "pickItemOutcomeIds": [chosen_outcome["pickItemOutcomeId"]],
            }
        )

    if missing:
        return None, missing

    payload = {
        "entryGroupKeys": [row["entryGroupKey"]],
        "pickSetId": int(row["pickSetId"]),
        "picks": picks + template_extras,
    }
    return payload, []


def main():
    args = parse_args()
    input_csv = Path(args.input_csv).resolve()
    output_json = Path(args.output).resolve()
    template_json = Path(args.template_json).resolve()
    dk_json = Path(args.dk_json).resolve()

    with input_csv.open(newline="") as handle:
        reader = csv.DictReader(handle)
        pick_cols = pick_columns(reader.fieldnames or [])
        csv_pick_item_ids = {pick_item_id for _, pick_item_id in pick_cols}
        template_extras = load_template_extras(template_json, csv_pick_item_ids)
        pick_metadata, parents_by_child = load_bracket_metadata(dk_json)

        payloads = []
        skipped = []
        for row_number, row in enumerate(reader, start=2):
            payload, missing = build_payload(
                row, pick_cols, template_extras, pick_metadata, parents_by_child
            )
            if payload is None:
                skipped.append(
                    {
                        "row_number": row_number,
                        "entryGroupKey": row.get("entryGroupKey", ""),
                        "missing_columns": missing,
                    }
                )
                continue
            payloads.append(payload)

    if skipped and args.strict:
        for item in skipped:
            print(
                f"row {item['row_number']} ({item['entryGroupKey']}): "
                f"unresolved picks in {', '.join(item['missing_columns'])}",
                file=sys.stderr,
            )
        raise SystemExit(1)

    for item in skipped:
        print(
            f"Skipping row {item['row_number']} ({item['entryGroupKey']}): "
            f"unresolved picks in {', '.join(item['missing_columns'])}",
            file=sys.stderr,
        )

    with output_json.open("w") as handle:
        json.dump(payloads, handle, indent=2)
        handle.write("\n")

    print(f"Wrote {len(payloads)} request payload(s) to {output_json}")


if __name__ == "__main__":
    main()
