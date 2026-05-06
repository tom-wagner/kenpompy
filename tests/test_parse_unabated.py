import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "parse_unabated.py"
SPEC = importlib.util.spec_from_file_location("parse_unabated", MODULE_PATH)
parse_unabated = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(parse_unabated)


def test_canonical_book_name_whitelist_aliases():
    assert parse_unabated.canonical_book_name("FanDuel - Delayed") == "fanduel"
    assert parse_unabated.canonical_book_name("CaesarsDirect") == "caesars"
    assert parse_unabated.canonical_book_name("BetMGM Direct") is None
    assert parse_unabated.canonical_book_name("Prophet Exchange") is None
    assert parse_unabated.canonical_book_name("Splash Sports") is None
    assert parse_unabated.canonical_book_name("Hard Rock (S)") is None


def test_allowed_books_matches_canonical_values():
    expected = set(parse_unabated.ALLOWED_BOOK_LABELS.values()) | set(parse_unabated.ALLOWED_BOOK_ALIASES.values())
    assert parse_unabated.ALLOWED_BOOKS == frozenset(expected)


def test_parse_unabated_filters_to_allowed_books():
    payload = {
        "people": {"1": {"firstName": "Test", "lastName": "Player"}},
        "marketSources": [
            {"id": 1, "name": "FanDuel - Delayed"},
            {"id": 2, "name": "CaesarsDirect"},
            {"id": 4, "name": "Fliff"},
            {"id": 3, "name": "Hard Rock (S)"},
        ],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {
                            "ms1": {"points": 10.5, "americanPrice": -110},
                            "ms2": {"points": 10.5, "americanPrice": -105},
                            "ms3": {"points": 10.5, "americanPrice": -120},
                            "ms4": {"points": 11.5, "americanPrice": -130},
                        },
                        "si1": {
                            "ms1": {"points": 10.5, "americanPrice": -110},
                            "ms2": {"points": 10.5, "americanPrice": -115},
                            "ms3": {"points": 10.5, "americanPrice": +100},
                            "ms4": {"points": 11.5, "americanPrice": +100},
                        },
                    },
                }
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload)

    player = parsed["Test Player"]["points"]
    assert set(player.keys()) == {"fanduel", "caesars", "avgObj"}
    assert "fliff" not in player
    assert player["avgObj"]["line"] == 10.5


def test_parse_unabated_includes_milestone_alt_markets():
    payload = {
        "people": {"1": {"firstName": "Alex", "lastName": "Karaban"}},
        "teams": {"10": {"name": "Connecticut"}},
        "marketSources": [
            {"id": 1, "name": "DraftKings"},
            {"id": 2, "name": "FanDuel"},
        ],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-04-04T22:09:00",
                    "betTypeId": 73,
                    "betSubType": "Milestone-Alt",
                    "sides": {
                        "si0": {
                            "ms1": {"points": 14.5, "americanPrice": 125},
                            "ms2": {"points": 14.5, "americanPrice": 148},
                        }
                    },
                }
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload, target_date="2026-04-04")

    player = parsed["Alex Karaban"]
    assert player["__meta__"]["team"] == "Connecticut"
    draftkings_key = next(key for key in player["points"] if key.startswith("draftkings"))
    fanduel_key = next(key for key in player["points"] if key.startswith("fanduel"))
    assert player["points"][draftkings_key]["line"] == 14.5
    assert player["points"][draftkings_key]["over"] == 125
    assert player["points"][draftkings_key]["_variant"] == "Milestone-Alt"
    assert player["points"][fanduel_key]["line"] == 14.5


def test_parse_unabated_keeps_standard_and_alt_markets_separate_for_same_book():
    payload = {
        "people": {"1": {"firstName": "Jake", "lastName": "Davis"}},
        "teams": {"10": {"name": "Illinois"}},
        "marketSources": [{"id": 1, "name": "FanDuel"}],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-04-04T22:09:00",
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 3.5, "americanPrice": 104}},
                        "si1": {"ms1": {"points": 3.5, "americanPrice": -135}},
                    },
                },
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-04-04T22:09:00",
                    "betTypeId": 73,
                    "betSubType": "Milestone-Alt",
                    "sides": {
                        "si0": {"ms1": {"points": 9.5, "americanPrice": 960}},
                    },
                },
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload, target_date="2026-04-04")

    player_points = parsed["Jake Davis"]["points"]
    assert player_points["fanduel"]["line"] == 3.5
    assert player_points["fanduel"]["over"] == 104
    assert player_points["fanduel"]["under"] == -135
    alt_key = next(key for key in player_points if key.startswith("fanduel__variant__"))
    assert player_points[alt_key]["line"] == 9.5
    assert player_points[alt_key]["over"] == 960
    assert player_points[alt_key]["under"] is None


def test_parse_unabated_preserves_team_metadata_and_name_collisions():
    payload = {
        "people": {
            "1": {"firstName": "Test", "lastName": "Player"},
            "2": {"firstName": "Test", "lastName": "Player"},
        },
        "teams": {
            "10": {"name": "Oregon State"},
            "11": {"name": "Saint Mary's"},
        },
        "marketSources": [{"id": 1, "name": "FanDuel"}],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 10.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 10.5, "americanPrice": -110}},
                    },
                },
                {
                    "personId": 2,
                    "teamId": 11,
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 11.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 11.5, "americanPrice": -110}},
                    },
                },
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload)

    assert "Test Player" in parsed
    assert "Test Player (Saint Mary's)" in parsed
    assert parsed["Test Player"]["__meta__"]["team"] == "Oregon State"
    assert parsed["Test Player (Saint Mary's)"]["__meta__"]["team"] == "Saint Mary's"


def test_normalize_cli_date():
    assert parse_unabated.normalize_cli_date("03-09-2026") == "2026-03-09"


def test_parse_unabated_filters_by_event_date():
    payload = {
        "people": {"1": {"firstName": "Test", "lastName": "Player"}},
        "teams": {"10": {"name": "Oregon State"}},
        "marketSources": [{"id": 1, "name": "FanDuel"}],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-03-09T20:00:00",
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 10.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 10.5, "americanPrice": -110}},
                    },
                },
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-03-10T20:00:00",
                    "betTypeId": 77,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 5.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 5.5, "americanPrice": -110}},
                    },
                },
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload, target_date="2026-03-09")

    assert "Test Player" in parsed
    assert "points" in parsed["Test Player"]
    assert "rebounds" not in parsed["Test Player"]


def test_parse_unabated_filters_by_event_date_in_america_chicago():
    payload = {
        "people": {"1": {"firstName": "Christian", "lastName": "Hammond"}},
        "teams": {"10": {"name": "Santa Clara"}},
        "marketSources": [{"id": 1, "name": "FanDuel"}],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-03-10T03:30:00",
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 10.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 10.5, "americanPrice": -110}},
                    },
                }
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload, target_date="2026-03-09")

    assert "Christian Hammond" in parsed
    assert "points" in parsed["Christian Hammond"]


def test_parse_unabated_preserves_game_time_cst_metadata():
    payload = {
        "people": {"1": {"firstName": "Test", "lastName": "Player"}},
        "teams": {"10": {"name": "Oregon State"}},
        "marketSources": [{"id": 1, "name": "FanDuel"}],
        "odds": {
            "game1": [
                {
                    "personId": 1,
                    "teamId": 10,
                    "eventStart": "2026-03-11T20:30:00Z",
                    "betTypeId": 73,
                    "betSubType": None,
                    "sides": {
                        "si0": {"ms1": {"points": 10.5, "americanPrice": -110}},
                        "si1": {"ms1": {"points": 10.5, "americanPrice": -110}},
                    },
                }
            ]
        },
    }

    parsed = parse_unabated.parse_unabated(payload)

    meta = parsed["Test Player"]["__meta__"]
    assert meta["event_start_cst_iso"] == "2026-03-11T15:30:00-05:00"
    assert meta["game_time_cst"] == "2026-03-11 03:30 PM CDT"


def test_count_lines_by_book_counts_each_book_entry():
    parsed = {
        "Player One": {
            "__meta__": {"team": "A"},
            "points": {
                "fanduel": {"line": 10.5, "over": -110, "under": -110},
                "caesars": {"line": 10.5, "over": -105, "under": -115},
                "avgObj": {"line": 10.5, "over": -108, "under": -112},
            },
            "rebounds": {
                "fanduel": {"line": 5.5, "over": -110, "under": -110},
                "caesars": {"line": None, "over": -110, "under": -110},
            },
        },
        "Player Two": {
            "assists": {
                "fanduel": {"line": 4.5, "over": -110, "under": -110},
            }
        },
    }

    counts = parse_unabated.count_lines_by_book(parsed)

    assert counts == {"fanduel": 3, "caesars": 1}
