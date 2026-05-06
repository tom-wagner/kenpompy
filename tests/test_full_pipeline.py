import importlib.util
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "full_pipeline.py"
SPEC = importlib.util.spec_from_file_location("full_pipeline", MODULE_PATH)
full_pipeline = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(full_pipeline)


def test_configure_logging_prunes_outputs_via_main_module(monkeypatch, tmp_path):
    class FakeMainModule:
        def __init__(self) -> None:
            self.pruned = False

        def prune_output_directories(self):
            self.pruned = True

    fake_main = FakeMainModule()

    monkeypatch.setattr(full_pipeline, "PIPELINE_XAI_LOG_DIR", tmp_path / "pipeline_x_ai_logs")
    monkeypatch.setattr(full_pipeline, "load_module", lambda path, module_name: fake_main)

    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        handler.close()

    if hasattr(full_pipeline.configure_logging, "_configured"):
        delattr(full_pipeline.configure_logging, "_configured")
    if hasattr(full_pipeline.configure_logging, "_log_path"):
        delattr(full_pipeline.configure_logging, "_log_path")

    try:
        log_path = full_pipeline.configure_logging()
        assert fake_main.pruned is True
        assert log_path.parent == tmp_path / "pipeline_x_ai_logs"
        assert log_path.exists()
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.handlers[:] = original_handlers
        root_logger.setLevel(original_level)
        if hasattr(full_pipeline.configure_logging, "_configured"):
            delattr(full_pipeline.configure_logging, "_configured")
        if hasattr(full_pipeline.configure_logging, "_log_path"):
            delattr(full_pipeline.configure_logging, "_log_path")


def test_prune_unabated_input_directory_keeps_only_three_newest_files(tmp_path):
    files = []
    for idx in range(5):
        path = tmp_path / f"unabatedResponse_fetch_{idx}.json"
        path.write_text(str(idx), encoding="utf-8")
        timestamp = 1_700_000_000 + idx
        path.touch()
        os.utime(path, (timestamp, timestamp))
        files.append(path)

    full_pipeline.prune_unabated_input_directory(tmp_path, max_files=3)

    remaining_names = sorted(path.name for path in tmp_path.iterdir())
    assert remaining_names == [
        "unabatedResponse_fetch_2.json",
        "unabatedResponse_fetch_3.json",
        "unabatedResponse_fetch_4.json",
    ]


def test_fetch_unabated_payload_writes_json_and_prunes(monkeypatch, tmp_path):
    class FakeHeaders:
        def get(self, key, default=None):
            return default

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"people":[],"odds":[]}'

        @property
        def headers(self):
            return FakeHeaders()

    calls = {"prune": 0}

    monkeypatch.setattr(full_pipeline, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(full_pipeline, "urlopen", lambda request, timeout=30: FakeResponse())
    monkeypatch.setattr(full_pipeline, "uuid4", lambda: "fixed-uuid")
    monkeypatch.setattr(
        full_pipeline,
        "prune_unabated_input_directory",
        lambda directory=None, max_files=3: calls.__setitem__("prune", calls["prune"] + 1),
    )

    output_path = full_pipeline.fetch_unabated_payload()

    assert output_path.parent == tmp_path / "inputs" / "unabated"
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {"people": [], "odds": []}
    assert calls["prune"] == 1


def test_resolve_market_player_matches_suffix_variant():
    lookup = full_pipeline.build_market_lookup({"Josiah Lake": {"points": {}}})

    matched = full_pipeline.resolve_market_player("Josiah Lake II", lookup)

    assert matched is not None
    assert matched[0] == "Josiah Lake"


def test_merge_player_stats_context_backfills_minutes_confidence():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "MINS PROJ": 34.1,
                "PROJ PTS": 15.2,
            }
        ]
    )
    player_stats = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MinsProjConfidence": 0.876,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "Stable role",
                "KenPomPlayerURL": "https://kenpom.com/player.php?p=54862",
            }
        ]
    )

    merged = full_pipeline.merge_player_stats_context(projections, player_stats)

    assert merged.loc[0, "MinsProjConfidence"] == 0.876
    assert merged.loc[0, "MinsProjConfidenceJustification"] == "Stable role"
    assert merged.loc[0, "NextOpponent"] == "Arkansas State"


def test_merge_player_stats_context_ignores_ambiguous_name_index():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "MINS PROJ": 34.1,
            }
        ]
    )
    player_stats = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "MinsProjConfidence": 0.876,
            }
        ]
    ).set_index("Name", drop=False)

    merged = full_pipeline.merge_player_stats_context(projections, player_stats)

    assert merged.loc[0, "MinsProjConfidence"] == 0.876


def test_build_rows_skips_zero_minute_players():
    projections = pd.DataFrame(
        [
            {
                "Name": "Ryan Prather",
                "Team": "Robert+Morris",
                "NextOpponent": "Detroit Mercy",
                "MINS PROJ": 0,
                "Ht": "6-4",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ STL": 0.4,
            }
        ]
    )
    markets = {"Ryan Prather": {"steals": {"fanduel": {"line": 0.5, "over": -120, "under": 100}}}}
    sim_data = {
        "steals": {
            "G": {
                "sample": {
                    "mean": 0.4,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 0, "occurrences": 6000},
                        {"isWholeNumber": True, "total": 1, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert result.empty
    assert {"Player", "team", "stat", "expected_value", "xAiScore", "xAiContext"}.issubset(result.columns)


def test_build_rows_ignores_zero_point_zero_market_lines():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34,
                "MinsProjConfidence": 0.9,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
                "Game -1": 39,
                "Game -2": 37,
                "Game -3": 38,
                "Game -4": 40,
                "Game -5": 39,
                "Game Fouls -1": 3.3,
                "Game Fouls -2": 3.1,
                "Game Fouls -3": 3.2,
                "Game Fouls -4": 3.4,
                "Game Fouls -5": 3.3,
            }
        ]
    )
    markets = {
        "Thomas Dowd": {
            "__meta__": {"player_name": "Thomas Dowd", "team": "Troy", "normalized_team": "troy"},
            "points": {
                "fanduel": {"line": 0.0, "over": -104, "under": -125},
                "sleeper": {"line": 12.5, "over": -118, "under": -141},
            },
        }
    }
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert len(result) == 1
    assert result.loc[0, "book"] == "sleeper"
    assert result.loc[0, "line"] == 12.5
    assert result.loc[0, "full_odds_meta_data"] == "sleeper 12.5 O -118; U -141"


def test_build_rows_treats_alt_variants_as_distinct_market_entries():
    projections = pd.DataFrame(
        [
            {
                "Name": "Jake Davis",
                "Team": "Illinois",
                "NextOpponent": "Connecticut",
                "MINS PROJ": 14.0,
                "Ht": "6-6",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 3.1355,
                "Game -1": 28,
                "Game -2": 17,
                "Game -3": 14,
                "Game -4": 13,
                "Game -5": 14,
                "Game Fouls -1": 1.9,
                "Game Fouls -2": 1.1,
                "Game Fouls -3": 0.9,
                "Game Fouls -4": 0.9,
                "Game Fouls -5": 0.9,
            }
        ]
    )
    markets = {
        "Jake Davis": {
            "__meta__": {"player_name": "Jake Davis", "team": "Illinois", "normalized_team": "illinois"},
            "points": {
                "fanduel": {"line": 3.5, "over": 104, "under": -135},
                "fanduel__variant__milestone_alt_9_5": {
                    "line": 9.5,
                    "over": 960,
                    "under": None,
                    "_base_book": "fanduel",
                    "_variant": "Milestone-Alt",
                },
            },
        }
    }
    sim_data = {
        "points": {
            "GF": {
                "sample": {
                    "mean": 3.1,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 2, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 3, "occurrences": 4000},
                        {"isWholeNumber": True, "total": 4, "occurrences": 3000},
                    ],
                }
            }
        }
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert len(result) == 1
    assert result.loc[0, "book"] == "fanduel"
    assert result.loc[0, "line"] in {3.5, 9.5}
    assert "fanduel 3.5 O +104; U -135" in result.loc[0, "full_odds_meta_data"]
    assert "fanduel [Milestone-Alt] 9.5 O +960; U NA" in result.loc[0, "full_odds_meta_data"]


def test_resolve_market_player_prefers_same_team_for_duplicate_names():
    lookup = full_pipeline.build_market_lookup(
        {
            "Josiah Lake": {
                "__meta__": {"player_name": "Josiah Lake", "team": "Oregon State", "normalized_team": "oregon st"},
                "points": {},
            },
            "Josiah Lake (Other)": {
                "__meta__": {"player_name": "Josiah Lake", "team": "Wright State", "normalized_team": "wright st"},
                "points": {},
            },
        }
    )

    matched = full_pipeline.resolve_market_player("Josiah Lake II", "Oregon+St.", lookup)

    assert matched is not None
    assert matched[0] == "Josiah Lake"
    assert matched[1]["__meta__"]["team"] == "Oregon State"


def test_build_rows_includes_game_time_cst_before_kenpom_player_url(monkeypatch):
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
                "KenPomPlayerURL": "https://kenpom.com/player.php?p=54862",
                "Game -1": 39,
                "Game -2": 37,
                "Game -3": 38,
                "Game -4": 40,
                "Game -5": 39,
                "Game Fouls -1": 3.3,
                "Game Fouls -2": 3.1,
                "Game Fouls -3": 3.2,
                "Game Fouls -4": 3.4,
                "Game Fouls -5": 3.3,
            }
        ]
    )
    markets = {
        "Thomas Dowd": {
            "__meta__": {
                "player_name": "Thomas Dowd",
                "team": "Troy",
                "normalized_team": "troy",
                "event_start_cst_iso": "2026-03-11T18:00:00-05:00",
                "game_time_cst": "2026-03-11 06:00 PM CDT",
            },
            "points": {"fanduel": {"line": 14.5, "over": -120, "under": 100}},
        }
    }
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    monkeypatch.setattr(
        full_pipeline,
        "current_central_time",
        lambda: full_pipeline.datetime.fromisoformat("2026-03-11T10:00:00-05:00"),
    )
    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert "kenpom_player_url" in result.columns
    assert result.columns[-1] == "kenpom_player_url"
    assert result.columns[-2] == "game_time_cst"
    assert result.loc[0, "last_5_games_fouls"] == "[3.3, 3.4, 3.2, 3.1, 3.3]"
    assert result.loc[0, "game_time_cst"] == "2026-03-11 06:00 PM CDT"
    assert result.loc[0, "kenpom_player_url"] == "https://kenpom.com/player.php?p=54862"


def test_build_rows_filters_out_games_that_have_already_started(monkeypatch):
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
            }
        ]
    )
    markets = {
        "Thomas Dowd": {
            "__meta__": {
                "player_name": "Thomas Dowd",
                "team": "Troy",
                "normalized_team": "troy",
                "event_start_cst_iso": "2026-03-11T09:30:00-05:00",
                "game_time_cst": "2026-03-11 09:30 AM CDT",
            },
            "points": {"fanduel": {"line": 14.5, "over": -120, "under": 100}},
        }
    }
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    monkeypatch.setattr(
        full_pipeline,
        "current_central_time",
        lambda: full_pipeline.datetime.fromisoformat("2026-03-11T10:00:00-05:00"),
    )

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert result.empty


def test_build_rows_uses_renamed_columns_and_rounds_win_pct():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34.1234,
                "MinsProjConfidence": 0.876,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
                "KenPomPlayerURL": "https://kenpom.com/player.php?p=54862",
                "Game -1": 39,
                "Game -2": 37,
                "Game -3": 38,
                "Game -4": 40,
                "Game -5": 39,
                "Game Fouls -1": 3.3,
                "Game Fouls -2": 3.1,
                "Game Fouls -3": 3.2,
                "Game Fouls -4": 3.4,
                "Game Fouls -5": 3.3,
            }
        ]
    )
    markets = {"Thomas Dowd": {"points": {"fanduel": {"line": 15.5, "over": -120, "under": 100}}}}
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 2610},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3330},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4060},
                    ],
                }
            }
        }
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert "proj_mins" in result.columns
    assert "conf" in result.columns
    assert "pos" in result.columns
    assert "line" in result.columns
    assert "book" in result.columns
    assert "win_pct" in result.columns
    assert "expected_profit_per_1u" not in result.columns
    assert "projected_minutes" not in result.columns
    assert "projected_minutes_confidence" not in result.columns
    assert "position_bucket" not in result.columns
    assert "best_available_line" not in result.columns
    assert "best_available_sportsbook" not in result.columns
    assert "win_likelihood" not in result.columns
    assert result.loc[0, "proj_mins"] == 34.1234
    assert result.loc[0, "conf"] == 0.876
    assert result.loc[0, "pos"] == "F"
    assert result.loc[0, "line"] == 15.5
    assert result.loc[0, "book"] == "fanduel"
    assert result.loc[0, "win_pct"] == 0.55


def test_build_rows_includes_team_projection_sums_as_team_averages_string():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
                "PROJ REB": 7.2,
                "PROJ AST": 2.3,
                "PROJ 3PM": 1.8,
                "PROJ STL": 1.1,
                "PROJ BLK": 0.9,
                "PROJ TO": 2.4,
            },
            {
                "Name": "Alex Wing",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 28,
                "Ht": "6-4",
                "ARate": 18,
                "OR%": 4,
                "DR%": 9,
                "Blk%": 0.5,
                "PROJ PTS": 11.0,
                "PROJ REB": 4.8,
                "PROJ AST": 3.7,
                "PROJ 3PM": 2.2,
                "PROJ STL": 0.8,
                "PROJ BLK": 0.1,
                "PROJ TO": 1.6,
            },
        ]
    )
    markets = {"Thomas Dowd": {"points": {"fanduel": {"line": 14.5, "over": -120, "under": 100}}}}
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert result.loc[0, "team_averages"] == (
        "Team REB: 12.00, Team AST: 6.00, Team 3PM: 4.00, "
        "Team STL: 1.90, Team BLK: 1.00, Team TO: 4.00"
    )


def test_build_rows_fills_blank_minutes_text_fields_with_na():
    projections = pd.DataFrame(
        [
            {
                "Name": "AJ Dybantsa",
                "Team": "BYU",
                "NextOpponent": "Kansas St.",
                "MINS PROJ": 40,
                "MinsProjConfidence": 0.95,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "",
                "Ht": "6-9",
                "ARate": 18,
                "OR%": 11,
                "DR%": 13,
                "Blk%": 4,
                "PROJ PTS": 30.94,
                "PROJ REB": 9.36,
                "Game -1": 40,
                "Game -2": 36,
                "Game -3": 38,
                "Game -4": 38,
                "Game -5": 40,
            }
        ]
    )
    markets = {
        "AJ Dybantsa": {
            "points_rebounds": {
                "fanduel": {"line": 34.5, "over": -104, "under": -120}
            }
        }
    }
    sim_data = {
        "points": {
            "C": {
                "sample": {
                    "mean": 20.0,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 0, "occurrences": 1},
                        {"isWholeNumber": True, "total": 20, "occurrences": 9999},
                    ],
                }
            }
        },
        "rebounds": {
            "C": {
                "sample": {
                    "mean": 9.36,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 9, "occurrences": 6000},
                        {"isWholeNumber": True, "total": 10, "occurrences": 4000},
                    ],
                }
            }
        },
    }

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
    )

    assert result.loc[0, "minutes_injury_summary"] == "N/A"
    assert result.loc[0, "minutes_confidence_justification"] == "N/A"
    assert result.loc[0, "xAiScore"] == "N/A"
    assert result.loc[0, "xAiContext"] == "N/A"


def test_build_rows_calls_xai_only_above_hurdle_and_includes_component_projections(monkeypatch):
    projections = pd.DataFrame(
        [
            {
                "Name": "AJ Dybantsa",
                "Team": "BYU",
                "NextOpponent": "Kansas St.",
                "MINS PROJ": 40,
                "MinsProjConfidence": 0.95,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "",
                "Ht": "6-9",
                "ARate": 18,
                "OR%": 11,
                "DR%": 13,
                "Blk%": 4,
                "PROJ PTS": 30.94,
                "PROJ REB": 9.36,
                "PROJ AST": 4.5,
                "PROJ 3PM": 2.2,
                "PROJ STL": 1.3,
                "PROJ BLK": 0.7,
                "PROJ TO": 2.9,
                "Game -1": 40,
                "Game -2": 36,
                "Game -3": 38,
                "Game -4": 38,
                "Game -5": 40,
            }
        ]
    )
    markets = {
        "AJ Dybantsa": {
            "points_rebounds": {
                "fanduel": {"line": 34.5, "over": -104, "under": -120}
            }
        }
    }
    sim_data = {
        "points": {
            "C": {
                "sample": {
                    "mean": 20.0,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 0, "occurrences": 1},
                        {"isWholeNumber": True, "total": 20, "occurrences": 9999},
                    ],
                }
            }
        },
        "rebounds": {
            "C": {
                "sample": {
                    "mean": 9.36,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 9, "occurrences": 6000},
                        {"isWholeNumber": True, "total": 10, "occurrences": 4000},
                    ],
                }
            }
        },
    }
    prompts = []

    def fake_score(prompt, **_kwargs):
        prompts.append(prompt)
        return 82, "Minutes are sturdy and the PR projection still clears the line."

    monkeypatch.setattr(full_pipeline, "score_bet_with_xai", fake_score)

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
        call_x_ai=True,
        x_ai_ev_hurdle=1.2,
    )

    assert len(prompts) == 1
    assert '"componentProjections":{"points":30.94,"rebounds":9.36}' in prompts[0]
    assert result.loc[0, "xAiScore"] == 82
    assert result.loc[0, "xAiContext"] == "Minutes are sturdy and the PR projection still clears the line."

    prompts.clear()
    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
        call_x_ai=True,
        x_ai_ev_hurdle=5.0,
    )

    assert len(prompts) == 0
    assert result.loc[0, "xAiScore"] == "N/A"
    assert result.loc[0, "xAiContext"] == "N/A"


def test_extract_target_teams_from_markets_uses_unabated_meta():
    markets = {
        "Player A": {"__meta__": {"team": "Oregon State", "normalized_team": "oregon st"}, "points": {}},
        "Player B": {"__meta__": {"team": "Saint Mary's", "normalized_team": "st marys"}, "rebounds": {}},
    }

    result = full_pipeline.extract_target_teams_from_markets(markets)

    assert result == {"oregon st", "st marys"}


def test_resolve_target_scrape_teams_matches_canonical_names():
    team_statuses = [
        {"team": "Oregon St."},
        {"team": "Saint Mary's"},
        {"team": "Gonzaga"},
    ]

    result = full_pipeline.resolve_target_scrape_teams(team_statuses, {"oregon st", "st marys"})

    assert result == {"Oregon St.", "Saint Mary's"}


def test_run_parallel_team_minutes_workflow_runs_xai_for_all_teams(monkeypatch):
    calls = []

    class FakeQueue:
        def __init__(self, max_parallel):
            self.max_parallel = max_parallel

        def run(self, tasks, worker, progress_callback=None):
            task_list = list(tasks)
            successes = []
            for task in task_list:
                successes.append((task, worker(task)))
            if progress_callback is not None:
                progress_callback(full_pipeline.QueueProgress(
                    event="finished",
                    total=len(task_list),
                    completed=len(successes),
                    active=0,
                    pending=0,
                    succeeded=len(successes),
                    failed=0,
                    max_parallel=self.max_parallel,
                ))
            return SimpleNamespace(successes=successes, dlq=[])

    class FakeMainModule:
        def call_xai_for_team_minutes_with_retries(self, team, df, recent_lineup_context=None):
            calls.append(team)
            projection_data = [
                {"player_name": row["Name"], "projected_minutes": 20.0, "confidence": 0.9}
                for _, row in df.iterrows()
            ]
            return {"text": "ok"}, projection_data

        def validate_xai_projection_data(self, df, projection_data):
            return []

        def apply_xai_minutes_projection(self, df, projection_data):
            updated = df.copy()
            updated["MinsProj"] = 20.0
            updated["MINS PROJ"] = 20.0
            updated["MinsProjConfidence"] = 0.9
            return updated

        def rebalance_team_minutes(self, df):
            return df

        def _low_confidence_players(self, df, threshold):
            return []

    team_statuses = [
        {"team": "Oregon St.", "df": pd.DataFrame([{"Name": "A"}]), "xai_status": "not_attempted", "issues": [], "low_confidence_adjustment_status": "not_attempted", "low_confidence_adjustment_issues": []},
        {"team": "Gonzaga", "df": pd.DataFrame([{"Name": "B"}]), "xai_status": "not_attempted", "issues": [], "low_confidence_adjustment_status": "not_attempted", "low_confidence_adjustment_issues": []},
    ]

    monkeypatch.setattr(full_pipeline, "ParallelXAIQueue", FakeQueue)

    full_pipeline.run_parallel_team_minutes_workflow(
        main_module=FakeMainModule(),
        team_statuses=team_statuses,
        run_follow_up_minutes=False,
        follow_up_threshold=0.8,
    )

    assert calls == ["Oregon St.", "Gonzaga"]
    assert all(status["xai_status"] == "ok" for status in team_statuses)


def test_minutes_stage_cache_updates_existing_date_file(tmp_path, monkeypatch):
    monkeypatch.setattr(full_pipeline, "MINUTES_CACHE_DIR", tmp_path / "minutes_cache")

    cache_payload, cache_path = full_pipeline.load_minutes_stage_cache("03-17-2026")
    assert cache_path == tmp_path / "minutes_cache" / "03-17-2026.json"
    assert cache_payload["cacheDate"] == "03-17-2026"

    team_entry = full_pipeline.get_minutes_cache_team_entry(cache_payload, "Oregon St.")
    team_entry["teamMinutes"] = {
        "projectionData": {"Player A": {"minutes": 200, "confidence": 0.9, "injurySummary": "", "confidenceJustification": ""}},
        "result": {"text": '{"Player A":{"minutes":200,"confidence":0.9,"injurySummary":"","confidenceJustification":""}}', "citations": []},
    }
    first_path = full_pipeline.write_minutes_stage_cache("03-17-2026", cache_payload)

    cache_payload_reloaded, reloaded_path = full_pipeline.load_minutes_stage_cache("03-17-2026")
    full_pipeline.get_minutes_cache_team_entry(cache_payload_reloaded, "Gonzaga")["teamMinutes"] = {
        "projectionData": {"Player B": {"minutes": 200, "confidence": 0.91, "injurySummary": "", "confidenceJustification": ""}},
        "result": {"text": '{"Player B":{"minutes":200,"confidence":0.91,"injurySummary":"","confidenceJustification":""}}', "citations": []},
    }
    second_path = full_pipeline.write_minutes_stage_cache("03-17-2026", cache_payload_reloaded)

    assert first_path == second_path == reloaded_path
    assert len(list((tmp_path / "minutes_cache").glob("*.json"))) == 1

    written_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert set(written_payload["teams"].keys()) == {"Gonzaga", "Oregon St."}


def test_write_minutes_stage_cache_coerces_iterable_citations(tmp_path, monkeypatch):
    class FakeRepeatedScalarContainer:
        def __iter__(self):
            yield "https://example.com/a"
            yield "https://example.com/b"

    monkeypatch.setattr(full_pipeline, "MINUTES_CACHE_DIR", tmp_path / "minutes_cache")
    cache_payload, cache_path = full_pipeline.load_minutes_stage_cache("03-17-2026")

    team_entry = full_pipeline.get_minutes_cache_team_entry(cache_payload, "Arkansas")
    team_entry["players"] = {
        "DJ Wagner": {
            "adjustmentData": {"DJ Wagner": {"minutesAdjustment": 0, "confidence": 0.9, "injurySummary": "", "confidenceJustification": ""}},
            "result": {
                "text": '{"DJ Wagner":{"minutesAdjustment":0,"confidence":0.9,"injurySummary":"","confidenceJustification":""}}',
                "citations": FakeRepeatedScalarContainer(),
            },
        }
    }

    full_pipeline.write_minutes_stage_cache("03-17-2026", cache_payload)

    written_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    citations = written_payload["teams"]["Arkansas"]["players"]["DJ Wagner"]["result"]["citations"]
    assert citations == ["https://example.com/a", "https://example.com/b"]


def test_run_parallel_team_minutes_workflow_uses_minutes_cache(monkeypatch):
    calls = []

    class FakeQueue:
        def __init__(self, max_parallel):
            self.max_parallel = max_parallel

        def run(self, tasks, worker, progress_callback=None):
            task_list = list(tasks)
            successes = [(task, worker(task)) for task in task_list]
            if progress_callback is not None:
                progress_callback(full_pipeline.QueueProgress(
                    event="finished",
                    total=len(task_list),
                    completed=len(successes),
                    active=0,
                    pending=0,
                    succeeded=len(successes),
                    failed=0,
                    max_parallel=self.max_parallel,
                ))
            return SimpleNamespace(successes=successes, dlq=[])

    class FakeMainModule:
        def call_xai_for_team_minutes_with_retries(self, team, df, recent_lineup_context=None):
            calls.append(team)
            return (
                {"text": f"{team} fresh", "citations": []},
                {"Fresh Player": {"minutes": 200, "confidence": 0.9, "injurySummary": "", "confidenceJustification": ""}},
            )

        def validate_xai_projection_data(self, df, projection_data):
            return []

        def apply_xai_minutes_projection(self, df, projection_data):
            updated = df.copy()
            updated["MinsProj"] = 200.0
            updated["MINS PROJ"] = 200.0
            updated["MinsProjConfidence"] = 0.9
            return updated

        def rebalance_team_minutes(self, df):
            return df

        def _low_confidence_players(self, df, threshold):
            return []

    minutes_cache = {
        "cacheDate": "03-17-2026",
        "teams": {
            "Oregon St.": {
                "players": {},
                "teamMinutes": {
                    "projectionData": {
                        "Cached Player": {
                            "minutes": 200,
                            "confidence": 0.95,
                            "injurySummary": "",
                            "confidenceJustification": "",
                        }
                    },
                    "result": {"text": "cached", "citations": []},
                },
            }
        },
    }
    team_statuses = [
        {"team": "Oregon St.", "df": pd.DataFrame([{"Name": "Cached Player"}]), "xai_status": "not_attempted", "issues": [], "low_confidence_adjustment_status": "not_attempted", "low_confidence_adjustment_issues": []},
        {"team": "Gonzaga", "df": pd.DataFrame([{"Name": "Fresh Player"}]), "xai_status": "not_attempted", "issues": [], "low_confidence_adjustment_status": "not_attempted", "low_confidence_adjustment_issues": []},
    ]

    monkeypatch.setattr(full_pipeline, "ParallelXAIQueue", FakeQueue)

    full_pipeline.run_parallel_team_minutes_workflow(
        main_module=FakeMainModule(),
        team_statuses=team_statuses,
        run_follow_up_minutes=False,
        follow_up_threshold=0.8,
        minutes_cache=minutes_cache,
    )

    assert calls == ["Gonzaga"]
    assert team_statuses[0]["minutes_cache_hit"] is True
    assert team_statuses[1]["minutes_cache_hit"] is False
    assert "Gonzaga" in minutes_cache["teams"]
    assert minutes_cache["teams"]["Gonzaga"]["teamMinutes"]["result"]["text"] == "Gonzaga fresh"


def test_build_rows_parallel_bet_grading_preserves_scores(monkeypatch):
    class FakeQueue:
        def __init__(self, max_parallel):
            self.max_parallel = max_parallel

        def run(self, tasks, worker, progress_callback=None):
            task_list = list(tasks)
            if progress_callback is not None:
                progress_callback(full_pipeline.QueueProgress(
                    event="started",
                    total=len(task_list),
                    completed=0,
                    active=min(len(task_list), self.max_parallel),
                    pending=max(len(task_list) - self.max_parallel, 0),
                    succeeded=0,
                    failed=0,
                    max_parallel=self.max_parallel,
                ))
            return SimpleNamespace(successes=[(task, worker(task)) for task in tasks], dlq=[])

    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "MINS PROJ": 34,
                "MinsProjConfidence": 0.9,
                "Ht": "6-8",
                "ARate": 10,
                "OR%": 5,
                "DR%": 10,
                "Blk%": 1,
                "PROJ PTS": 15.2,
                "Game -1": 39,
                "Game -2": 37,
                "Game -3": 38,
                "Game -4": 40,
                "Game -5": 39,
                "Game Fouls -1": 3.3,
                "Game Fouls -2": 3.1,
                "Game Fouls -3": 3.2,
                "Game Fouls -4": 3.4,
                "Game Fouls -5": 3.3,
            }
        ]
    )
    markets = {"Thomas Dowd": {"points": {"fanduel": {"line": 14.5, "over": -120, "under": 100}}}}
    sim_data = {
        "points": {
            "F": {
                "sample": {
                    "mean": 15.2,
                    "simulationDetails": [
                        {"isWholeNumber": True, "total": 14, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 15, "occurrences": 3000},
                        {"isWholeNumber": True, "total": 16, "occurrences": 4000},
                    ],
                }
            }
        }
    }

    monkeypatch.setattr(full_pipeline, "ParallelXAIQueue", FakeQueue)
    monkeypatch.setattr(
        full_pipeline,
        "score_bet_with_xai",
        lambda prompt, **kwargs: (77, "Parallel grading ok"),
    )

    result = full_pipeline.build_rows(
        projections=projections,
        markets=markets,
        sim_data=sim_data,
        model_type="existing",
        projection_file=Path("dummy.xlsx"),
        call_x_ai=True,
        x_ai_ev_hurdle=1.0,
    )

    assert result.loc[0, "xAiScore"] == 77
    assert result.loc[0, "xAiContext"] == "Parallel grading ok"
    assert "_xai_prompt" not in result.columns


def test_hydrate_recent_lineup_contexts_for_workbook_populates_statuses():
    statuses = [{"team": "Gonzaga", "recent_lineup_context": None}]

    class FakeMainModule:
        def login(self, username, password):
            class Browser:
                pass

            return Browser()

        def get_recent_lineup_context(self, browser, team):
            return {"lineups": [{"pctMinutes": 0.2, "lineup": ["A", "B", "C", "D", "E"]}], "coveragePctMinutes": 0.2, "unknownPctMinutes": 0.0}

    full_pipeline.hydrate_recent_lineup_contexts_for_workbook(FakeMainModule(), statuses)

    assert statuses[0]["recent_lineup_context"]["coveragePctMinutes"] == 0.2


def test_validate_minutes_player_df_rejects_duplicate_team_player_rows():
    player_df = pd.DataFrame(
        [
            {"Team": "A", "Name": "P", "MinsProj": 20, "MinsProjConfidence": 0.8},
            {"Team": "A", "Name": "P", "MinsProj": 21, "MinsProjConfidence": 0.9},
        ]
    )

    try:
        full_pipeline.validate_minutes_player_df(player_df, stage_label="test")
    except RuntimeError as exc:
        assert "duplicate team/player rows" in str(exc)
    else:
        raise AssertionError("Expected duplicate validation failure")


def test_validate_minutes_player_df_rejects_null_player_names():
    player_df = pd.DataFrame(
        [
            {"Team": "A", "Name": pd.NA, "MinsProj": 20, "MinsProjConfidence": 0.8},
        ]
    )

    try:
        full_pipeline.validate_minutes_player_df(player_df, stage_label="test")
    except RuntimeError as exc:
        assert "blank player names" in str(exc)
    else:
        raise AssertionError("Expected blank-name validation failure")
