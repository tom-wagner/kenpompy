import importlib.util
import json
import logging
import os
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "main.py"
SPEC = importlib.util.spec_from_file_location("main_script", MODULE_PATH)
main_script = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(main_script)


def test_apply_xai_minutes_projection_matches_suffix_variant():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Josiah Lake",
                "MinsProj": pd.NA,
                "MINS PROJ": pd.NA,
                "MinsProjConfidence": pd.NA,
                "MinsProjInjurySummary": pd.NA,
                "MinsProjConfidenceJustification": pd.NA,
            }
        ]
    )
    projection_data = [
        {
            "name": "Josiah Lake II",
            "minutes": 34,
            "confidence": 0.95,
            "injurySummary": "",
            "confidenceJustification": "",
        }
    ]

    result = main_script.apply_xai_minutes_projection(team_df, projection_data)

    assert float(result.loc[0, "MinsProj"]) == 34.0
    assert float(result.loc[0, "MINS PROJ"]) == 34.0
    assert float(result.loc[0, "MinsProjConfidence"]) == 0.95


def test_extract_player_urls_maps_canonical_name():
    html = """
    <html><body>
    <a href="player.php?p=54862">Thomas Dowd</a>
    <a href="player.php?p=12345">Josiah Lake</a>
    </body></html>
    """

    urls = main_script.kp_team._extract_player_urls(BeautifulSoup(html, "html.parser"))

    assert urls["thomas dowd"] == "https://kenpom.com/player.php?p=54862"
    assert urls["josiah lake"] == "https://kenpom.com/player.php?p=12345"


def test_apply_xai_minutes_projection_does_not_zero_unmatched_player():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Josiah Lake",
                "MinsProj": pd.NA,
                "MINS PROJ": pd.NA,
                "MinsProjConfidence": pd.NA,
                "MinsProjInjurySummary": pd.NA,
                "MinsProjConfidenceJustification": pd.NA,
            }
        ]
    )
    projection_data = [{"name": "Different Player", "minutes": 22}]

    result = main_script.apply_xai_minutes_projection(team_df, projection_data)

    assert pd.isna(result.loc[0, "MinsProj"])
    assert pd.isna(result.loc[0, "MINS PROJ"])


def test_prepare_player_df_backfills_name_from_index():
    player_df = pd.DataFrame(
        [
            {"Team": "Boston+University", "Name": pd.NA, "Number": pd.NA, "Game -1": 21, "FC/40": 0},
            {"Team": "Boston+University", "Name": "Azmar Abdullah", "Number": 7, "Game -1": 28, "FC/40": 0},
        ],
        index=pd.Index(["Andrew Bhesania", "Azmar Abdullah"], name="Name"),
    )

    result = main_script.prepare_player_df(player_df)

    assert result.loc["Andrew Bhesania", "Name"] == "Andrew Bhesania"
    assert result.loc["Azmar Abdullah", "Name"] == "Azmar Abdullah"


def test_prepare_player_df_drops_unrecoverable_blank_names():
    player_df = pd.DataFrame(
        [
            {"Team": "Boston+University", "Name": pd.NA, "Game -1": 10, "FC/40": 0},
            {"Team": "Boston+University", "Name": "Azmar Abdullah", "Game -1": 28, "FC/40": 0},
        ]
    )

    result = main_script.prepare_player_df(player_df)

    assert list(result["Name"]) == ["Azmar Abdullah"]


def test_build_team_minutes_prompt_requires_exact_roster_names():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Josiah Lake",
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "Last5MinsStdDev": 5.2,
            }
        ]
    )

    prompt = main_script.build_team_minutes_prompt("Oregon St.", team_df)

    assert "exact same player names" in prompt
    assert "Josiah Lake" in prompt
    assert "MUST exactly match one of the provided roster names" in prompt
    assert "last 7 games" in prompt
    assert "top 10 most recent lineups" in prompt
    assert "Use the recent lineup JSON in addition to your evaluation of injuries and minute logs." in prompt
    assert "assume the player can handle a full minutes load unless a coach has specifically said otherwise" in prompt


def test_validate_xai_projection_data_uses_index_when_name_column_missing():
    team_df = pd.DataFrame(
        [
            {
                "MinsProj": pd.NA,
                "MINS PROJ": pd.NA,
                "MinsProjConfidence": pd.NA,
                "MinsProjInjurySummary": pd.NA,
                "MinsProjConfidenceJustification": pd.NA,
            }
        ],
        index=pd.Index(["Josiah Lake"], name="Name"),
    )
    projection_data = {
        "Josiah Lake": {
            "minutes": 200,
            "confidence": 0.95,
            "injurySummary": "",
            "confidenceJustification": "",
        }
    }

    issues = main_script.validate_xai_projection_data(team_df, projection_data)

    assert issues == []


def test_build_team_minutes_prompt_uses_index_when_name_column_missing():
    team_df = pd.DataFrame(
        [
            {
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "Last5MinsStdDev": 5.2,
            }
        ],
        index=pd.Index(["Josiah Lake"], name="Name"),
    )

    prompt = main_script.build_team_minutes_prompt("Oregon St.", team_df)

    assert "Josiah Lake" in prompt


def test_validate_xai_adjustment_data_requires_exact_players_and_zero_sum():
    team_df = pd.DataFrame(
        [
            {"Name": "Player A"},
            {"Name": "Player B"},
        ]
    )
    issues = main_script.validate_xai_adjustment_data(
        team_df,
        "Player A",
        {
            "Player A": {
                "minutesAdjustment": 2,
                "confidence": 0.8,
                "injurySummary": "",
                "confidenceJustification": "",
            },
            "Player B": {
                "minutesAdjustment": -1,
                "confidence": 0.9,
                "injurySummary": "",
                "confidenceJustification": "",
            },
        },
    )

    assert issues == ["adjustments sum to 1, expected 0"]


def test_apply_xai_minutes_adjustments_updates_matching_players_and_metadata():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "MinsProj": 20.0,
                "MINS PROJ": 20.0,
                "MinsProjConfidence": 0.5,
                "MinsProjInjurySummary": "old injury",
                "MinsProjConfidenceJustification": "old note",
            },
            {
                "Name": "Player B",
                "MinsProj": 15.0,
                "MINS PROJ": 15.0,
                "MinsProjConfidence": 0.6,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "",
            },
        ]
    )

    result = main_script.apply_xai_minutes_adjustments(
        team_df,
        {
            "Player A": {
                "minutesAdjustment": 2,
                "confidence": 0.83,
                "injurySummary": "Available but workload uncertain",
                "confidenceJustification": "Beat report suggests a slightly larger role",
            },
            "Player B": {
                "minutesAdjustment": -2,
                "confidence": 0.91,
                "injurySummary": "",
                "confidenceJustification": "",
            },
        },
    )

    assert float(result.loc[0, "MinsProj"]) == 22.0
    assert float(result.loc[0, "MINS PROJ"]) == 22.0
    assert float(result.loc[1, "MinsProj"]) == 13.0
    assert float(result.loc[0, "MinsProjConfidence"]) == 0.83
    assert result.loc[0, "MinsProjInjurySummary"] == "Available but workload uncertain"
    assert result.loc[0, "MinsProjConfidenceJustification"] == "Beat report suggests a slightly larger role"
    assert float(result.loc[1, "MinsProjConfidence"]) == 0.91


def test_apply_last5_minutes_fallback_sets_minutes_and_rebalances_team_total():
    team_df = pd.DataFrame(
        [
            {"Name": "Player A", "Last5AvgMins": 40.0},
            {"Name": "Player B", "Last5AvgMins": 35.0},
            {"Name": "Player C", "Last5AvgMins": 25.0},
            {"Name": "Player D", "Last5AvgMins": 20.0},
            {"Name": "Player E", "Last5AvgMins": 15.0},
            {"Name": "Player F", "Last5AvgMins": 10.0},
            {"Name": "Player G", "Last5AvgMins": 5.0},
            {"Name": "Player H", "Last5AvgMins": 3.0},
        ]
    ).set_index("Name", drop=False)

    result = main_script.apply_last5_minutes_fallback(team_df)

    assert round(float(result["MinsProj"].sum()), 4) == 200.0
    assert round(float(result["MINS PROJ"].sum()), 4) == 200.0
    assert float(result.loc["Player A", "MinsProjConfidence"]) == 0.5
    assert "last 5 games" in result.loc["Player A", "MinsProjConfidenceJustification"]


def test_process_low_confidence_minutes_skips_follow_up_when_flag_disabled():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "%Min": 30,
                "MinsProj": 28,
                "MinsProjConfidence": 0.6,
            }
        ]
    )
    team_status = {}

    result = main_script.process_low_confidence_minutes(
        "Oregon St.",
        team_df,
        team_status,
        run_follow_up=False,
    )

    assert result.equals(team_df)
    assert team_status["low_confidence_players"] == ["Player A"]
    assert team_status["low_confidence_adjustment_status"] == "skipped"


def test_process_low_confidence_minutes_uses_player_cache(monkeypatch):
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "%Min": 30,
                "MinsProj": 28,
                "MINS PROJ": 28,
                "MinsProjConfidence": 0.6,
                "MinsProjInjurySummary": "Available",
                "MinsProjConfidenceJustification": "Uncertain",
            },
            {
                "Name": "Player B",
                "%Min": 20,
                "MinsProj": 22,
                "MINS PROJ": 22,
                "MinsProjConfidence": 0.9,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "",
            },
        ]
    )
    team_status = {"recent_lineup_context": {}}
    minutes_cache = {
        "cacheDate": "03-17-2026",
        "teams": {
            "Oregon St.": {
                "players": {
                    "Player A": {
                        "adjustmentData": {
                            "Player A": {
                                "minutesAdjustment": 2,
                                "confidence": 0.83,
                                "injurySummary": "Available",
                                "confidenceJustification": "Cached adjustment",
                            },
                            "Player B": {
                                "minutesAdjustment": -2,
                                "confidence": 0.92,
                                "injurySummary": "",
                                "confidenceJustification": "",
                            },
                        },
                        "result": {
                            "text": json.dumps(
                                {
                                    "Player A": {
                                        "minutesAdjustment": 2,
                                        "confidence": 0.83,
                                        "injurySummary": "Available",
                                        "confidenceJustification": "Cached adjustment",
                                    },
                                    "Player B": {
                                        "minutesAdjustment": -2,
                                        "confidence": 0.92,
                                        "injurySummary": "",
                                        "confidenceJustification": "",
                                    },
                                }
                            ),
                            "citations": [],
                        },
                    }
                }
            }
        },
    }

    monkeypatch.setattr(
        main_script,
        "call_xai_for_low_confidence_adjustments_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("xAI should not be called")),
    )
    monkeypatch.setattr(main_script, "rebalance_team_minutes", lambda df: df)

    result = main_script.process_low_confidence_minutes(
        "Oregon St.",
        team_df,
        team_status,
        run_follow_up=True,
        follow_up_threshold=0.94,
        minutes_cache=minutes_cache,
    )

    assert float(result.loc[0, "MinsProj"]) == 30.0
    assert float(result.loc[1, "MinsProj"]) == 20.0
    assert team_status["low_confidence_adjustment_player_results"]["Player A"]["cache_hit"] is True


def test_build_low_confidence_minutes_adjustment_prompt_requires_exact_json_object():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "MinsProj": 34,
                "MinsProjConfidence": 0.91,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "Recent volatility",
                "Last5MinsStdDev": 5.2,
            }
        ]
    )

    prompt = main_script.build_low_confidence_minutes_adjustment_prompt(
        "Oregon St.",
        team_df,
        ["Player A"],
    )

    assert "DIG DEEP" in prompt
    assert "x_search" in prompt
    assert "web_search" in prompt
    assert '"minutesAdjustment": 0' in prompt
    assert '"confidence"' in prompt
    assert '"injurySummary"' in prompt
    assert '"confidenceJustification"' in prompt
    assert "must sum to 0" in prompt
    assert "last 7 games" in prompt
    assert "top 10 most recent lineups" in prompt
    assert "corroborate those trends with recent news or comments from the coach regarding playing time" in prompt
    assert "Use the recent lineup JSON in addition to your evaluation of injuries and minute logs." in prompt
    assert "assume the player can handle a full minutes load unless a coach has specifically said otherwise" in prompt


def test_low_confidence_minutes_context_uses_10_game_lookback_for_target_player():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "Game -1": 31,
                "Game -2": 30,
                "Game -3": 29,
                "Game -4": 28,
                "Game -5": 27,
                "Game -6": 26,
                "Game -7": 25,
                "Game -8": 24,
                "Game -9": 23,
                "Game -10": 22,
                "MinsProj": 29,
                "MinsProjConfidence": 0.8,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "",
                "Last5MinsStdDev": 1.4,
            }
        ]
    )

    context = main_script._low_confidence_minutes_context(team_df, ["Player A"])

    assert context["Player A"]["recentMinutes"] == [22, 23, 24, 25, 26, 27, 28, 29, 30, 31]


def test_build_team_minutes_reformat_prompt_includes_reasons_and_exact_example():
    team_df = pd.DataFrame(
        [
            {
                "Name": "Josiah Lake",
                "MinsProj": 34,
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "Last5MinsStdDev": 5.2,
            }
        ]
    )

    prompt = main_script.build_team_minutes_reformat_prompt(
        team_df,
        '{"Josiah Lake":{"minutes":199}}',
        ["minutes sum to 199, expected 200"],
    )

    assert "Why it was malformed" in prompt
    assert "minutes sum to 199, expected 200" in prompt
    assert "no character differences whatsoever" in prompt
    assert '"Josiah Lake"' in prompt
    assert "sum to exactly 200" in prompt


def test_build_low_confidence_reformat_prompt_includes_zero_sum_and_exact_key():
    prompt = main_script.build_low_confidence_minutes_adjustment_reformat_prompt(
        pd.DataFrame([{"Name": "Player A"}, {"Name": "Player B"}]),
        "Player A",
        '{"Player A": {"minutesAdjustment": 2}}',
        ["adjustments sum to 2, expected 0"],
    )

    assert "Why it was malformed" in prompt
    assert "adjustments sum to 2, expected 0" in prompt
    assert '"minutesAdjustment": 0' in prompt
    assert "no character differences whatsoever" in prompt
    assert "sum to 0" in prompt


class _FakeChunk:
    def __init__(self, content):
        self.content = content


class _FakeResponse:
    citations = []


class _FakeChat:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def append(self, message):
        self.requests.append(message)

    def stream(self):
        text = self.responses.pop(0)
        yield _FakeResponse(), _FakeChunk(text)


def test_team_minutes_retry_uses_reformat_prompt_after_validation_failure(monkeypatch):
    team_df = pd.DataFrame(
        [
            {
                "Name": "Josiah Lake",
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "Last5MinsStdDev": 5.2,
            }
        ]
    )
    fake_chat = _FakeChat(
        [
            '{"Josiah Lake":{"minutes":199,"confidence":0.95,"injurySummary":"","confidenceJustification":""}}',
            '{"Josiah Lake":{"minutes":200,"confidence":0.95,"injurySummary":"","confidenceJustification":""}}',
        ]
    )

    monkeypatch.setattr(main_script, "create_xai_chat", lambda: fake_chat)
    monkeypatch.setattr(main_script, "user", lambda text: text)
    monkeypatch.setattr(main_script.time, "sleep", lambda _: None)

    result = main_script.call_xai_for_team_minutes_with_retries("Oregon St.", team_df)

    assert result is not None
    _, projection_data = result
    assert projection_data["Josiah Lake"]["minutes"] == 200
    assert len(fake_chat.requests) == 2
    assert "upcoming game vs Gonzaga" in fake_chat.requests[0]
    assert "Why it was malformed" in fake_chat.requests[1]
    assert "minutes sum to 199, expected 200" in fake_chat.requests[1]
    assert "Do not redo the research" in fake_chat.requests[1]
    assert "no character differences whatsoever" in fake_chat.requests[1]
    assert "sum to exactly 200" in fake_chat.requests[1]


def test_low_confidence_retry_uses_reformat_prompt_after_malformed_json(monkeypatch):
    team_df = pd.DataFrame(
        [
            {
                "Name": "Player A",
                "NextOpponent": "Gonzaga",
                "KenPomResult": "L, 83-63",
                "Game -1": 35,
                "Game -2": 33,
                "Game -3": 43,
                "Game -4": 37,
                "Game -5": 27,
                "MinsProj": 34,
                "MinsProjConfidence": 0.91,
                "MinsProjInjurySummary": "",
                "MinsProjConfidenceJustification": "Recent volatility",
                "Last5MinsStdDev": 5.2,
            }
        ]
    )
    fake_chat = _FakeChat(
        [
            '{"Player A":',
            '{"Player A": {"minutesAdjustment": 0, "confidence": 0.87, "injurySummary": "Available", "confidenceJustification": "No strong signal to change the prior view"}}',
        ]
    )

    monkeypatch.setattr(main_script, "create_xai_chat", lambda: fake_chat)
    monkeypatch.setattr(main_script, "user", lambda text: text)
    monkeypatch.setattr(main_script.time, "sleep", lambda _: None)

    result = main_script.call_xai_for_low_confidence_adjustments_with_retries(
        "Oregon St.",
        team_df,
        "Player A",
    )

    assert result is not None
    _, adjustment_data = result
    assert adjustment_data == {
        "Player A": {
            "minutesAdjustment": 0,
            "confidence": 0.87,
            "injurySummary": "Available",
            "confidenceJustification": "No strong signal to change the prior view",
        }
    }
    assert len(fake_chat.requests) == 2
    assert "DIG DEEP" in fake_chat.requests[0]
    assert "Why it was malformed" in fake_chat.requests[1]
    assert "response was not valid JSON" in fake_chat.requests[1]
    assert '"minutesAdjustment": 0' in fake_chat.requests[1]
    assert '"confidence"' in fake_chat.requests[1]
    assert "sum to 0" in fake_chat.requests[1]


def test_prune_output_directories_keeps_only_three_newest_files(tmp_path, monkeypatch):
    output_dir = tmp_path / "kenpom"
    output_dir.mkdir()
    files = []
    for idx in range(5):
        path = output_dir / f"file_{idx}.txt"
        path.write_text(str(idx), encoding="utf-8")
        timestamp = 1_700_000_000 + idx
        os.utime(path, (timestamp, timestamp))
        files.append(path)

    monkeypatch.setattr(main_script, "OUTPUT_RETENTION_DIRS", (output_dir,))

    main_script.prune_output_directories()

    remaining_names = sorted(path.name for path in output_dir.iterdir())
    assert remaining_names == ["file_2.txt", "file_3.txt", "file_4.txt"]


def test_log_xai_request_and_response_logs_request_and_follow_up(caplog):
    caplog.set_level(logging.INFO)

    main_script.log_xai_request_and_response(
        "low-confidence follow-up",
        "Oregon St.",
        '{"Player A": 0}',
        {
            "text": '{"Player A": -1}',
            "citations": [{"title": "Example"}],
        },
        target_player="Player A",
    )

    log_text = caplog.text
    assert "xAI low-confidence follow-up request for Oregon St. (Player A)" in log_text
    assert '{"Player A": 0}' in log_text
    assert "xAI low-confidence follow-up response for Oregon St. (Player A)" in log_text
    assert '{"Player A": -1}' in log_text
