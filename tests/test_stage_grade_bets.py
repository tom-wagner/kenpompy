import importlib.util
import os
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "stage_grade_bets.py"
SPEC = importlib.util.spec_from_file_location("stage_grade_bets", MODULE_PATH)
stage_grade_bets = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stage_grade_bets)


def test_prune_unabated_input_directory_keeps_only_three_newest_files(tmp_path):
    files = []
    for idx in range(5):
        path = tmp_path / f"unabatedResponse_fetch_{idx}.json"
        path.write_text(str(idx), encoding="utf-8")
        timestamp = 1_700_000_000 + idx
        os.utime(path, (timestamp, timestamp))
        files.append(path)

    stage_grade_bets.prune_unabated_input_directory(tmp_path, max_files=3)

    remaining_names = sorted(path.name for path in tmp_path.iterdir())
    assert remaining_names == [
        "unabatedResponse_fetch_2.json",
        "unabatedResponse_fetch_3.json",
        "unabatedResponse_fetch_4.json",
    ]


def test_find_latest_existing_analysis_csv_returns_newest_matching_csv(tmp_path):
    non_matching = tmp_path / "notes.csv"
    non_matching.write_text("foo,bar\n1,2\n", encoding="utf-8")

    older = tmp_path / "older.csv"
    older.write_text("Player,team,stat,line,manualRtg,manualNotes\nA,B,Points,10,4,keep\n", encoding="utf-8")
    os.utime(older, (1_700_000_001, 1_700_000_001))

    newer = tmp_path / "newer.csv"
    newer.write_text("Player,team,stat,line,manualRtg,manualNotes\nC,D,Rebounds,8,5,latest\n", encoding="utf-8")
    os.utime(newer, (1_700_000_002, 1_700_000_002))

    assert stage_grade_bets.find_latest_existing_analysis_csv(tmp_path) == newer


def test_prune_stage_grade_output_directory_keeps_only_ten_newest_files(tmp_path):
    for idx in range(12):
        path = tmp_path / f"pipeline_output_{idx:02d}.csv"
        path.write_text("Player,team,stat,line,manualRtg,manualNotes\n", encoding="utf-8")
        timestamp = 1_700_000_000 + idx
        os.utime(path, (timestamp, timestamp))

    stage_grade_bets.prune_stage_grade_output_directory(tmp_path, max_files=10)

    remaining_names = sorted(path.name for path in tmp_path.iterdir())
    assert "pipeline_output_00.csv" not in remaining_names
    assert "pipeline_output_01.csv" not in remaining_names
    assert "pipeline_output_02.csv" in remaining_names
    assert "pipeline_output_11.csv" in remaining_names


def test_merge_existing_analysis_copies_manual_and_context_columns(tmp_path):
    existing_analysis = tmp_path / "existing.csv"
    existing_analysis.write_text(
        "\n".join(
            [
                "Player,team,stat,line,manualRtg,manualNotes,opponent,conf,last_5_games,last_5_games_fouls",
                "Jane Doe,Team A,points,10,4,strong read,Team B,0.91,\"[31, 29, 33, 28, 30]\",\"[2, 3, 4, 2, 1]\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    output_df = pd.DataFrame(
        [
            {
                "Player": "Jane Doe",
                "team": "Team A",
                "stat": "points",
                "line": 10,
                "manualRtg": "",
                "manualNotes": "",
                "opponent": "Wrong Opponent",
                "conf": float("nan"),
                "last_5_games": "",
                "last_5_games_fouls": "",
            }
        ]
    )

    merged = stage_grade_bets.merge_existing_analysis(output_df, existing_analysis)

    assert merged.loc[0, "manualRtg"] == "4"
    assert merged.loc[0, "manualNotes"] == "strong read"
    assert merged.loc[0, "opponent"] == "Wrong Opponent"
    assert merged.loc[0, "conf"] == 0.91
    assert merged.loc[0, "last_5_games"] == "[31, 29, 33, 28, 30]"
    assert merged.loc[0, "last_5_games_fouls"] == "[2, 3, 4, 2, 1]"


def test_merge_existing_analysis_preserves_optional_columns_compatibility_and_flags_line_changes(tmp_path):
    existing_analysis = tmp_path / "existing.csv"
    existing_analysis.write_text(
        "Player,team,stat,line,manualRtg,manualNotes\nJane Doe,Team A,points,10,4,strong read\n",
        encoding="utf-8",
    )

    output_df = pd.DataFrame(
        [
            {
                "Player": "Jane Doe",
                "team": "Team A",
                "stat": "points",
                "line": 11,
                "manualRtg": "",
                "manualNotes": "",
            }
        ]
    )

    merged = stage_grade_bets.merge_existing_analysis(output_df, existing_analysis)

    assert merged.loc[0, "manualRtg"] == "4"
    assert merged.loc[0, "manualNotes"] == "LINE_CHANGED_10_TO_11: strong read"
    assert merged.loc[0, "opponent"] == ""
    assert merged.loc[0, "conf"] == ""
    assert merged.loc[0, "last_5_games"] == ""
    assert merged.loc[0, "last_5_games_fouls"] == ""


def test_merge_existing_analysis_copies_context_columns_by_player_team_when_stat_differs(tmp_path):
    existing_analysis = tmp_path / "existing.csv"
    existing_analysis.write_text(
        "\n".join(
            [
                "Player,team,stat,line,manualRtg,manualNotes,opponent,conf,last_5_games,last_5_games_fouls",
                "Jane Doe,Team A,points,10,4,strong read,Team B,0.91,\"[31, 29, 33, 28, 30]\",\"[2, 3, 4, 2, 1]\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    output_df = pd.DataFrame(
        [
            {
                "Player": "Jane Doe",
                "team": "Team A",
                "stat": "rebounds",
                "line": 7,
                "manualRtg": "",
                "manualNotes": "",
                "opponent": "",
                "conf": float("nan"),
                "last_5_games": "",
                "last_5_games_fouls": "",
            }
        ]
    )

    merged = stage_grade_bets.merge_existing_analysis(output_df, existing_analysis)

    assert merged.loc[0, "manualRtg"] == ""
    assert merged.loc[0, "manualNotes"] == ""
    assert merged.loc[0, "opponent"] == "Team B"
    assert merged.loc[0, "conf"] == 0.91
    assert merged.loc[0, "last_5_games"] == "[31, 29, 33, 28, 30]"
    assert merged.loc[0, "last_5_games_fouls"] == "[2, 3, 4, 2, 1]"


def test_merge_existing_analysis_preserves_current_context_when_present(tmp_path):
    existing_analysis = tmp_path / "existing.csv"
    existing_analysis.write_text(
        "\n".join(
            [
                "Player,team,stat,line,manualRtg,manualNotes,opponent,conf,last_5_games,last_5_games_fouls,kenpom_player_url",
                "Ivan Kharchenkov,Arizona,rebounds,6.5,3,old note,Utah St.,0.95,\"[24, 29, 32, 36, 26]\",\"[1.4, 1.7, 1.8, 2.1, 1.5]\",https://kenpom.com/player.php?p=59287",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    output_df = pd.DataFrame(
        [
            {
                "Player": "Ivan Kharchenkov",
                "team": "Arizona",
                "stat": "points",
                "line": 8.5,
                "manualRtg": "",
                "manualNotes": "",
                "opponent": "Utah St.",
                "conf": 0.95,
                "last_5_games": "[36, 26, 36, 35, 38]",
                "last_5_games_fouls": "[1.8, 1.3, 1.8, 1.8, 1.9]",
                "kenpom_player_url": "https://kenpom.com/player.php?p=59287",
            }
        ]
    )

    merged = stage_grade_bets.merge_existing_analysis(output_df, existing_analysis)

    assert merged.loc[0, "manualRtg"] == ""
    assert merged.loc[0, "manualNotes"] == ""
    assert merged.loc[0, "opponent"] == "Utah St."
    assert merged.loc[0, "conf"] == 0.95
    assert merged.loc[0, "last_5_games"] == "[36, 26, 36, 35, 38]"
    assert merged.loc[0, "last_5_games_fouls"] == "[1.8, 1.3, 1.8, 1.8, 1.9]"
    assert merged.loc[0, "kenpom_player_url"] == "https://kenpom.com/player.php?p=59287"
