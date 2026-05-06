import importlib.util
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "unabated_real_lines_eval.py"
SPEC = importlib.util.spec_from_file_location("unabated_real_lines_eval", MODULE_PATH)
unabated_real_lines_eval = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(unabated_real_lines_eval)


def test_build_eval_rows_aggregates_books_to_one_row_per_player_stat():
    projections = pd.DataFrame(
        [
            {
                "Name": "Thomas Dowd",
                "Team": "Troy",
                "NextOpponent": "Arkansas State",
                "PROJ PTS": 15.2,
                "PROJ REB": 7.1,
            }
        ]
    )
    markets = {
        "Thomas Dowd": {
            "__meta__": {"player_name": "Thomas Dowd", "team": "Troy", "normalized_team": "troy"},
            "points": {
                "betmgm": {"line": 14.5, "over": -120, "under": 100},
                "fanduel": {"line": 15.5, "over": -110, "under": -110},
                "avgObj": {"line": 15.0, "over": -115, "under": -105},
            },
            "rebounds": {
                "betmgm": {"line": 6.5, "over": -110, "under": -110},
                "avgObj": {"line": 6.5, "over": -110, "under": -110},
            },
        }
    }

    result = unabated_real_lines_eval.build_eval_rows(projections, markets)

    assert len(result) == 2

    points = result[result["stat"] == "points"].iloc[0]
    assert points["projection"] == 15.2
    assert points["market_line"] == 15.0
    assert points["diff"] == 0.2
    assert points["lean"] == "over"
    assert points["market_over_odds"] == -115
    assert points["market_under_odds"] == -105
    assert points["book_count"] == 2
    assert points["market_books"] == "betmgm 14.5 O -120; U 100 | fanduel 15.5 O -110; U -110"

    rebounds = result[result["stat"] == "rebounds"].iloc[0]
    assert rebounds["market_line"] == 6.5
    assert rebounds["book_count"] == 1
    assert rebounds["market_books"] == "betmgm 6.5 O -110; U -110"


def test_summarize_eval_reports_mae_rmse_and_bias_by_stat():
    eval_df = pd.DataFrame(
        [
            {"stat": "points", "diff": 1.0, "abs_diff": 1.0, "squared_error": 1.0},
            {"stat": "points", "diff": -2.0, "abs_diff": 2.0, "squared_error": 4.0},
            {"stat": "rebounds", "diff": 0.5, "abs_diff": 0.5, "squared_error": 0.25},
        ]
    )

    result = unabated_real_lines_eval.summarize_eval(eval_df)

    points = result[result["stat"] == "points"].iloc[0]
    rebounds = result[result["stat"] == "rebounds"].iloc[0]
    assert points["count"] == 2
    assert points["mae"] == 1.5
    assert points["rmse"] == 1.5811
    assert points["bias"] == -0.5
    assert rebounds["count"] == 1
    assert rebounds["mae"] == 0.5
