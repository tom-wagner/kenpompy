from __future__ import annotations

import math

import pandas as pd

from scripts import stage_augment_with_providers


def test_provider_matcher_fuzzy_matches_composite_player_team_key() -> None:
    provider_df = pd.DataFrame(
        [
            {
                "PLAYER": "John Doe",
                "TEAMNAME": "Saint Mary's (CA)",
                "TEAM": "STMRY",
                "MINUTES": 31.0,
                "PTS": 15.5,
                "REB": 4.0,
                "AST": 2.5,
                "STL": 1.0,
                "BLK": 0.5,
                "3PM": 2.0,
            }
        ]
    )
    base_df = pd.DataFrame([{"Player": "Jon Doe", "team": "Saint Mary's", "stat": "points"}])
    unique_target_teams = stage_augment_with_providers.build_unique_player_team_map(
        base_df,
        player_col="Player",
        team_col="team",
    )
    records = stage_augment_with_providers.build_provider_records(
        provider_df,
        player_col="PLAYER",
        team_col="TEAMNAME",
        extra_team_cols=("TEAM",),
        values_map={"rg_mins": "MINUTES", "PTS": "PTS"},
        unique_target_teams_by_player=unique_target_teams,
    )
    matcher = stage_augment_with_providers.ProviderMatcher(records)

    match = matcher.resolve("Jon Doe", "Saint Mary's")

    assert match is not None
    assert match.player_name == "John Doe"
    assert math.isclose(stage_augment_with_providers.numeric_or_nan(match.values["PTS"]), 15.5)


def test_augment_with_rg_maps_minutes_and_combo_stats() -> None:
    base_df = pd.DataFrame(
        [
            {"Player": "Liam Campbell", "team": "Saint Mary's", "stat": "points", "proj_mins": 5.0, "stat_projection": 1.0},
            {
                "Player": "Liam Campbell",
                "team": "Saint Mary's",
                "stat": "points_rebounds_assists",
                "proj_mins": 5.0,
                "stat_projection": 2.0,
            },
        ]
    )
    rg_df = pd.DataFrame(
        [
            {
                "PLAYER": "Liam Campbell",
                "TEAMNAME": "Saint Mary's (CA)",
                "TEAM": "STMRY",
                "MINUTES": 6.0,
                "PTS": 1.53,
                "REB": 0.67,
                "AST": 0.19,
                "STL": 0.08,
                "BLK": 0.0,
                "3PM": 0.19,
            }
        ]
    )

    augmented, matched_rows = stage_augment_with_providers.augment_with_rg(base_df, rg_df)
    augmented = stage_augment_with_providers.order_provider_columns(augmented)

    assert matched_rows == 2
    assert list(augmented.columns) == [
        "Player",
        "team",
        "stat",
        "proj_mins",
        "stat_projection",
        "rg_mins",
        "rg_mins_diff",
        "rg_proj",
        "rg_proj_diff",
    ]
    assert math.isclose(float(augmented.loc[0, "rg_mins"]), 6.0)
    assert math.isclose(float(augmented.loc[0, "rg_mins_diff"]), 1.0, rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[0, "rg_proj"]), 1.53, rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[0, "rg_proj_diff"]), 0.53, rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[1, "rg_proj"]), round(1.53 + 0.67 + 0.19, 2), rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[1, "rg_proj_diff"]), round((1.53 + 0.67 + 0.19) - 2.0, 2), rel_tol=1e-9)


def test_augment_with_etr_derives_scaled_projection_from_fantasy_points() -> None:
    base_df = pd.DataFrame(
        [
            {"Player": "Rashaun Agee", "team": "Texas A&M", "stat": "points", "proj_mins": 28.0, "stat_projection": 12.0},
            {
                "Player": "Rashaun Agee",
                "team": "Texas A&M",
                "stat": "points_rebounds_assists",
                "proj_mins": 28.0,
                "stat_projection": 22.0,
            },
            {"Player": "Rashaun Agee", "team": "Texas A&M", "stat": "threes", "proj_mins": 28.0, "stat_projection": 1.5},
        ]
    )
    etr_df = pd.DataFrame(
        [
            {
                "Name": "Rashaun Agee",
                "Team": "TA&amp;M",
                "Minutes": 30.0,
                "DK Points": 30.0,
            }
        ]
    )
    projections_df = pd.DataFrame(
        [
            {
                "Name": "Rashaun Agee",
                "Team": "Texas A&M",
                "PROJ PTS": 16.0,
                "PROJ REB": 8.0,
                "PROJ AST": 4.0,
                "PROJ STL": 1.0,
                "PROJ BLK": 0.5,
                "PROJ TO": 2.0,
                "PROJ 3PM": 2.0,
            }
        ]
    )

    augmented, matched_rows = stage_augment_with_providers.augment_with_etr(base_df, etr_df, projections_df)
    augmented = stage_augment_with_providers.order_provider_columns(augmented)

    # 16 + (2 * 0.5) + (8 * 1.25) + (4 * 1.5) + (1 * 2) + (0.5 * 2) - (2 * 0.5) = 35
    scale_factor = 30.0 / 35.0

    assert matched_rows == 3
    assert list(augmented.columns) == [
        "Player",
        "team",
        "stat",
        "proj_mins",
        "stat_projection",
        "etr_mins",
        "etr_mins_diff",
        "etr_proj",
        "etr_proj_diff",
    ]
    assert math.isclose(float(augmented.loc[0, "etr_mins"]), 30.0, rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[0, "etr_mins_diff"]), 2.0, rel_tol=1e-9)
    assert math.isclose(float(augmented.loc[0, "etr_proj"]), round(16.0 * scale_factor, 2), rel_tol=1e-9)
    assert math.isclose(
        float(augmented.loc[0, "etr_proj_diff"]),
        round((16.0 * scale_factor) - 12.0, 2),
        rel_tol=1e-9,
    )
    assert math.isclose(
        float(augmented.loc[1, "etr_proj"]),
        round((16.0 + 8.0 + 4.0) * scale_factor, 2),
        rel_tol=1e-9,
    )
    assert math.isclose(
        float(augmented.loc[1, "etr_proj_diff"]),
        round(((16.0 + 8.0 + 4.0) * scale_factor) - 22.0, 2),
        rel_tol=1e-9,
    )
    assert math.isclose(float(augmented.loc[2, "etr_proj"]), round(2.0 * scale_factor, 2), rel_tol=1e-9)


def test_order_provider_columns_matches_requested_layout() -> None:
    df = pd.DataFrame(
        [
            {
                "Player": "A",
                "team": "B",
                "stat": "points",
                "etr_mins": 3.0,
                "etr_mins_diff": 0.5,
                "etr_proj": 2.0,
                "etr_proj_diff": 0.25,
                "rg_mins": 4.0,
                "rg_mins_diff": 1.0,
                "rg_proj": 1.0,
                "rg_proj_diff": -0.5,
            }
        ]
    )

    ordered = stage_augment_with_providers.order_provider_columns(df)

    assert list(ordered.columns) == [
        "Player",
        "team",
        "stat",
        "etr_mins",
        "etr_mins_diff",
        "etr_proj",
        "etr_proj_diff",
        "rg_mins",
        "rg_mins_diff",
        "rg_proj",
        "rg_proj_diff",
    ]


def test_carry_forward_manual_fields_preserves_manual_annotations(tmp_path) -> None:
    existing_path = tmp_path / "existing.csv"
    existing_df = pd.DataFrame(
        [
            {
                "Player": "Javohn Garcia",
                "team": "McNeese",
                "stat": "points",
                "line": 11.5,
                "manualRtg": "4",
                "manualNotes": "strong read",
            }
        ]
    )
    existing_df.to_csv(existing_path, index=False)

    output_df = pd.DataFrame(
        [
            {
                "Player": "Javohn Garcia",
                "team": "McNeese",
                "stat": "points",
                "line": 11.5,
                "manualRtg": "",
                "manualNotes": "",
            }
        ]
    )

    merged = stage_augment_with_providers.carry_forward_manual_fields(output_df, existing_path)

    assert merged.loc[0, "manualRtg"] == "4"
    assert merged.loc[0, "manualNotes"] == "strong read"
