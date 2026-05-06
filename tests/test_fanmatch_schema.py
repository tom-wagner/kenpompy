from kenpompy.FanMatch import FanMatch


class DummyBrowser:
    pass


def test_fanmatch_accepts_time_utc_column(monkeypatch):
    html = b"""
    <html>
      <body>
        <div class="lh12">FanMatch for Saturday, April 4th</div>
        <table>
          <thead>
            <tr>
              <th>Game</th>
              <th>Prediction</th>
              <th>Time(UTC)</th>
              <th>Location</th>
              <th>Thrill Score</th>
              <th>Come back</th>
              <th>Excite ment</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1 Michigan vs.  2 Arizona NCAA</td>
              <td>Michigan 79-78 (51%) [72]</td>
              <td>12:49 am TBS</td>
              <td>Indianapolis, IN Lucas Oil Stadium</td>
              <td>92.81</td>
              <td></td>
              <td></td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
    """

    monkeypatch.setattr("kenpompy.FanMatch.get_html", lambda browser, url: html)

    fm = FanMatch(DummyBrowser(), "2026-04-04")

    assert fm.fm_df is not None
    assert "Time(UTC)" not in fm.fm_df.columns
    assert fm.fm_df.loc[0, "Tournament"] == "NCAA"
    assert fm.fm_df.loc[0, "PredictedWinner"] == "Michigan"
