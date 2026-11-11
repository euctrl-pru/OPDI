"""Draw the arrivals ``point_merge_report.py --plot`` exported.

    python benchmarks/plot_point_merge.py --plots-dir <paper>/data/point_merge/plots --apt EIDW

Needs matplotlib (not a project dependency): run it from a scratch environment.
One PNG per track: the track in blue, the merge points as red crosses, the
leg entry/exit as green dots, the merge-point overflight as an orange dot.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def main(argv=None) -> int:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--plots-dir", required=True)
    ap.add_argument("--apt", required=True)
    args = ap.parse_args(argv)
    d = Path(args.plots_dir)
    tracks = pd.read_csv(d / f"tracks_{args.apt}.csv", parse_dates=["event_time"])
    events = pd.read_csv(d / f"events_{args.apt}.csv")
    mps = pd.read_csv(d / f"merge_points_{args.apt}.csv")
    lat0, lon0 = mps["lat"].mean(), mps["lon"].mean()
    for (tid, kind), t in tracks.groupby(["track_id", "kind"]):
        t = t.sort_values("event_time")
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.plot(t["lon"], t["lat"], "-", lw=0.8, color="tab:blue")
        ax.scatter(mps["lon"], mps["lat"], marker="x", c="red", zorder=3)
        for _, r in mps.iterrows():
            ax.annotate(
                f"{r['merge_point']} ({r['legs_mp_min_nm']:.0f}-{r['legs_mp_max_nm']:.0f} NM)",
                (r["lon"], r["lat"]),
                fontsize=7,
            )
        ev = events[events["flight_id"] == tid]
        for _, e in ev.iterrows():
            c = "orange" if e["type"] == "pms-merge-point" else "green"
            ax.scatter([e["longitude"]], [e["latitude"]], c=c, s=30, zorder=4)
            if e["type"] == "pms-leg-exit":
                info = json.loads(e["info"])
                ax.set_xlabel(
                    f"leg {info['leg_distance_nm']} NM, {info['leg_level_ft']:.0f} ft, {info['duration_s']:.0f} s, {info['direction']}"  # noqa: E501
                )
        ax.set_xlim(lon0 - 1.6, lon0 + 1.6)
        ax.set_ylim(lat0 - 1.0, lat0 + 1.0)
        ax.set_aspect(1 / 0.6)
        ax.set_title(f"{args.apt} {kind} {tid}", fontsize=8)
        fig.savefig(d / f"{args.apt}_{kind}_{tid}.png", dpi=80)
        plt.close(fig)
    print(f"drew {tracks['track_id'].nunique()} tracks into {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
