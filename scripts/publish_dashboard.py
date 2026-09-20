"""Publish the bundle's Lakeview dashboard.

``databricks bundle deploy`` updates the dashboard *draft*; viewers see the last
published revision, so a deploy needs to be followed by a publish. The dashboard
ID is read from ``databricks bundle summary -o json``, so this works for any
target without hardcoding workspace IDs.

Usage:
    python scripts/publish_dashboard.py [-t free]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

RESOURCE_KEY = "energy_quality_dashboard"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-t", "--target", default="free", help="bundle target")
    args = parser.parse_args()

    summary = json.loads(
        subprocess.run(
            ["databricks", "bundle", "summary", "-o", "json", "-t", args.target],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )

    dashboard = summary.get("resources", {}).get("dashboards", {}).get(RESOURCE_KEY)
    if not dashboard:
        print(
            f"dashboard resource {RESOURCE_KEY!r} is not deployed; run "
            f"`databricks bundle deploy -t {args.target}` first",
            file=sys.stderr,
        )
        return 1

    subprocess.run(
        ["databricks", "lakeview", "publish", dashboard["id"]],
        check=True,
    )
    print(f"published dashboard {dashboard['display_name']} ({dashboard['id']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
