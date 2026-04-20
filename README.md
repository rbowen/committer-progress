# Committer Progression

Visualize the contributor → committer → PMC journey for Apache projects.
Uses only public data — ASF LDAP, Whimsy, Ponymail, and GitHub APIs.

## Quick Start

```bash
export GITHUB_TOKEN=ghp_your_token_here  # see below
./asf_progression.py iceberg --open
```

One command. Auto-discovers GitHub repos, auto-detects the project start
date, fetches all data, generates the HTML dashboard, and opens it.

## GitHub Token

`$GITHUB_TOKEN` is needed for commit data. Without it, GitHub data is
skipped (everything else still works).

To get one:

1. https://github.com/settings/tokens → **Generate new token (classic)**
2. Select `public_repo` scope
3. Add to `~/.zshrc`: `export GITHUB_TOKEN=ghp_...`

If you have `gh` CLI installed and authed, the script detects it automatically.

## Usage

```bash
# Iceberg (auto-discovers repos, auto-detects start date):
./asf_progression.py iceberg --open

# Spark:
./asf_progression.py spark --open

# Override start date if needed:
./asf_progression.py kafka --since 2021-01 --open

# Override repo discovery:
./asf_progression.py iceberg --repos apache/iceberg apache/iceberg-python --open

# Skip GitHub:
./asf_progression.py kafka --no-github --open

# JSON only, no HTML:
./asf_progression.py iceberg --no-html -o data/iceberg.json

# See all options:
./asf_progression.py --help
```

## Data Sources

| Source | Auth | Data |
|--------|------|------|
| Whimsy LDAP | None | Committer account creation dates |
| committee-info.json | None | PMC roster with join dates; project establishment date |
| projects.apache.org | None | Group memberships |
| Ponymail stats.lua | None | Monthly ML message counts per sender |
| GitHub API | `$GITHUB_TOKEN` | Commit counts per contributor; repo auto-discovery |

**Note**: The LDAP `createTimestamp` is when the ASF account was created.
For people who were committers on other Apache projects first, this
predates their involvement in your project.

## Output

```
{project}_progression.json   # Per-person data (dates, activity, roles)
viz/{project}.html            # Standalone HTML dashboard (open in any browser)
```

## Project Structure

```
committer-progress/
├── README.md
├── asf_progression.py         # The script. Does everything.
├── data/                       # Cached data files
└── viz/                        # Generated HTML dashboards
```
