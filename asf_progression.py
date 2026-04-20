#!/usr/bin/env -S uv run --script
"""
ASF Committer Progression Data Gatherer

Fetches committer dates, PMC dates, mailing list activity, and GitHub commit
counts for any Apache project. Outputs a unified JSON with phase-transition
timeline per contributor.

Usage:
    ./asf_progression.py iceberg --open
    ./asf_progression.py iceberg --repos apache/iceberg apache/iceberg-python  # override auto-discovery
    ./asf_progression.py iceberg -o data/iceberg_progression.json
    ./asf_progression.py spark --lists dev user --open

Data sources:
  Public (no auth):
    - whimsy.apache.org/public/public_ldap_people.json  (committer account dates)
    - whimsy.apache.org/public/committee-info.json       (PMC roster with dates)
    - projects.apache.org/json/foundation/people.json    (group memberships)
    - lists.apache.org/api/stats.lua                     (mailing list activity)

  Requires GITHUB_TOKEN env var:
    - api.github.com/repos/{owner}/{repo}/contributors   (commit counts per contributor)

  To get a GitHub token:
    1. Go to https://github.com/settings/tokens
    2. Generate a new token (classic) with 'public_repo' scope
    3. export GITHUB_TOKEN=ghp_your_token_here
    (Or install `gh` CLI and run `gh auth login`)
"""
# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx"]
# ///

import argparse
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx


def fetch_json(url: str, client: httpx.Client) -> dict:
    """Fetch and parse JSON from a URL."""
    print(f"  Fetching {url.split('/')[-1]}...", file=sys.stderr)
    resp = client.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def discover_github_repos(project_id: str, token: str, client: httpx.Client) -> list[str]:
    """
    Auto-discover GitHub repos for an Apache project.
    Checks for apache/{project}, then searches for apache/{project}-* repos.
    Returns list of "apache/{name}" strings.
    Borrowed from oss-project-metrics/bin/add_project.py.
    """
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    repos = []

    # Check if main repo exists
    main_repo = f"apache/{project_id}"
    try:
        resp = client.get(f"https://api.github.com/repos/{main_repo}", headers=headers, timeout=10)
        if resp.status_code == 200:
            repos.append(main_repo)
    except Exception:
        pass

    # Search for repos matching {project}-*
    try:
        url = f"https://api.github.com/search/repositories?q=org:apache+{project_id}-+in:name"
        resp = client.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for repo in data.get("items", []):
                name = repo["name"]
                full = f"apache/{name}"
                if name.startswith(f"{project_id}-") and full not in repos:
                    repos.append(full)
    except Exception:
        pass

    if len(repos) > 10:
        print(f"  Found {len(repos)} repos — that's a lot. Using top 10 by stars.", file=sys.stderr)
        # Could sort by stars if needed, but for now just cap
        repos = repos[:10]

    return repos


def get_github_token() -> str | None:
    """Get GitHub token from env or gh CLI. Returns None if unavailable."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def get_github_contributors(repos: list[str], token: str, client: httpx.Client) -> dict:
    """
    Fetch contributor commit counts from GitHub for multiple repos.
    Returns: {login: {total, repos: {repo: count}}}
    """
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    all_contribs = {}

    for repo in repos:
        print(f"  Fetching {repo} contributors...", file=sys.stderr)
        page = 1
        repo_total = 0
        while True:
            for attempt in range(3):
                resp = client.get(
                    f"https://api.github.com/repos/{repo}/contributors",
                    params={"per_page": 100, "page": page},
                    headers=headers,
                    timeout=30,
                )
                if resp.status_code == 200:
                    break
                if resp.status_code == 403:
                    reset = int(resp.headers.get("x-ratelimit-reset", 0))
                    wait = max(reset - int(time.time()), 1)
                    print(f"    Rate limited, waiting {min(wait, 60)}s...", file=sys.stderr)
                    time.sleep(min(wait, 60))
                else:
                    print(f"    HTTP {resp.status_code} for {repo} page {page}", file=sys.stderr)
                    break
            else:
                break

            if resp.status_code != 200:
                break

            data = resp.json()
            if not isinstance(data, list) or len(data) == 0:
                break

            for c in data:
                if c.get("type") == "Bot":
                    continue
                login = c["login"]
                count = c["contributions"]
                if login not in all_contribs:
                    all_contribs[login] = {"total": 0, "repos": {}}
                all_contribs[login]["repos"][repo] = count
                all_contribs[login]["total"] += count
                repo_total += count

            if len(data) < 100:
                break
            page += 1

        print(f"    {repo}: {repo_total} total commits", file=sys.stderr)

    print(f"  Total unique GitHub contributors: {len(all_contribs)}", file=sys.stderr)
    return all_contribs


def get_committer_dates(project_id: str, people_data: dict, ldap_data: dict) -> dict:
    """
    Get ASF account creation dates for project committers.

    Note: For people who had ASF accounts before joining this project,
    the date reflects their original ASF onboarding, not their project join.
    """
    committer_dates = {}
    project_members = set()

    for person_id, person_info in people_data.items():
        groups = person_info.get("groups", [])
        if project_id in groups:
            project_members.add(person_id)

    for pid in project_members:
        if pid not in ldap_data.get("people", {}):
            continue
        person = ldap_data["people"][pid]
        ts = person.get("createTimestamp", "")
        if ts:
            date_str = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
        else:
            date_str = None

        committer_dates[pid] = {
            "name": person.get("name", pid),
            "asf_created": date_str,
        }

    return committer_dates


def get_pmc_dates(project_id: str, committee_data: dict) -> dict:
    """Get PMC roster with join dates from committee-info.json."""
    pmc_dates = {}
    project_info = committee_data.get("committees", {}).get(project_id, {})

    for member_id, member_info in project_info.get("roster", {}).items():
        pmc_dates[member_id] = {
            "name": member_info.get("name", member_id),
            "pmc_date": member_info.get("date"),
        }

    return pmc_dates


def get_mailing_list_activity(
    domain: str, lists: list[str], since: str, client: httpx.Client
) -> dict:
    """
    Fetch per-person monthly mailing list activity from Ponymail.
    Returns: {key: {names, months: {YYYY-MM: count}, total}}
    """
    activity = defaultdict(lambda: {"names": set(), "months": defaultdict(int), "total": 0})

    since_year, since_month = map(int, since.split("-"))
    now = datetime.now(timezone.utc)
    months = []
    y, m = since_year, since_month
    while (y, m) <= (now.year, now.month):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    for list_name in lists:
        total = len(months)
        est_secs = total // 5 + 1
        print(f"  Fetching {list_name}@{domain} ({total} months, ~{est_secs}s)...", file=sys.stderr)
        for mi, month in enumerate(months):
            if mi > 0 and mi % 5 == 0:
                time.sleep(1)  # rate limit: ~5 req/sec
            if mi % 10 == 0:
                print(f"    {mi}/{total} ...", file=sys.stderr)
            url = f"https://lists.apache.org/api/stats.lua?list={list_name}&domain={domain}&d={month}"
            try:
                resp = client.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
                data = resp.json()

                sender_counts = defaultdict(int)
                sender_names = {}
                emails_list = data.get("emails", [])
                if isinstance(emails_list, list):
                    for email_data in emails_list:
                        if not isinstance(email_data, dict):
                            continue
                        from_field = email_data.get("from", "")
                        name = from_field.split("<")[0].strip().strip('"')
                        sender_counts[from_field] += 1
                        sender_names[from_field] = name

                participants = data.get("participants", [])
                if isinstance(participants, list):
                    for pdata in participants:
                        if not isinstance(pdata, dict):
                            continue
                        email_addr = pdata.get("email", "")
                        name = pdata.get("name", email_addr)
                        count = pdata.get("count", 0)
                        activity[email_addr]["names"].add(name)
                        activity[email_addr]["months"][month] += count
                        activity[email_addr]["total"] += count

                participant_names = set()
                if isinstance(participants, list):
                    for p in participants:
                        if isinstance(p, dict):
                            participant_names.add(p.get("name", ""))

                for from_field, cnt in sender_counts.items():
                    name = sender_names.get(from_field, from_field)
                    if name and name not in participant_names:
                        activity[from_field]["names"].add(name)
                        activity[from_field]["months"][month] += cnt
                        activity[from_field]["total"] += cnt

            except Exception as e:
                print(f"    Warning: {month} failed: {e}", file=sys.stderr)

    for email in activity:
        activity[email]["names"] = list(activity[email]["names"])

    return dict(activity)



def build_html(data: dict) -> str:
    """Generate a standalone HTML visualization with swim lane chart."""
    project = data["project"]
    records = data["records"]

    all_months = set()
    for r in records:
        all_months.update(r.get("monthly_ml_activity", {}).keys())
    all_months = sorted(all_months)

    chart_people = []
    for r in records:
        ml = r.get("monthly_ml_activity", {})
        total = r.get("total_ml_messages", 0)
        gh = r.get("github_commits", 0)
        if total < 3 and not r.get("is_committer") and gh < 5:
            continue

        cumulative = []
        running = 0
        monthly = []
        for m in all_months:
            val = ml.get(m, 0)
            running += val
            cumulative.append(running)
            monthly.append(val)

        first_ml = r.get("first_ml_post")
        first_gh = r.get("first_github_commit")
        candidates = [d for d in [first_ml, first_gh] if d]
        first_activity = min(candidates) if candidates else first_ml

        chart_people.append({
            "name": r["name"],
            "apache_id": r.get("apache_id"),
            "is_committer": r.get("is_committer", False),
            "is_pmc": r.get("is_pmc", False),
            "asf_created": r.get("asf_account_created"),
            "pmc_date": r.get("pmc_date"),
            "total_messages": total,
            "github_commits": gh,
            "first_ml": first_ml,
            "first_activity": first_activity,
            "last_ml": r.get("last_ml_post"),
            "active_months": r.get("active_ml_months", 0),
            "cumulative": cumulative,
            "monthly": monthly,
        })

    chart_people.sort(key=lambda x: -(x["total_messages"] + x["github_commits"]))
    chart_people = chart_people[:100]

    chart_data = json.dumps({
        "project": project,
        "months": all_months,
        "people": chart_people,
        "summary": data.get("summary", {}),
        "generated": data.get("generated_at", ""),
    })

    return """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>""" + project.capitalize() + """ Contributor Progression</title>
<script src="https://cdn.jsdelivr.net/npm/highcharts@12.1.2/highcharts.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highcharts@12.1.2/modules/xrange.js"></script>
<script src="https://cdn.jsdelivr.net/npm/highcharts@12.1.2/modules/accessibility.js"></script>
<style>
:root{--bg:#f8fafc;--sf:#fff;--bd:#e2e8f0;--tx:#1e293b;--t2:#64748b;--t3:#94a3b8;--bl:#2563eb;--gn:#16a34a;--gy:#94a3b8;--fn:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;--mn:ui-monospace,"Cascadia Mono",Menlo,monospace;--rd:8px}
@media(prefers-color-scheme:dark){:root{--bg:#0f172a;--sf:#1e293b;--bd:#334155;--tx:#f1f5f9;--t2:#94a3b8;--t3:#64748b}}
body{background:var(--bg);margin:0;padding:16px;font-family:var(--fn);color:var(--tx)}
h1{font-size:22px;font-weight:700;margin:0 0 4px}
.sub{font-size:13px;color:var(--t3);margin-bottom:20px}
.card{background:var(--sf);border:1px solid var(--bd);border-radius:var(--rd);padding:16px;margin-bottom:16px}
.card h2{font-size:15px;font-weight:600;margin:0 0 4px}
.card p{font-size:12px;color:var(--t3);margin:0 0 12px;line-height:1.5}
.stats{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.stat{background:var(--sf);border:1px solid var(--bd);border-radius:var(--rd);padding:14px 18px;flex:1;min-width:110px}
.stat .lb{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--t3)}
.stat .vl{font-size:24px;font-weight:700;font-family:var(--mn)}
.stat .dt{font-size:11px;color:var(--t2);margin-top:2px}
.leg{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px}
.leg span{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--t2)}
.leg i{display:inline-block;width:14px;height:14px;border-radius:3px}
.flt{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.flt button{padding:5px 12px;border-radius:6px;border:1px solid var(--bd);background:var(--sf);color:var(--t2);font-size:12px;cursor:pointer;font-family:var(--fn)}
.flt button:hover{background:var(--bd)}
.flt button.on{background:var(--bl);color:#fff;border-color:var(--bl)}
</style>
</head>
<body>
<h1>Apache """ + project.capitalize() + """ — Contributor Progression</h1>
<p class="sub">Committer dates from ASF LDAP, PMC dates from committee-info.json, mailing list from Ponymail, GitHub commits from GitHub API.</p>
<div class="stats" id="stats"></div>
<div class="card">
<h2>Swim Lanes — Phase Timeline</h2>
<p>Each row is a person. Grey = contributor, green = committer, blue = PMC. Bar opacity = mailing list activity intensity. GitHub commits shown in labels. Hover for details.</p>
<div class="leg">
<span><i style="background:#94a3b8"></i> Contributor</span>
<span><i style="background:#16a34a"></i> Committer</span>
<span><i style="background:#2563eb"></i> PMC</span>
</div>
<div class="flt" id="sf"></div>
<div id="swim" style="height:800px"></div>
</div>
<script>
try {
var D = """ + chart_data + """;
var M = D.months;
var now = Date.UTC(2026,3,20);
function mTS(m){if(!m)return null;var p=m.split('-');return Date.UTC(+p[0],+p[1]-1,1)}

var sm = D.summary;
var ghTotal = 0;
D.people.forEach(function(p){ ghTotal += (p.github_commits||0); });
document.getElementById('stats').innerHTML =
  '<div class="stat"><div class="lb">Committers</div><div class="vl">'+sm.total_committers+'</div></div>'+
  '<div class="stat"><div class="lb">PMC Members</div><div class="vl">'+sm.total_pmc+'</div></div>'+
  '<div class="stat"><div class="lb">ML Senders</div><div class="vl">'+sm.total_ml_senders+'</div></div>'+
  '<div class="stat"><div class="lb">GH Commits</div><div class="vl">'+ghTotal.toLocaleString()+'</div><div class="dt">tracked contributors</div></div>';

function avgAct(p,s,e){
  var si=M.indexOf(s),ei=e?M.indexOf(e):M.length-1;
  if(si<0)return 0;var t=0,n=0;
  for(var i=si;i<=Math.min(ei,M.length-1);i++){t+=(p.monthly[i]||0);n++}
  return n>0?t/n:0;
}

var withRole=D.people.filter(function(p){return p.is_committer||p.is_pmc});
var activeC=D.people.filter(function(p){return !p.is_committer&&!p.is_pmc&&p.last_ml&&p.last_ml>='2025-01'&&(p.total_messages>=10||p.github_commits>=10)});
withRole.sort(function(a,b){
  if(a.is_pmc&&!b.is_pmc)return -1;if(!a.is_pmc&&b.is_pmc)return 1;
  if(a.pmc_date&&b.pmc_date)return a.pmc_date.localeCompare(b.pmc_date);
  return(a.asf_created||'z').localeCompare(b.asf_created||'z');
});
activeC.sort(function(a,b){return(b.total_messages+b.github_commits)-(a.total_messages+a.github_commits)});
var swimAll=withRole.concat(activeC.slice(0,15));

function makeLabel(p){
  var parts=[p.name];var m=[];
  if(p.total_messages)m.push(p.total_messages+' ML');
  if(p.github_commits)m.push(p.github_commits+' GH');
  if(m.length)parts.push('('+m.join(', ')+')');
  return parts.join(' ');
}

function renderSwim(f){
  var btns=document.querySelectorAll('#sf button');
  for(var i=0;i<btns.length;i++)btns[i].className=btns[i].getAttribute('data-f')===f?'on':'';
  var list=swimAll;
  if(f==='pmc')list=swimAll.filter(function(p){return p.is_pmc});
  if(f==='comm')list=swimAll.filter(function(p){return p.is_committer&&!p.is_pmc});
  if(f==='rising')list=swimAll.filter(function(p){return !p.is_committer&&!p.is_pmc});
  var cats=list.map(makeLabel);
  var sd=[];
  list.forEach(function(p,yi){
    var firstTS=mTS(p.first_activity||p.first_ml||p.asf_created||M[0]);
    var commTS=p.asf_created?mTS(p.asf_created.slice(0,7)):null;
    var pmcTS=p.pmc_date?mTS(p.pmc_date.slice(0,7)):null;
    var ghFactor=p.github_commits?Math.min(p.github_commits/200,0.3):0;
    if((p.first_activity||p.first_ml)&&(!commTS||firstTS<commTS)){
      var a=avgAct(p,p.first_activity||p.first_ml,p.asf_created?p.asf_created.slice(0,7):M[M.length-1]);
      var op=Math.min(.25+a/15+ghFactor,1);
      sd.push({x:firstTS,x2:commTS||now,y:yi,color:'rgba(148,163,184,'+op+')',phase:'Contributor',name:p.name,avg:a.toFixed(1),gh:p.github_commits||0,firstGH:p.first_github_commit||null,firstML:p.first_ml||null});
    }
    if(commTS){
      var a2=avgAct(p,p.asf_created.slice(0,7),p.pmc_date?p.pmc_date.slice(0,7):M[M.length-1]);
      var op2=Math.min(.3+a2/12+ghFactor,1);
      sd.push({x:commTS,x2:pmcTS||now,y:yi,color:'rgba(22,163,74,'+op2+')',phase:'Committer',name:p.name,avg:a2.toFixed(1),gh:p.github_commits||0,firstGH:p.first_github_commit||null,firstML:p.first_ml||null});
    }
    if(pmcTS){
      var a3=avgAct(p,p.pmc_date.slice(0,7),M[M.length-1]);
      var op3=Math.min(.3+a3/10+ghFactor,1);
      sd.push({x:pmcTS,x2:now,y:yi,color:'rgba(37,99,235,'+op3+')',phase:'PMC',name:p.name,avg:a3.toFixed(1),gh:p.github_commits||0,firstGH:p.first_github_commit||null,firstML:p.first_ml||null});
    }
  });
  document.getElementById('swim').style.height=Math.max(400,list.length*24+100)+'px';
  Highcharts.chart('swim',{
    chart:{type:'xrange',backgroundColor:'transparent'},title:{text:null},
    xAxis:{type:'datetime'},
    yAxis:{categories:cats,reversed:true,labels:{style:{fontSize:'11px'}},title:null},
    tooltip:{useHTML:true,formatter:function(){
      var pt=this.point;var dur=Math.round((pt.x2-pt.x)/(30.44*86400000));
      return '<b>'+pt.name+'</b> — '+pt.phase+'<br>Duration: '+dur+' months<br>Avg ML: '+pt.avg+' msgs/month<br>GitHub commits: '+pt.gh+(pt.firstGH?'<br>First GH commit: '+pt.firstGH:'')+(pt.firstML?'<br>First ML post: '+pt.firstML:'');
    }},
    series:[{name:'Phases',data:sd,borderRadius:3,pointWidth:18}],
    legend:{enabled:false},credits:{enabled:false}
  });
}
document.getElementById('sf').innerHTML=
  '<button data-f="all" class="on" onclick="renderSwim(&apos;all&apos;)">All ('+swimAll.length+')</button>'+
  '<button data-f="pmc" onclick="renderSwim(&apos;pmc&apos;)">PMC</button>'+
  '<button data-f="comm" onclick="renderSwim(&apos;comm&apos;)">Committers</button>'+
  '<button data-f="rising" onclick="renderSwim(&apos;rising&apos;)">Rising</button>';
renderSwim('all');
} catch(e) {
  document.body.innerHTML += "<pre style='color:red;padding:16px;white-space:pre-wrap'>"+e.message+"\\n"+e.stack+"</pre>";
}
</script>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(
        description="Fetch ASF committer progression data for an Apache project"
    )
    parser.add_argument("project", help="Apache project ID (e.g. 'iceberg', 'spark')")
    parser.add_argument(
        "--repos",
        nargs="+",
        default=None,
        help="GitHub repos to include (e.g. apache/iceberg apache/iceberg-python). Auto-discovered if omitted.",
    )
    parser.add_argument(
        "--lists",
        nargs="+",
        default=["dev"],
        help="Mailing list prefixes to query (default: dev)",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Earliest month for ML data (default: auto-detect from project establishment date)",
    )
    parser.add_argument(
        "--no-github",
        action="store_true",
        help="Skip GitHub data (useful if you don't have a token)",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output JSON file (default: {project}_progression.json)",
    )
    parser.add_argument(
        "--html",
        default=None,
        help="Also generate an HTML visualization (default: viz/{project}.html). "
             "Use --no-html to skip.",
    )
    parser.add_argument(
        "--no-html",
        action="store_true",
        help="Skip HTML visualization generation",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the HTML visualization in the default browser",
    )
    args = parser.parse_args()

    output_file = args.output or f"data/{args.project}_progression.json"
    domain = f"{args.project}.apache.org"
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)

    client = httpx.Client(follow_redirects=True)

    print(f"Gathering data for Apache {args.project.capitalize()}", file=sys.stderr)

    # 1. ASF metadata
    print("\nStep 1: ASF metadata", file=sys.stderr)
    people_data = fetch_json("https://projects.apache.org/json/foundation/people.json", client)
    ldap_data = fetch_json("https://whimsy.apache.org/public/public_ldap_people.json", client)
    committee_data = fetch_json("https://whimsy.apache.org/public/committee-info.json", client)

    # 2. Project committers & PMC
    print(f"\nStep 2: {args.project} committers & PMC", file=sys.stderr)
    committers = get_committer_dates(args.project, people_data, ldap_data)
    pmc_members = get_pmc_dates(args.project, committee_data)
    print(f"  Committers: {len(committers)}", file=sys.stderr)
    print(f"  PMC members: {len(pmc_members)}", file=sys.stderr)

    # Auto-detect project start date from committee established field
    project_committee = committee_data.get("committees", {}).get(args.project, {})
    established = project_committee.get("established")
    if args.since:
        since = args.since
    elif established:
        # Handle multiple date formats: "2020-05", "05/2020", "01/2007", etc.
        if "/" in established:
            # Format: MM/YYYY
            parts = established.split("/")
            if len(parts) == 2:
                since = f"{parts[1]}-{parts[0].zfill(2)}"
            else:
                since = "2018-01"
        elif "-" in established:
            since = established[:7]
        else:
            since = "2018-01"
        print(f"  Project established: {established} (using as start date)", file=sys.stderr)
    else:
        since = "2018-01"
        print(f"  Could not detect establishment date, using {since}", file=sys.stderr)

    # 3. GitHub
    gh_contribs = {}
    if not args.no_github:
        gh_token = get_github_token()
        if gh_token:
            # Auto-discover repos if not specified
            if args.repos:
                github_repos = args.repos
            else:
                print(f"\nStep 3a: Discovering GitHub repos for {args.project}...", file=sys.stderr)
                github_repos = discover_github_repos(args.project, gh_token, client)
                if github_repos:
                    print(f"  Found: {', '.join(github_repos)}", file=sys.stderr)
                else:
                    github_repos = [f"apache/{args.project}"]
                    print(f"  Discovery found nothing, falling back to {github_repos[0]}", file=sys.stderr)

            print(f"\nStep 3b: Fetching GitHub contributors ({', '.join(github_repos)})...", file=sys.stderr)
            gh_contribs = get_github_contributors(github_repos, gh_token, client)
        else:
            print("  Skipping: no GITHUB_TOKEN found. Set it or use --no-github.", file=sys.stderr)
            print("  To get a token: https://github.com/settings/tokens", file=sys.stderr)
    else:
        print("\nStep 3: GitHub (skipped via --no-github)", file=sys.stderr)
        github_repos = []

    # 4. Mailing list
    print(f"\nStep 4: Mailing list ({', '.join(args.lists)}@{domain})", file=sys.stderr)
    ml_activity = get_mailing_list_activity(domain, args.lists, since, client)
    print(f"  Unique senders: {len(ml_activity)}", file=sys.stderr)

    # 5. Merge
    print(f"\nStep 5: Merging...", file=sys.stderr)
    records = {}

    for cid, cdata in committers.items():
        records[cid] = {
            "apache_id": cid,
            "name": cdata["name"],
            "asf_account_created": cdata["asf_created"],
            "is_committer": True,
            "is_pmc": cid in pmc_members,
            "pmc_date": pmc_members.get(cid, {}).get("pmc_date"),
            "mailing_list_emails": [],
            "total_ml_messages": 0,
            "monthly_ml_activity": {},
            "github_commits": 0,
            "github_repos": {},
        }

    for pid, pdata in pmc_members.items():
        if pid not in records:
            records[pid] = {
                "apache_id": pid,
                "name": pdata["name"],
                "asf_account_created": None,
                "is_committer": True,
                "is_pmc": True,
                "pmc_date": pdata.get("pmc_date"),
                "mailing_list_emails": [],
                "total_ml_messages": 0,
                "monthly_ml_activity": {},
                "github_commits": 0,
                "github_repos": {},
            }

    # Match GitHub contributors to ASF records by name
    gh_matched = set()
    for rid, rdata in records.items():
        name_lower = rdata["name"].lower()
        for login, ghdata in gh_contribs.items():
            # Simple heuristic: if the login contains parts of the name
            if login.lower() == rid or login.lower().replace("-", "") in name_lower.replace(" ", ""):
                rdata["github_commits"] = ghdata["total"]
                rdata["github_repos"] = ghdata["repos"]
                gh_matched.add(login)
                break

    # ML matching
    name_to_id = {}
    first_name_counts = defaultdict(list)  # first_name -> [rid, ...]
    for rid, rdata in records.items():
        name_lower = rdata["name"].lower()
        name_to_id[name_lower] = rid
        # Also index by "first last" (skipping middle names)
        parts = name_lower.split()
        if len(parts) >= 2:
            name_to_id[f"{parts[0]} {parts[-1]}"] = rid
        # Track first names for single-match fallback
        if parts:
            first_name_counts[parts[0]].append(rid)

    # For first names that map to exactly ONE committer/PMC, allow first-name matching.
    # This handles "Drew" -> "Drew Gallardo", "Péter" -> "Péter Váry", etc.
    # Skip very common first names to avoid false positives.
    first_name_to_id = {}
    for fname, rids in first_name_counts.items():
        if len(rids) == 1 and len(fname) >= 3:
            first_name_to_id[fname] = rids[0]

    def match_name(name_lower):
        """Try exact, first+last, then unique first-name match."""
        if name_lower in name_to_id:
            return name_to_id[name_lower]
        # Try first-name-only match (e.g. "Drew" -> "Drew Gallardo")
        parts = name_lower.split()
        if parts and parts[0] in first_name_to_id:
            return first_name_to_id[parts[0]]
        return None

    ml_unmatched = {}
    for email, mldata in ml_activity.items():
        matched = False
        for name in mldata["names"]:
            name_lower = name.lower()
            rid = match_name(name_lower)
            if rid:
                records[rid]["mailing_list_emails"].append(email)
                records[rid]["total_ml_messages"] += mldata["total"]
                for month, count in mldata["months"].items():
                    records[rid]["monthly_ml_activity"][month] = (
                        records[rid]["monthly_ml_activity"].get(month, 0) + count
                    )
                matched = True
                break
        if not matched:
            ml_unmatched[email] = mldata

    # Add unmatched ML senders
    for email, mldata in ml_unmatched.items():
        if mldata["total"] < 3:
            continue
        best_name = mldata["names"][0] if mldata["names"] else email

        # Check if this ML sender has GitHub activity
        gh_commits = 0
        gh_repos = {}
        for login, ghdata in gh_contribs.items():
            if login not in gh_matched and login.lower() in best_name.lower().replace(" ", ""):
                gh_commits = ghdata["total"]
                gh_repos = ghdata["repos"]
                gh_matched.add(login)
                break

        records[f"ml:{email}"] = {
            "apache_id": None,
            "name": best_name,
            "asf_account_created": None,
            "is_committer": False,
            "is_pmc": False,
            "pmc_date": None,
            "mailing_list_emails": [email],
            "total_ml_messages": mldata["total"],
            "monthly_ml_activity": dict(mldata["months"]),
            "github_commits": gh_commits,
            "github_repos": gh_repos,
        }

    # Add GitHub-only contributors (not on ML, not committers)
    for login, ghdata in gh_contribs.items():
        if login in gh_matched:
            continue
        if ghdata["total"] < 5:
            continue
        records[f"gh:{login}"] = {
            "apache_id": None,
            "name": login,
            "asf_account_created": None,
            "is_committer": False,
            "is_pmc": False,
            "pmc_date": None,
            "mailing_list_emails": [],
            "total_ml_messages": 0,
            "monthly_ml_activity": {},
            "github_commits": ghdata["total"],
            "github_repos": ghdata["repos"],
        }

    # Derived fields
    for rid, rdata in records.items():
        months = rdata["monthly_ml_activity"]
        if months:
            sorted_months = sorted(months.keys())
            rdata["first_ml_post"] = sorted_months[0]
            rdata["last_ml_post"] = sorted_months[-1]
            rdata["active_ml_months"] = len(sorted_months)
        else:
            rdata["first_ml_post"] = None
            rdata["last_ml_post"] = None
            rdata["active_ml_months"] = 0

    # Output
    output = {
        "project": args.project,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "ldap": "whimsy.apache.org/public/public_ldap_people.json",
            "committee": "whimsy.apache.org/public/committee-info.json",
            "people": "projects.apache.org/json/foundation/people.json",
            "mailing_lists": [f"{l}@{domain}" for l in args.lists],
            "github_repos": github_repos if not args.no_github else [],
        },
        "summary": {
            "total_committers": sum(1 for r in records.values() if r["is_committer"]),
            "total_pmc": sum(1 for r in records.values() if r["is_pmc"]),
            "total_ml_senders": sum(1 for r in records.values() if r["total_ml_messages"] > 0),
            "total_gh_contributors": sum(1 for r in records.values() if r.get("github_commits", 0) > 0),
            "total_records": len(records),
        },
        "records": list(sorted(
            records.values(),
            key=lambda x: -(x["total_ml_messages"] + x.get("github_commits", 0)),
        )),
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    s = output["summary"]
    print(f"\nOutput: {output_file}", file=sys.stderr)
    print(f"  {s['total_committers']} committers, {s['total_pmc']} PMC", file=sys.stderr)
    print(f"  {s['total_ml_senders']} ML senders, {s['total_gh_contributors']} GH contributors", file=sys.stderr)
    print(f"  {s['total_records']} total records", file=sys.stderr)


    # 6. Generate HTML visualization
    if not args.no_html:
        html_file = args.html or f"viz/{args.project}.html"
        os.makedirs(os.path.dirname(html_file) or ".", exist_ok=True)
        html = build_html(output)
        with open(html_file, "w") as f:
            f.write(html)
        print(f"  HTML: {html_file}", file=sys.stderr)

        if args.open:
            import subprocess as _sp
            _sp.run(["open", html_file])

    client.close()


if __name__ == "__main__":
    main()
