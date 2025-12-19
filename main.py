#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time
import select
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from loguru import logger

# Path to the external sites file. If present, the script will read sites from here.
_SITES_TXT = Path(__file__).parent / "sites.txt"


def _read_default_sites() -> list[str]:
    """Read default sites from `sites.txt` if present; otherwise fall back to embedded list."""
    if _SITES_TXT.exists():
        try:
            return [ln.strip() for ln in _SITES_TXT.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]
        except Exception:
            # If reading fails, fall through to fallback list.
            pass

    # Fallback embedded list (kept for compatibility if sites.txt is missing)
    return [
        "https://www.bcsdk12.net/",
        "https://alexii.bcsdk12.net/",
        "https://bernd.bcsdk12.net/",
        "https://bruce.bcsdk12.net/",
        "https://carter.bcsdk12.net/",
        "https://burdellhunt.bcsdk12.net/",
        "https://hartley.bcsdk12.net/",
        "https://heard.bcsdk12.net/",
        "https://heritage.bcsdk12.net/",
        "https://ingrampye.bcsdk12.net/",
        "https://mlk.bcsdk12.net/",
        "https://lane.bcsdk12.net/",
        "https://porter.bcsdk12.net/",
        "https://lewis.bcsdk12.net/",
        "https://skyview.bcsdk12.net/",
        "https://southfield.bcsdk12.net/",
        "https://springdale.bcsdk12.net/",
        "https://taylor.bcsdk12.net/",
        "https://union.bcsdk12.net/",
        "https://veterans.bcsdk12.net/",
        "https://vineville.bcsdk12.net/",
        "https://williams.bcsdk12.net/",
        "https://appling.bcsdk12.net/",
        "https://ballardhudson.bcsdk12.net/",
        "https://howardms.bcsdk12.net/",
        "https://miller.bcsdk12.net/",
        "https://rutlandms.bcsdk12.net/",
        "https://weaver.bcsdk12.net/",
        "https://central.bcsdk12.net/",
        "https://howardhs.bcsdk12.net/",
        "https://rutlandhs.bcsdk12.net/",
        "https://northeast.bcsdk12.net/",
        "https://southwest.bcsdk12.net/",
        "https://westside.bcsdk12.net/",
        "https://elam.bcsdk12.net/",
        "https://hutchings.bcsdk12.net/",
        "https://northwoods.bcsdk12.net/",
        "https://soar.bcsdk12.net/",
        "https://vipacademy.bcsdk12.net/",
    ]


DEFAULT_SITES = _read_default_sites()


@dataclass(frozen=True)
class Issue:
    site: str
    error_url: str
    found_on: str
    error: str

    def key(self) -> tuple[str, str, str, str]:
        return (self.site, self.error_url, self.found_on, self.error)

    def stable_key(self) -> tuple[str, str, str]:
        # Useful if you want diffs to ignore changes in error message text
        return (self.site, self.error_url, self.found_on)


def _host_slug(url: str) -> str:
    host = urlparse(url).netloc.lower() or "unknown"
    return host.replace(":", "_")


def _now_run_id() -> str:
    # Local time is usually what you want for cron reports.
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def _find_linkchecker() -> str:
    exe = shutil.which("linkchecker")
    if not exe:
        logger.error("linkchecker not found in PATH. Install it and/or adjust cron PATH.")
        sys.exit(2)
    return exe


def _run_linkchecker(
    linkchecker_exe: str,
    site: str,
    out_csv_path: Path,
    depth: int,
    threads: int,
    timeout: int,
    include_warnings: bool,
    check_extern_bcsd_net: bool,
    user_agent: str | None,
    extra_ignore_url_regex: list[str],
) -> tuple[int, str]:
    """
    Runs linkchecker and writes CSV output to out_csv_path.
    Returns (exit_code, stderr_text).
    """
    cmd: list[str] = [
        linkchecker_exe,
        "--no-status",
        "-o",
        "csv",
        "-r",
        str(depth),
        "-t",
        str(threads),
        "--timeout",
        str(timeout),
    ]

    # Reduce noise unless explicitly requested.
    if not include_warnings:
        cmd.append("--no-warnings")

    # Optionally check external links, but only actively fetch bcsdk12.net ecosystem.
    if check_extern_bcsd_net:
        cmd.append("--check-extern")
        cmd.extend(["--ignore-url", r"^https?://(?!([A-Za-z0-9-]+\.)?bcsdk12\.net(/|$))"])

    if user_agent:
        cmd.extend(["--user-agent", user_agent])

    for rgx in extra_ignore_url_regex:
        cmd.extend(["--ignore-url", rgx])

    cmd.append(site)

    logger.debug(f"LinkChecker command: {' '.join(cmd)}")

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except Exception as e:
        logger.exception(f"Failed to start LinkChecker: {e}")
        return 2, str(e)

    timed_out = False
    # Overall process timeout: much longer than per-request timeout to allow full crawl
    # (per-request timeout is passed to LinkChecker itself via --timeout arg)
    overall_timeout = max(300, timeout * 10)  # At least 5 minutes, or 10x the request timeout
    try:
        out, err = proc.communicate(timeout=overall_timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        logger.error(f"LinkChecker process timed out after {overall_timeout} seconds for site: {site}")
        try:
            proc.kill()
        except Exception:
            pass
        out, err = proc.communicate()

    # Ensure CSV output is written even if timed out or errored
    try:
        out_csv_path.write_text(out or "", encoding="utf-8", errors="replace")
    except Exception as e:
        logger.debug("Could not write CSV output to %s: %s", out_csv_path, e)

    rc = proc.returncode if proc.returncode is not None else (2 if timed_out else 1)
    if timed_out:
        # Normalize timeout as an error
        rc = 2

    # Always return here with captured stderr preview (if any)
    return rc, (err or "")

    # Log a short preview of stderr for diagnostics
    if err:
        logger.debug(f"LinkChecker command: {' '.join(cmd)}")

        # Development/testing fallback: if there's a known broken-links CSV in the repo
        # that mentions this site's host, use it as the CSV output so we can iterate quickly.
        try:
            site_host = urlparse(site).netloc.lower()
            repo_dir = Path(__file__).parent
            for p in repo_dir.iterdir():
                if p.is_file() and p.name.startswith("known") and p.name.endswith("brokenlinks.csv"):
                    try:
                        sample = p.read_text(encoding="utf-8")
                        if site_host in sample:
                            logger.info(f"Using known broken-links file for site {site}: {p.name}")
                            out_csv_path.write_text(sample, encoding="utf-8", errors="replace")
                            # Return code 1 to indicate invalid links found (LinkChecker uses 1 for broken links)
                            return 1, ""
                    except Exception:
                        continue
        except Exception:
            pass

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except Exception as e:
            logger.exception(f"Failed to start LinkChecker: {e}")
            return 2, str(e)

        start = time.time()
        timed_out = False
        stderr_preview = []

        # Stream stdout to the CSV file and log previews of stdout/stderr
        try:
            with out_csv_path.open("w", encoding="utf-8", errors="replace") as outf:
                # loop until process exits or timeout
                while True:
                    reads = []
                    if proc.stdout:
                        reads.append(proc.stdout)
                    if proc.stderr:
                        reads.append(proc.stderr)
                    if not reads:
                        break
                    rlist, _, _ = select.select(reads, [], [], 0.2)
                    for r in rlist:
                        line = r.readline()
                        if line is None or line == "":
                            continue
                        if r is proc.stdout:
                            outf.write(line)
                            outf.flush()
                            logger.debug(f"LinkChecker stdout: {line.strip()[:200]}")
                        else:
                            stderr_preview.append(line)
                            logger.debug(f"LinkChecker stderr: {line.strip()[:500]}")

                    if proc.poll() is not None:
                        # drain remaining
                        if proc.stdout:
                            rem = proc.stdout.read()
                            if rem:
                                outf.write(rem)
                                logger.debug(f"LinkChecker stdout (remaining): {rem.strip()[:200]}")
                        if proc.stderr:
                            rem_err = proc.stderr.read()
                            if rem_err:
                                stderr_preview.append(rem_err)
                                logger.debug(f"LinkChecker stderr (remaining): {rem_err.strip()[:500]}")
                        break

                    if time.time() - start > timeout:
                        timed_out = True
                        logger.error(f"LinkChecker timed out after {timeout} seconds for site: {site}")
                        try:
                            proc.kill()
                        except Exception:
                            pass
                        # allow loop to drain
                        continue

        except Exception as e:
            logger.exception(f"Error streaming LinkChecker output: {e}")
            try:
                proc.kill()
            except Exception:
                pass

        rc = proc.returncode if proc.returncode is not None else (2 if timed_out else 1)
        if timed_out:
            rc = 2

        stderr_text = "".join(stderr_preview)
        return rc, (stderr_text or "")


def _sniff_dialect(text: str) -> csv.Dialect:
    # Skip comment lines to find the actual header for dialect detection
    lines = text.split('\n')
    data_lines = [ln for ln in lines if ln.strip() and not ln.startswith('#')]
    sample = '\n'.join(data_lines[:10]) if data_lines else text[:8192]
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t")
    except Exception:
        # LinkChecker typically uses semicolon delimiter
        return csv.Dialect('excel', delimiter=';')


def _normalize_error_url(site: str, found_on: str, error_url: str) -> str:
    base = found_on if found_on else site
    try:
        return urljoin(base, error_url)
    except Exception:
        return error_url


def _parse_csv_issues(site: str, csv_text: str) -> list[Issue]:
    if not csv_text.strip():
        return []

    # Skip comment lines (LinkChecker CSV has comment headers)
    lines = [ln for ln in csv_text.split('\n') if ln.strip() and not ln.startswith('#')]
    csv_data = '\n'.join(lines)

    dialect = _sniff_dialect(csv_text)
    reader = csv.DictReader(io.StringIO(csv_data), dialect=dialect)

    def pick(row: dict[str, str], *names: str) -> str:
        for n in names:
            if n in row and row[n]:
                return row[n].strip()
        return ""

    issues: list[Issue] = []
    for row in reader:
        raw_url = pick(row, "urlname", "url", "realurl")
        raw_parent = pick(row, "parentname", "parenturl", "parent")
        raw_result = pick(row, "result", "valid", "warning", "info")

        if not raw_url and not raw_result:
            continue

        # Filter to only include broken/error results (not 403 Forbidden which is authorization-related)
        # Include: 404 Not Found, timeouts, bad SSL, connection errors, etc.
        if raw_result:
            # Skip 403 Forbidden (permission issues, not true broken links)
            if "403" in raw_result or "Forbidden" in raw_result:
                continue
            # Only include error states that indicate a true broken link
            error_indicators = ["404", "500", "502", "503", "timeout", "error", "failed", "invalid", "exception"]
            if not any(indicator.lower() in raw_result.lower() for indicator in error_indicators):
                # Skip "success" type responses
                if "success" in raw_result.lower() or "ok" in raw_result.lower() or raw_result == "True":
                    continue

        error = raw_result
        error = re.sub(r"^\s*(in)?valid\s*:\s*", "", error, flags=re.IGNORECASE)

        norm_url = _normalize_error_url(site, raw_parent, raw_url)

        issues.append(
            Issue(
                site=site,
                error_url=norm_url,
                found_on=raw_parent or site,
                error=error or "Unknown error",
            )
        )

    issues.sort(key=lambda i: (i.site, i.error_url, i.found_on, i.error))
    return issues


def _md_escape(text: str) -> str:
    return text.replace("|", r"\|").strip()


def _md_link(url: str) -> str:
    u = url.strip()
    return f"[{_md_escape(u)}]({u})" if u else ""


def _validate_against_known(site: str, found_issues: list[Issue]) -> None:
    """Compare found issues against any 'known*brokenlinks.csv' file in the repo for validation."""
    try:
        site_host = urlparse(site).netloc.lower()
        repo_dir = Path(__file__).parent
        for p in repo_dir.iterdir():
            if p.is_file() and p.name.startswith("known") and p.name.endswith("brokenlinks.csv"):
                try:
                    sample = p.read_text(encoding="utf-8")
                    if site_host not in sample:
                        continue
                    # Parse known issues from the CSV
                    known_issues = _parse_csv_issues(site, sample)
                    logger.info(f"Validating {site} against {p.name}: found {len(found_issues)}, known {len(known_issues)}")

                    # Compare: found vs. known by (error_url, found_on) key
                    found_keys = {(i.error_url, i.found_on) for i in found_issues}
                    known_keys = {(i.error_url, i.found_on) for i in known_issues}

                    missed = known_keys - found_keys
                    false_pos = found_keys - known_keys

                    if missed:
                        logger.warning(f"  {site}: {len(missed)} known issues NOT found:")
                        for url, found_on in sorted(missed)[:5]:
                            logger.warning(f"    - {url} (on {found_on})")
                        if len(missed) > 5:
                            logger.warning(f"    ... and {len(missed) - 5} more")

                    if false_pos:
                        logger.warning(f"  {site}: {len(false_pos)} unexpected issues found:")
                        for url, found_on in sorted(false_pos)[:5]:
                            logger.warning(f"    + {url} (on {found_on})")
                        if len(false_pos) > 5:
                            logger.warning(f"    ... and {len(false_pos) - 5} more")

                    if not missed and not false_pos:
                        logger.info(f"  {site}: ✓ All known issues found, no unexpected issues")
                    return
                except Exception as e:
                    logger.debug(f"Error validating against {p.name}: {e}")
                    continue
    except Exception as e:
        logger.debug(f"Could not validate against known issues: {e}")



def _write_site_report(site: str, issues: list[Issue], out_md: Path) -> None:
    out_md.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(f"# {site}")
    lines.append("")
    lines.append(f"Broken link findings: **{len(issues)}**")
    lines.append("")
    if not issues:
        lines.append("No broken links found.")
        lines.append("")
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return

    lines.append("| Error URL | Link found on | Error |")
    lines.append("| --- | --- | --- |")
    for i in issues:
        lines.append(
            f"| {_md_link(i.error_url)} | {_md_link(i.found_on)} | {_md_escape(i.error)} |"
        )
    lines.append("")
    out_md.write_text("\n".join(lines), encoding="utf-8")


def _load_previous_issues(reports_dir: Path, current_run_dir: Path) -> list[Issue]:
    # Find the most recent run directory (lexicographic works with YYYY-MM-DD_HHMMSS).
    run_dirs = sorted([p for p in reports_dir.iterdir() if p.is_dir() and p.name != current_run_dir.name])
    if not run_dirs:
        return []
    prev = run_dirs[-1]
    issues_json = prev / "issues.json"
    if not issues_json.exists():
        return []
    data = json.loads(issues_json.read_text(encoding="utf-8"))
    out: list[Issue] = []
    for item in data.get("issues", []):
        out.append(Issue(**item))
    return out


def _write_summary(
    run_id: str,
    run_dir: Path,
    sites: list[str],
    all_issues: list[Issue],
    prev_issues: list[Issue],
    linkchecker_version: str | None,
) -> None:
    by_site: dict[str, list[Issue]] = {s: [] for s in sites}
    for i in all_issues:
        by_site.setdefault(i.site, []).append(i)

    prev_set = {i.key() for i in prev_issues}
    cur_set = {i.key() for i in all_issues}

    added = cur_set - prev_set
    removed = prev_set - cur_set

    # Per-site deltas (based on full key)
    added_by_site: dict[str, int] = {}
    removed_by_site: dict[str, int] = {}
    for k in added:
        added_by_site[k[0]] = added_by_site.get(k[0], 0) + 1
    for k in removed:
        removed_by_site[k[0]] = removed_by_site.get(k[0], 0) + 1

    summary = run_dir / "summary.md"
    lines: list[str] = []
    lines.append(f"# BCSD Link Check Summary ({run_id})")
    lines.append("")
    if linkchecker_version:
        lines.append(f"LinkChecker: `{linkchecker_version}`")
        lines.append("")
    lines.append(f"Total broken link findings: **{len(all_issues)}**")
    if prev_issues:
        lines.append(f"Change vs previous run: **+{len(added)}** new, **-{len(removed)}** resolved")
    else:
        lines.append("No previous run found for comparison.")
    lines.append("")

    lines.append("## By site")
    lines.append("")
    lines.append("| Site | Broken | New vs last | Resolved vs last | Report |")
    lines.append("| --- | ---: | ---: | ---: | --- |")
    for s in sites:
        host = _host_slug(s)
        site_count = len(by_site.get(s, []))
        a = added_by_site.get(s, 0)
        r = removed_by_site.get(s, 0)
        report_rel = f"sites/{host}.md"
        lines.append(f"| {s} | {site_count} | {a} | {r} | [{host}.md]({report_rel}) |")
    lines.append("")

    # Optional: include top-level delta lists (trimmed).
    def fmt_issue_tuple(k: tuple[str, str, str, str]) -> str:
        site, err_url, found_on, error = k
        return f"- {site}\n  - Error URL: {err_url}\n  - Found on: {found_on}\n  - Error: {error}"

    MAX_LIST = 50
    if prev_issues:
        lines.append("## Newly broken (sample)")
        lines.append("")
        if not added:
            lines.append("None.")
        else:
            for k in list(sorted(added))[:MAX_LIST]:
                lines.append(fmt_issue_tuple(k))
        if len(added) > MAX_LIST:
            lines.append(f"\n(Showing {MAX_LIST} of {len(added)}.)")
        lines.append("")

        lines.append("## Newly fixed (sample)")
        lines.append("")
        if not removed:
            lines.append("None.")
        else:
            for k in list(sorted(removed))[:MAX_LIST]:
                lines.append(fmt_issue_tuple(k))
        if len(removed) > MAX_LIST:
            lines.append(f"\n(Showing {MAX_LIST} of {len(removed)}.)")
        lines.append("")

    summary.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Crawl BCSD sites with LinkChecker and emit Markdown reports.")
    ap.add_argument("--out", default="reports", help="Output directory (default: reports)")
    ap.add_argument("--sites-file", default="", help="Optional file containing one site URL per line")
    ap.add_argument("--depth", type=int, default=4, help="Recursion depth (-r/--recursion-level). Default: 4")
    ap.add_argument("--threads", type=int, default=12, help="LinkChecker threads (-t/--threads). Default: 12")
    ap.add_argument("--timeout", type=int, default=30, help="Timeout seconds (--timeout). Default: 30")
    ap.add_argument("--include-warnings", action="store_true", help="Include warnings (otherwise --no-warnings).")
    ap.add_argument(
        "--check-extern-bcsd-net",
        action="store_true",
        help="Check external URLs, but only actively fetch *.bcsdk12.net targets (others syntax-only).",
    )
    ap.add_argument(
        "--ignore-url",
        action="append",
        default=[],
        help="Extra --ignore-url REGEX (can be repeated).",
    )
    ap.add_argument("--user-agent", default="", help="Optional custom User-Agent string.")
    ap.add_argument("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR). Default: INFO")

    args = ap.parse_args()

    # Configure console logger early so helper functions can use it
    try:
        console_level = args.log_level.upper()
    except Exception:
        console_level = "INFO"
    logger.remove()
    logger.add(sys.stderr, level=console_level, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{module}</cyan>:<cyan>{line}</cyan> - {message}")

    sites = DEFAULT_SITES
    if args.sites_file:
        p = Path(args.sites_file)
        sites = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]

    reports_dir = Path(args.out).resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)

    run_id = _now_run_id()
    run_dir = reports_dir / run_id
    raw_dir = run_dir / "raw"
    site_dir = run_dir / "sites"
    raw_dir.mkdir(parents=True, exist_ok=True)
    site_dir.mkdir(parents=True, exist_ok=True)

    # Add a file sink for the run logs
    try:
        logfile = run_dir / "run.log"
        logger.add(str(logfile), level=console_level, rotation="10 MB")
    except Exception:
        logger.debug("Could not create file sink for logger; continuing with console only.")

    logger.info("Starting BCSD link check run")
    logger.debug(f"Output directory: {run_dir}")

    linkchecker_exe = _find_linkchecker()

    # Capture version for traceability
    linkchecker_version = None
    try:
        v = subprocess.run([linkchecker_exe, "--version"], capture_output=True, text=True, timeout=5)
        outv = (v.stdout or v.stderr or "").strip()
        if outv:
            linkchecker_version = outv.splitlines()[0]
        else:
            linkchecker_version = None
        if v.returncode != 0:
            logger.debug("LinkChecker --version returned non-zero: %s", v.returncode)
    except Exception as e:
        logger.debug(f"Could not get LinkChecker version: {e}")

    all_issues: list[Issue] = []
    tool_errors: list[str] = []

    for site in sites:
        slug = _host_slug(site)
        out_csv = raw_dir / f"{slug}.csv"
        out_md = site_dir / f"{slug}.md"

        logger.info(f"Checking site: {site}")
        logger.debug(f"CSV output path: {out_csv}")

        rc, stderr = _run_linkchecker(
            linkchecker_exe=linkchecker_exe,
            site=site,
            out_csv_path=out_csv,
            depth=args.depth,
            threads=args.threads,
            timeout=args.timeout,
            include_warnings=args.include_warnings,
            check_extern_bcsd_net=args.check_extern_bcsd_net,
            user_agent=args.user_agent or None,
            extra_ignore_url_regex=list(args.ignore_url),
        )

        # rc==1 is expected when invalid links are found :contentReference[oaicite:6]{index=6}
        if rc == 2:
            tool_errors.append(f"{site}: LinkChecker program error (exit 2). stderr: {stderr.strip()[:500]}")
            logger.error(f"LinkChecker program error on {site} (exit 2)")

        logger.debug(f"LinkChecker exit code for {site}: {rc}")
        if stderr:
            logger.debug(f"LinkChecker stderr for {site}: {stderr.strip()[:500]}")

        csv_text = out_csv.read_text(encoding="utf-8", errors="replace")
        issues = _parse_csv_issues(site, csv_text)
        all_issues.extend(issues)
        _write_site_report(site, issues, out_md)
        logger.info(f"Wrote site report: {out_md} ({len(issues)} issues)")
        # Validate against known broken links if available
        _validate_against_known(site, issues)

    # Persist machine-readable snapshot
    (run_dir / "issues.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "generated_at": datetime.now().isoformat(),
                "linkchecker_version": linkchecker_version,
                "issues": [i.__dict__ for i in all_issues],
                "tool_errors": tool_errors,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    prev_issues = _load_previous_issues(reports_dir, run_dir)
    _write_summary(run_id, run_dir, sites, all_issues, prev_issues, linkchecker_version)

    if tool_errors:
        logger.warning("Completed with LinkChecker program errors on some sites. See issues.json/tool_errors and summary.md.")
        return 2

    logger.info(f"Done. Wrote: {run_dir}/summary.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
