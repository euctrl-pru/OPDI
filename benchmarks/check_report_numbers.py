"""Every number in the report's prose must be computed or declared.

A number a chunk prints came from a staged CSV and moves when the data moves.
A number typed into prose does not: it goes stale silently, and the reader
cannot tell the two apart. v6.2 shipped three such -- an accuracy delta of
0.16 pp against a real 0.15, a "seventy per cent" against a real 70.0%, and a
radius v6 claimed shipped and which never did, for three versions running.

Each was found by tripping over it. This finds them by looking.

    python benchmarks/check_report_numbers.py <paper>/index.qmd \\
        --allowlist <paper>/constants.yml

Exits non-zero, and lists them, if any prose number is neither computed by an
inline ``r`` expression nor declared in the allowlist.
"""

import argparse
import re
import sys
from collections import namedtuple
from pathlib import Path

import yaml

Finding = namedtuple("Finding", "line value text")

#: A fenced R chunk opener, and any bare fence that closes one. Whatever a
#: chunk prints is computed by construction, so its body is out of scope.
CHUNK_OPEN = re.compile(r"^\s*```+\s*\{r[^}]*\}\s*$")
FENCE = re.compile(r"^\s*```+\s*$")

#: Inline R -- the computed form. Cut out of a line before scanning rather than
#: causing the whole line to be skipped: one sentence can carry a computed
#: number and a typed one, and skipping the line would hide the typed one.
INLINE_R = re.compile(r"`r [^`]*`")

#: A numeral with any unit glued to it. The unit is what distinguishes a
#: measurement from a count.
#:
#: A comma must be followed by exactly three digits, so it is a thousands
#: separator rather than the sentence's punctuation. Without that, "in 2025,
#: across 12 cells" parses the year as "2025," -- which is not an integer, so
#: the year exemption never fires and every date in the paper is flagged.
NUM = re.compile(
    r"(?<![\w.])([+-]?\d+(?:,\d{3})*(?:\.\d+)?)\s*(%|pp|ft|NM|km|kt)?")

#: Constructs that merely contain digits and are not measurements: section
#: references, step and node identifiers, version numbers, and anything already
#: inside backticks (a code span is naming a thing, not reporting a value).
IDENT = re.compile(
    r"`[^`]*`"                       # any code span
    r"|@[\w-]+"                      # crossrefs: @sec-datum, @fig-...
    r"|FL\d+"                        # flight levels
    r"|\bS\d+\b|\bE\d+\b"            # diagram nodes
    r"|path\d+"                      # ladder rungs
    r"|step \d+"                     # pipeline steps
    r"|res(?:olution)? \d+"          # H3 resolution
    r"|\bv\d+(?:\.\d+)*\b"           # v6, v6.1
    r"|[Vv]ersion \d+(?:\.\d+)*"     # version 6.2, and sentence-initial Version
    r"|\bk\s*=\s*\d+"                # the exchange rate
)


def _strip_front_matter(lines):
    """Drop the YAML front matter, if the document opens with one.

    The subtitle carries the version number, and front matter is metadata the
    author sets deliberately rather than a claim about the data.

    Only a `---` on the very first line opens front matter. Elsewhere `---` is
    a horizontal rule, and treating one as a fence would silence every number
    after it.
    """
    if not lines or lines[0].strip() != "---":
        return list(enumerate(lines, 1))
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            return list(enumerate(lines, 1))[i + 1:]
    return list(enumerate(lines, 1))     # unterminated: treat it all as prose


def prose_lines(text: str):
    """(line number, text) for every line outside a fenced R chunk."""
    out, in_chunk = [], False
    for i, line in _strip_front_matter(text.splitlines()):
        if not in_chunk and CHUNK_OPEN.match(line):
            in_chunk = True
            continue
        if in_chunk and FENCE.match(line):
            in_chunk = False
            continue
        if not in_chunk:
            out.append((i, line))
    return out


def _exempt(value: str, unit) -> bool:
    """Bare counts and years need no declaration; anything with a unit does.

    A thousands separator or a decimal point also disqualifies: nobody writes
    "1,500" or "0.12" as a count in this paper, they write it as a measurement.
    """
    if unit:
        return False
    if "," in value or "." in value:
        return False
    try:
        n = int(value)
    except ValueError:
        return False
    return 0 <= n <= 12 or 2000 <= n <= 2099


def typed_numbers(text: str):
    """Numerals the author typed into prose, in document order."""
    found = []
    for lineno, line in prose_lines(text):
        s = line.strip()
        # Headings carry section numbers; table rows are generated or are
        # definition tables; div markers carry no prose.
        if s.startswith("#") or s.startswith("|") or s.startswith(":::"):
            continue
        s = INLINE_R.sub(" ", s)     # computed spans are not typed
        s = IDENT.sub(" ", s)        # identifiers are not measurements
        for m in NUM.finditer(s):
            value, unit = m.group(1), m.group(2)
            if _exempt(value, unit):
                continue
            found.append(Finding(lineno, value.lstrip("+"), line.strip()[:100]))
    return found


def load_allowlist(path) -> dict:
    """value -> list of (context regex or None, why).

    A ``why`` is mandatory. A bare list of permitted numbers becomes a place to
    silence this check; a field that must say why the number is not a result
    makes silencing it visible in review.

    An optional ``context`` restricts where the exemption applies -- the line
    must match it. Without that, an allowlist keyed only by value silences a
    *result* that happens to share a constant's number: 110 is both the
    candidate search radius in NM and the ex-busiest control in the elevation
    bands, and exempting the radius hid the result. That is the very failure
    this module exists to prevent, reintroduced through its own escape hatch.
    """
    path = Path(path)
    if not path.is_file():
        return {}
    entries = yaml.safe_load(path.read_text()) or []
    out = {}
    for e in entries:
        if not isinstance(e, dict) or "value" not in e:
            raise ValueError(f"allowlist entry without a value: {e!r}")
        if not str(e.get("why", "")).strip():
            raise ValueError(
                f"allowlist entry {e['value']!r} has no `why`. A bare list of "
                f"permitted numbers becomes a place to silence this check; a "
                f"field that must say why the number is not a result makes "
                f"silencing it visible in review.")
        ctx = e.get("context")
        out.setdefault(str(e["value"]).lstrip("+"), []).append(
            (re.compile(ctx) if ctx else None, e["why"]))
    return out


def check(qmd, allowlist):
    """Prose numbers that are neither computed nor declared."""
    allowed = load_allowlist(allowlist)
    bad = []
    for f in typed_numbers(Path(qmd).read_text()):
        rules = allowed.get(f.value)
        if rules and any(rx is None or rx.search(f.text) for rx, _ in rules):
            continue
        bad.append(f)
    return bad


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("qmd", type=Path)
    ap.add_argument("--allowlist", type=Path, required=True)
    args = ap.parse_args()

    bad = check(args.qmd, args.allowlist)
    for f in bad:
        print(f"  line {f.line:5d}  {f.value:>10s}  {f.text}")
    print(f"\n{len(bad)} undeclared number(s) in prose")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
