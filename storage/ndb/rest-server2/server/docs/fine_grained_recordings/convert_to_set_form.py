"""Convert mysqldump `INSERT INTO t (cols) VALUES (...)` lines into the
human-readable one-column-per-line `INSERT INTO t SET col = value,` form
used by test_go/resources/testdbs/fixed/hopsworks-data/hopsworks_data.sql.

Non-INSERT lines (CREATE TABLE etc.) pass through unchanged.

Usage: python convert_to_set_form.py <file.sql>   (rewrites in place)
"""

import re
import sys


def split_values(s):
    """Split a VALUES tuple body on top-level commas (quote/escape aware)."""
    parts, buf, in_str, i = [], [], False, 0
    while i < len(s):
        c = s[i]
        if in_str:
            if c == "\\":
                buf.append(s[i:i + 2])
                i += 2
                continue
            if c == "'":
                # '' is an escaped quote inside a string
                if i + 1 < len(s) and s[i + 1] == "'":
                    buf.append("''")
                    i += 2
                    continue
                in_str = False
            buf.append(c)
        elif c == "'":
            in_str = True
            buf.append(c)
        elif c == ",":
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
        i += 1
    parts.append("".join(buf).strip())
    return parts


INSERT_RE = re.compile(
    r"^INSERT INTO (`[^`]+`(?:\.`[^`]+`)?) \(([^)]*)\) VALUES \((.*)\);$"
)


def convert(line):
    m = INSERT_RE.match(line.rstrip("\n"))
    if not m:
        return line
    table, cols_s, vals_s = m.groups()
    cols = [c.strip() for c in cols_s.split(",")]
    vals = split_values(vals_s)
    if len(cols) != len(vals):
        sys.exit(f"column/value count mismatch on: {line[:120]}")
    body = ",\n".join(f"  {c} = {v}" for c, v in zip(cols, vals))
    return f"INSERT INTO {table} SET\n{body};\n"


def main(path):
    with open(path) as f:
        lines = f.readlines()
    with open(path, "w") as f:
        for line in lines:
            f.write(convert(line))


if __name__ == "__main__":
    main(sys.argv[1])
