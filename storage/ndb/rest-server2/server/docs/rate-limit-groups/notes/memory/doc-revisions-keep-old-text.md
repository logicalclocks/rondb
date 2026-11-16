---
name: doc-revisions-keep-old-text
description: Feedback (2026-10-21) — when Salman asks for a new version or a split of an existing design doc, build it FROM the old HTML verbatim (text + images), extend it; never rewrite/condense from memory
metadata:
  type: feedback
---

When extending or splitting an existing design doc (e.g. the rate-limit buckets doc),
start from the old HTML and keep its wording, tables, worked examples and images; add
new sections in the same voice. Also reuse the old images in any derived doc.

**Why:** On 2026-10-21 I produced two condensed rewrites of
RonDB-Group-Rate-Limits.html; Salman: "the doc not close to what we wrote before… model
the two docs base on this PDF and extend it", then "you have to use the old images too".
The rebuild via a Python script that splices the old `<section id=…>` blocks and inserts
new ones was accepted.

**How to apply:** extract sections programmatically, renumber "section N" references
with a map, insert new sections, verify anchors/images, render with headless Chrome
(sandbox off). Generator kept at rondb/.claude/build_rate_limit_docs.py. See
[[rondb-978-group-rate-limits-idea]].
