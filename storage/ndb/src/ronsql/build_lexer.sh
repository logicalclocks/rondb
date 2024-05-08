#!/usr/bin/env bash

# Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

set -euo pipefail

FLEX="$1"
SOURCE_RONSQLLEXER_L="$2"
TARGET_RONSQLLEXER_L_HPP="$3"
TARGET_RONSQLLEXER_L_CPP="$4"

[ -x "$FLEX" ]
[ -f "$SOURCE_RONSQLLEXER_L" ]

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# As we are not quite satisfied with the flex output, we let flex output to
# temporary files, allowing us to edit them before they are saved to the target
# location.

"$FLEX" \
    --header-file="$TMPDIR/RonSQLLexer.l.hpp" \
    --outfile="$TMPDIR/RonSQLLexer.l.cpp" \
    "$SOURCE_RONSQLLEXER_L" \
    2>&1 | \
    tee "$TMPDIR/RonSQLLexer.l.err"

# flex has no option to treat warnings as errors, so we use a temporary file for
# that.
if [ -s "$TMPDIR/RonSQLLexer.l.err" ]; then
    echo "flex produced non-empty output on stderr:"
    cat "$TMPDIR/RonSQLLexer.l.err"
    exit 1
fi

# flex temporarily inserts a null byte after each token it scans so that the
# user can comfortably use the token as a null-terminated C string. We don't
# need that, and we also don't want it, for two reasons:
# 1) In case of a parse error, we can't use the underlying buffer to print the
#    entire SQL statement since it may have a null byte somewhere in the middle.
# 2) Unnecessarily saving a character, writing a null byte and restoring it, for
#    every token, could have a slight performance impact.
# Unfortunately, flex does not provide an option to turn off this feature.
# Fortunately, removing this unwanted feature from the flex output in an
# incredibly hacky way is both possible and easy, so that's what we'll do. First
# though, we need to make sure no such patterns are in the source file.

if grep -qE "yy_hold_char| = '.0';" "$SOURCE_RONSQLLEXER_L"; then
    echo "Source file must not contain patterns that need to be removed from the generated file:"
    grep -HEn "yy_hold_char| = '.0';" "$SOURCE_RONSQLLEXER_L"
    exit 1
fi

sed -r "/yy_hold_char/d; / = '.0';/d;" "$TMPDIR/RonSQLLexer.l.cpp" > "$TMPDIR/RonSQLLexer.l.fix_1.cpp"

if diff -q "$TMPDIR/RonSQLLexer.l.cpp" "$TMPDIR/RonSQLLexer.l.fix_1.cpp"; then
    echo "Editing to remove hold_char ineffective."
    exit 1
else
    echo "Confirmed that attempt to remove hold_char made some change."
fi

if grep -qE "yy_hold_char| = '.0';" "$TMPDIR/RonSQLLexer.l.fix_1.cpp"; then
    echo "Editing to remove hold_char ineffective."
    grep -HEn "yy_hold_char| = '.0';" "$TMPDIR/RonSQLLexer.l.fix_1.cpp"
    exit 1
fi

# Flex uses #line directives in the output file to aid error messages. Some of
# them refer to the source file $SOURCE_RONSQLLEXER_L and others to the output
# file. The latter are incorrect in two ways: We have removed some lines, making
# the line number incorrect, and we will move the file to the target below,
# making the file name incorrect. We use `awk` to fix those #line directives.

AWK_SCRIPT='
  # Handle #line directives specially
  /^#line/ {
    if ($3 == "\"" change_from_file "\"")
    {
      correct_line = NR + 1
      # Correct both file file name and line number.
      print "#line " correct_line " \"" change_to_file "\""
      next
    }
    else if ($3 == "\"" no_touch_file "\"")
    {
      # This file name is expected, and should remain. The line number can be
      # anything since it refers to another file, and should also remain.
      print
      next
    }
    else
    {
      # Unexpected file name
      print "Expected file name " change_from_file " or " no_touch_file " but got: " $0 | "cat >&2"
      exit 1
    }
  }
  # Default action, applied to lines other than #line directives is to change
  # nothing
  { print }
'

awk \
    -v change_from_file="$TMPDIR/RonSQLLexer.l.hpp" \
    -v change_to_file="$TARGET_RONSQLLEXER_L_HPP" \
    -v no_touch_file="$SOURCE_RONSQLLEXER_L" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonSQLLexer.l.hpp" \
    > "$TMPDIR/RonSQLLexer.l.fix_1.hpp"
awk \
    -v change_from_file="$TMPDIR/RonSQLLexer.l.cpp" \
    -v change_to_file="$TARGET_RONSQLLEXER_L_CPP" \
    -v no_touch_file="$SOURCE_RONSQLLEXER_L" \
    "$AWK_SCRIPT" \
    "$TMPDIR/RonSQLLexer.l.fix_1.cpp" \
    > "$TMPDIR/RonSQLLexer.l.fix_2.cpp"

# Success

mv "$TMPDIR/RonSQLLexer.l.fix_1.hpp" "$TARGET_RONSQLLEXER_L_HPP"
mv "$TMPDIR/RonSQLLexer.l.fix_2.cpp" "$TARGET_RONSQLLEXER_L_CPP"

echo "Done building RonSQL lexer."
