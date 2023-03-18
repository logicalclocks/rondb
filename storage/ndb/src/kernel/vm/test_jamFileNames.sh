#!/bin/bash

# Copyright (c) 2022, 2022, Logical Clocks and/or its affiliates.
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

set -e
NDB_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && cd ../../.. && pwd)

tmpTable=`mktemp`
tmpUses=`mktemp`
tmpErrors=`mktemp`

# Get a list of jam file id and corresponding filenames from the jamFileNames[]
# table in Emulator.hpp. The jam file id (table index) is produced by trusting
# the comment at the end of the line (but this is checked later).
sed -r \
    '# Get the contents of the jamFileNames[] table
     1,/BEGIN jamFileNames/d;
     /END jamFileNames/,$d;
     # Transform entries to number: "filename" format and empty entries to
     # number: NULL format.
     s/^ *("[^"]+"|NULL), *\/\/ *([0-9]+) *$/\2: \1/g;' \
         "$NDB_DIR/src/kernel/vm/Emulator.hpp" > $tmpTable

# Get a list of jam file id and corresponding filenames as they are actually
# used in code.
find "$NDB_DIR" -name '*.?pp' -exec egrep -H '^ *# *define *JAM_FILE_ID' {} + |
    sed -r \
        '# Transform search results to number: "file base name" format.
         s/^.*\/([^/]+\..pp):.*JAM_FILE_ID *([0-9]+) *$/\2: "\1"/' |
    sort -n > $tmpUses

# Report on any discrepancies between the two lists generated above.
diff -u $tmpTable $tmpUses |
    sed -r \
        '# Ignore diff headers for files and hunks, matching lines and the
         # expected difference that empty entries in jamFileNames[] cannot be
         # found in the code.
         /^(@|---|\+\+\+| |-[0-9]+: NULL$)/d;
         # Explain any remaining differences.
         s/^-/Present in jamFileNames\[\] but not used in code: /;
         s/^\+/Used in code but not present in jamFileNames\[\]: /;' \
             >> $tmpErrors

# Check the correctness of the index comments in jamFileNames[] by comparing
# them to a generated sequence.
lastIndex=`tail -n 1 $tmpTable | sed -r 's/:.*//'`
diff -u <(seq 0 $lastIndex) <(sed -r 's/:.*//' $tmpTable) |
    sed -r \
        '# Ignore diff headers for files and hunks, and matching lines.
         /^(@|---|\+\+\+| )/d;
         # Explain any remaining differences.
         s/^-/Missing index comment in jamFileNames\[\]: /;
         s/^\+/Out of place in jamFileNames\[\]: /;' \
             >> $tmpErrors

numberOfErrors=`wc -l < $tmpErrors`
if [ $numberOfErrors == 0 ]; then
    echo "jamFileNames[] looks good."
    rm $tmpTable $tmpUses $tmpErrors
    exit 0
else
    echo "$numberOfErrors errors found in jamFileNames[]:"
    sed -r 's/^/  /;' $tmpErrors
    rm $tmpTable $tmpUses $tmpErrors
    exit 1
fi
