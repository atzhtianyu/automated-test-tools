#! /bin/sh
#
# A simple wrapper for executing all of the tests.
#
# See COPYING for licensing details.
#
# Ngie Cooper, July 2010
#

OUTPUT_FORMAT="${OUTPUT_FORMAT:-ORIGINAL}"
PROG_SCRIPT="$(dirname "$0")/run-posix-option-group-test.sh"

FAILED=0

for option_group in AIO MEM MSG SEM SIG THR TMR TPS; do
    OUTPUT_FORMAT="$OUTPUT_FORMAT" $PROG_SCRIPT $option_group 2>/dev/null
    if [ $? -ne 0 ]; then
        FAILED=1
    fi
done

if [ "$OUTPUT_FORMAT" != "ORIGINAL" ]; then
    echo "[ALL_COMPLETE]"
fi

exit $FAILED
