#! /bin/sh
# Copyright (c) Linux Test Project, 2010-2022
# Copyright (c) 2002, Intel Corporation. All rights reserved.
# Created by:  julie.n.fleischer REMOVE-THIS AT intel DOT com
# This file is licensed under the GPL license.  For the full content
# of this license, see the COPYING file at the top level of this
# source tree.
#
# Use to build and run tests for a specific area
# Modified to output structured results for easier parsing

TESTPATH=""
OUTPUT_FORMAT="${OUTPUT_FORMAT:-ORIGINAL}"
EXECUTED_TESTS=""

BASEDIR="$(dirname "$0")/../${TESTPATH}/conformance/interfaces"

usage()
{
    cat <<EOF
usage: $(basename "$0") [AIO|MEM|MSG|SEM|SIG|THR|TMR|TPS] [--json|--csv|--structured]

Options:
  --json        Output in JSON format
  --csv         Output in CSV format
  --structured  Output in structured text format (default)
  (no args)    Original output format

EOF
}

parse_test_result()
{
    local test_script="$1"
    local test_dir="$(dirname "$test_script")"
    local test_name="$(basename "$test_script" .run-test)"
    local test_output
    
    cd "$test_dir" 2>/dev/null
    
    if [ "$OUTPUT_FORMAT" = "json" ]; then
        test_output=$(./$(basename "$test_script") 2>&1)
        local result="PASS"
        local reason=""
        
        if echo "$test_output" | grep -q "Test FAILED\|return code didn"; then
            result="FAIL"
            reason=$(echo "$test_output" | grep -E "Test FAILED|return code didn" | head -1 | sed 's/"/\\"/g')
        elif echo "$test_output" | grep -qi "unsupported\|unresolved"; then
            result="SKIP"
            reason=$(echo "$test_output" | grep -iE "unsupported|unresolved" | head -1 | sed 's/"/\\"/g')
        fi
        
        printf '{"testcase":"%s","result":"%s","reason":"%s"}\n' "$test_name" "$result" "$reason"
    elif [ "$OUTPUT_FORMAT" = "csv" ]; then
        test_output=$(./$(basename "$test_script") 2>&1)
        local result="PASS"
        local reason=""
        
        if echo "$test_output" | grep -q "Test FAILED\|return code didn"; then
            result="FAIL"
            reason=$(echo "$test_output" | grep -E "Test FAILED|return code didn" | head -1)
        elif echo "$test_output" | grep -qi "unsupported\|unresolved"; then
            result="SKIP"
            reason=$(echo "$test_output" | grep -iE "unsupported|unresolved" | head -1)
        fi
        
        printf '%s,%s,"%s"\n' "$test_name" "$result" "$reason"
    elif [ "$OUTPUT_FORMAT" = "structured" ]; then
        test_output=$(./$(basename "$test_script") 2>&1)
        
        if echo "$test_output" | grep -q "Test FAILED\|return code didn"; then
            local reason=$(echo "$test_output" | grep -E "Test FAILED|return code didn" | head -1)
            printf '[RESULT] FAIL [TESTCASE] %s [REASON] %s\n' "$test_name" "$reason"
        elif echo "$test_output" | grep -qi "unsupported\|unresolved"; then
            local reason=$(echo "$test_output" | grep -iE "unsupported|unresolved" | head -1)
            printf '[RESULT] SKIP [TESTCASE] %s [REASON] %s\n' "$test_name" "$reason"
        else
            printf '[RESULT] PASS [TESTCASE] %s\n' "$test_name"
        fi
    else
        ./$(basename "$test_script")
    fi
    
    cd - >/dev/null 2>&1
}

run_option_group_tests()
{
	local list_of_tests

	list_of_tests=`find $1 -name '*.run-test' | sort`

	if [ -z "$list_of_tests" ]; then
		if [ "$OUTPUT_FORMAT" != "ORIGINAL" ]; then
			echo ".run-test files not found under $1" >&2
		fi
		return
	fi

	for test_script in $list_of_tests; do
		real_path="$(realpath "$test_script" 2>/dev/null)"
		if [ -z "$EXECUTED_TESTS" ]; then
			EXECUTED_TESTS="$real_path"
		elif echo "$EXECUTED_TESTS" | grep -qF "$real_path"; then
			continue
		else
			EXECUTED_TESTS="$EXECUTED_TESTS
$real_path"
		fi
		parse_test_result "$test_script"
	done
}

case $1 in
--json)
    OUTPUT_FORMAT="json"
    shift
    ;;
--csv)
    OUTPUT_FORMAT="csv"
    shift
    ;;
--structured)
    OUTPUT_FORMAT="structured"
    shift
    ;;
--help|-h)
    usage
    exit 0
    ;;
esac

case $1 in
AIO)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] AIO"
    run_option_group_tests "$BASEDIR/aio_*"
    run_option_group_tests "$BASEDIR/lio_listio"
    ;;
SIG)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] SIG"
    run_option_group_tests "$BASEDIR/sig*"
    run_option_group_tests $BASEDIR/raise
    run_option_group_tests $BASEDIR/kill
    run_option_group_tests $BASEDIR/killpg
    run_option_group_tests $BASEDIR/pthread_kill
    run_option_group_tests $BASEDIR/pthread_sigmask
    ;;
SEM)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] SEM"
    run_option_group_tests "$BASEDIR/sem*"
    ;;
THR)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] THR"
    run_option_group_tests "$BASEDIR/pthread_*"
    ;;
TMR)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] TMR"
    run_option_group_tests "$BASEDIR/time*"
    run_option_group_tests "$BASEDIR/*time"
    run_option_group_tests "$BASEDIR/clock*"
    run_option_group_tests $BASEDIR/nanosleep
    ;;
MSG)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] MSG"
    run_option_group_tests "$BASEDIR/mq_*"
    ;;
TPS)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] TPS"
    run_option_group_tests "$BASEDIR/*sched*"
    ;;
MEM)
    [ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[TESTSUITE] MEM"
    run_option_group_tests "$BASEDIR/m*lock*"
    run_option_group_tests "$BASEDIR/m*map"
    run_option_group_tests "$BASEDIR/shm_*"
    ;;
*)
    usage
    exit 1
    ;;
esac

[ "$OUTPUT_FORMAT" != "ORIGINAL" ] && echo "[COMPLETE]"
