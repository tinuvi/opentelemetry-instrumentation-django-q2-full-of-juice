#!/usr/bin/env python3
"""Filter test output to show only failed tests with their exceptions and the overall result.

Usage:
    command | python scripts/filter_failed_tests.py [--slow]

Options:
    --slow  Include the "Slowest test durations" section in the output.

Exit code: 0 if all tests passed, 1 if any test failed.
"""

import argparse
import re
import sys

SEPARATOR = "=" * 70
DASH_SEPARATOR = "-" * 70


def parse_args():
    parser = argparse.ArgumentParser(
        description="Filter test output to show only failures and the overall result.",
    )
    parser.add_argument(
        "--slow",
        action="store_true",
        help='Include the "Slowest test durations" section in the output.',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    lines = sys.stdin.read().splitlines()

    # Extract failure blocks: each starts with SEPARATOR + "FAIL:/ERROR:" and ends
    # at the next SEPARATOR or at a known non-failure section.
    failure_blocks = []
    i = 0
    while i < len(lines):
        if (
            lines[i] == SEPARATOR
            and i + 1 < len(lines)
            and re.match(r"^(FAIL|ERROR): ", lines[i + 1])
        ):
            block = [lines[i]]
            i += 1
            while i < len(lines):
                # Stop before the next failure separator
                if lines[i] == SEPARATOR:
                    break
                # Stop before "Slowest test durations"
                if lines[i].startswith("Slowest test durations"):
                    break
                # Stop before the summary dash separator
                if (
                    lines[i] == DASH_SEPARATOR
                    and i + 1 < len(lines)
                    and re.match(r"^Ran \d+ tests? in", lines[i + 1])
                ):
                    break
                block.append(lines[i])
                i += 1
            # Trim trailing blank lines from the block
            while block and block[-1].strip() == "":
                block.pop()
            failure_blocks.append(block)
        else:
            i += 1

    # Extract "Slowest test durations" section
    slow_lines = []
    if args.slow:
        in_slow = False
        for i, line in enumerate(lines):
            if line.startswith("Slowest test durations"):
                in_slow = True
            if in_slow:
                if (
                    line == DASH_SEPARATOR
                    and i + 1 < len(lines)
                    and re.match(r"^Ran \d+ tests? in", lines[i + 1])
                ):
                    break
                slow_lines.append(line)

    # Find the summary: "------..."\n"Ran X tests in Ys"\n\n"FAILED/OK"
    summary_lines = []
    for i, line in enumerate(lines):
        if (
            line == DASH_SEPARATOR
            and i + 1 < len(lines)
            and re.match(r"^Ran \d+ tests? in", lines[i + 1])
        ):
            summary_lines.append(line)
            summary_lines.append(lines[i + 1])
            for j in range(i + 2, min(i + 4, len(lines))):
                summary_lines.append(lines[j])
                if re.match(r"^(OK|FAILED)", lines[j]):
                    break
            break

    has_failures = len(failure_blocks) > 0

    if has_failures:
        for block in failure_blocks:
            print("\n".join(block))
            print()

    if slow_lines:
        print("\n".join(slow_lines))
        print()

    for line in summary_lines:
        print(line)

    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
