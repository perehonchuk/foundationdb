#!/usr/bin/env python3
"""
Validate that a PR has the minimum required number of approving reviews.

This script checks GitHub PR review status and ensures that PRs meet
the project's review requirements before they can be merged.

Usage:
    validate_pr_reviews.py --pr-number <number> [--min-reviews <count>]

Requirements:
    - GitHub CLI (gh) must be installed and authenticated
    - Must be run from within a git repository

Exit codes:
    0: PR has sufficient reviews
    1: PR does not have sufficient reviews
    2: Error occurred (missing dependencies, invalid arguments, etc.)
"""

import argparse
import json
import subprocess
import sys
from typing import List, Dict, Any


# Minimum required approving reviews (updated from 1 to 2)
DEFAULT_MIN_REVIEWS = 2


class PRReviewValidator:
    """Validates PR review requirements using GitHub CLI."""

    def __init__(self, pr_number: int, min_reviews: int = DEFAULT_MIN_REVIEWS):
        self.pr_number = pr_number
        self.min_reviews = min_reviews

    def get_pr_reviews(self) -> List[Dict[str, Any]]:
        """Fetch PR review data from GitHub using gh CLI."""
        try:
            cmd = [
                "gh", "pr", "view", str(self.pr_number),
                "--json", "reviews,author"
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error fetching PR data: {e.stderr}", file=sys.stderr)
            sys.exit(2)
        except FileNotFoundError:
            print("Error: GitHub CLI (gh) not found. Please install it.", file=sys.stderr)
            sys.exit(2)

    def count_approvals(self, pr_data: Dict[str, Any]) -> int:
        """Count the number of approving reviews, excluding the PR author."""
        reviews = pr_data.get("reviews", [])
        author = pr_data.get("author", {}).get("login")

        # Track unique approvers (excluding author)
        approvers = set()

        # Process reviews in order, latest review per user wins
        reviewer_states = {}
        for review in reviews:
            reviewer = review.get("author", {}).get("login")
            state = review.get("state")

            if reviewer and reviewer != author:
                reviewer_states[reviewer] = state

        # Count APPROVED states
        for reviewer, state in reviewer_states.items():
            if state == "APPROVED":
                approvers.add(reviewer)

        return len(approvers)

    def validate(self) -> bool:
        """Validate that the PR has sufficient reviews."""
        print(f"Validating PR #{self.pr_number}...")
        print(f"Minimum required approving reviews: {self.min_reviews}")

        pr_data = self.get_pr_reviews()
        approval_count = self.count_approvals(pr_data)

        print(f"Current approving reviews: {approval_count}")

        if approval_count >= self.min_reviews:
            print(f"✓ PR has sufficient reviews ({approval_count}/{self.min_reviews})")
            return True
        else:
            print(f"✗ PR needs {self.min_reviews - approval_count} more approving review(s)")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Validate PR review requirements",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--pr-number",
        type=int,
        required=True,
        help="GitHub PR number to validate"
    )
    parser.add_argument(
        "--min-reviews",
        type=int,
        default=DEFAULT_MIN_REVIEWS,
        help=f"Minimum required approving reviews (default: {DEFAULT_MIN_REVIEWS})"
    )

    args = parser.parse_args()

    validator = PRReviewValidator(args.pr_number, args.min_reviews)

    if validator.validate():
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
