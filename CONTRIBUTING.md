# Contributing

Thanks for helping people learn data engineering. Small fixes, beginner-friendly explanations, tests, and new labs are all welcome.

1. Open an issue (or comment on an existing one) before a large new module so scope and maintenance are clear.
2. Fork the repository, create a focused branch, and keep the PR small enough to review.
3. For code changes, run `python3 -m unittest discover -s tests -v` and the Lab 01 CLI example. Add a regression test for behavior changes.
4. For learning content, state prerequisites, learning objectives, runnable steps, expected output, failure cases, and limitations. Link primary docs for tool-specific or changing claims. Record a review date.
5. Explain your change and testing in the PR. Follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Do not include proprietary course material, copied interview question banks, credentials, private datasets, or personal data. Use synthetic or properly licensed samples and document attribution. Maintainers review for correctness and learner accessibility; passing CI alone is not approval.

Suggested first contributions: add a SQL query exercise with a test, improve an error message, add a timezone test, or write a reproducible backfill exercise.
