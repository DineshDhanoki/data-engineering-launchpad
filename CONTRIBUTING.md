# Contributing

Thanks for helping people learn data engineering. Small fixes, beginner-friendly explanations, tests, and new labs are all welcome.

1. Open an issue (or comment on an existing one) before a large new module so scope and maintenance are clear.
2. Fork the repository, create a focused branch, and keep the PR small enough to review.
3. For code changes, run the full local gate: `python -m launchpad.preflight` and `python -m ruff check .`. The preflight runs the full unit suite and checks the Lab 01 CLI with a temporary database. Add a regression test for behavior changes.
4. For learning content, state prerequisites, learning objectives, runnable steps, expected output, failure cases, and limitations. Link primary docs for tool-specific or changing claims. Record a review date.
5. Explain your change and testing in the PR. Follow the [Code of Conduct](CODE_OF_CONDUCT.md).

Do not include proprietary course material, copied interview question banks, credentials, private datasets, or personal data. Use synthetic or properly licensed samples and document attribution. Maintainers review for correctness and learner accessibility; passing CI alone is not approval.

Suggested first contributions: add a SQL query exercise with a test, improve an error message, add a timezone test, or write a reproducible backfill exercise.

For focused testing, run `python -m unittest discover -s tests -p test_pipeline.py -v`
or replace `test_pipeline.py` with the file you are changing.

The repository supports Python 3.11 through 3.14. CI runs the same preflight on
every supported version on Ubuntu and on Python 3.13 on Windows for SQLite
file-handle behavior. Install the pinned lint version with
`python -m pip install ruff==0.17.6`.

The Ruff gate intentionally checks Python syntax and indentation (`E4`, `E7`,
`E9`), Pyflakes correctness (`F`), and import ordering (`I`). It does not impose
automatic formatting or a broad style migration on learning content.
