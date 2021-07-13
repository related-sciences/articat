# Contributing to articat

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.
This page provides resources on how best to contribute.

## Discussion forums

If you have any questions, requests, issues, please use
[GitHub issues](https://github.com/related-sciences/articat/issues).

## Development environment

### Download code

Make a fork of the main [articat repository](https://github.com/related-sciences/articat),
and then clone:

```bash
git clone https://github.com/<your-github-username>/articat
```

Contributions to *articat* can then be made by submitting pull requests on GitHub.

### Install

You can install the necessary requirements using pip:

```bash
cd articat
pip install -r requirements.txt -r requirements-dev.txt
```

Also install [pre-commit](https://pre-commit.com/), which is used to enforce coding standards:

```bash
pre-commit install
```

### Run tests

*articat* uses [pytest](https://docs.pytest.org/en/latest/) for testing.
You can run tests from the main ``articat`` directory as follows:

```bash
pytest
```

To run GCP Datastore emulated tests, run:

```bash
pytest --datastore_emulated
```

## Contributing to code

### Continuous Integration

*articat* uses GitHub Actions as a Continuous Integration (CI) service to check code
contributions. Every push to every pull request on GitHub will run the tests,
check test coverage, check coding standards.

### Test

Test files live in [articat/tests](../articat/tests) directory,
test filename naming convention: `<MODULE>_test.py`.

Use double underscore to organize tests into groups, for example:

```python
def test_foo__accepts_empty_input():
    ...

def test_foo__accepts_strings():
    ...
```

### Coding standards

*articat* uses [pre-commit](https://pre-commit.com/) to enforce coding standards. Pre-commit
runs when you commit code to your local git repository, and the commit will only succeed
if the change passes all the checks. It is also run for pull requests using CI.

To manually enforce (or check) the source code adheres to our coding standards without
doing a git commit, run:

```bash
pre-commit run --all-files
```

To run a specific tool (`black`/`flake8`/`isort`/`mypy` etc)::

```bash
pre-commit run black --all-files
```

You can omit ``--all-files`` to only check changed files.

### PR/Git ops

We currently use ``rebase`` or ``squash`` PR merge strategies. This means that
following certain git best practices will make your development life easier.

1. Try to create isolated/single issue PRs

   This makes it easier to review your changes, and should guarantee
   a speedy review.

2. Try to push meaningful small commits

   Again this makes it easier to review your code, and in case of
   bugs easier to isolate specific buggy commits.

Please read [git best practices](https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project#_public_project)
and specifically a very handy [interactive rebase doc](https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History#_rewriting_history>).

### Build/publish

*articat* uses [setuptools_scm](https://github.com/pypa/setuptools_scm/) for versioning.
Packaging and publish happens on GitHub release via `release` GitHub workflow.
