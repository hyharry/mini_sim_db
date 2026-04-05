an extremely simple and portable simulation database based on pandas and csv file

example usage: simulation management, parameter management

requirements: pandas

## CI/CD

This repository includes a GitHub Actions pipeline at `.github/workflows/ci-cd.yml`:

- **CI**: Runs unit tests (`python -m unittest -v`) on Python 3.10, 3.11, and 3.12 for every push and pull request.
- **CD**: On pushes to `main`, creates and uploads a source tarball artifact.

