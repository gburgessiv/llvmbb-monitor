# Contributing to llvm_buildbot_monitor

Thank you for your interest in contributing!

## Code Style and Linting

- Run `cargo fmt --all` before submitting a pull request to ensure your code is properly formatted.
- Run `cargo clippy --no-deps --all-targets --all-features -- -D warnings` and fix any warnings or errors.
- CI will verify formatting and linting automatically.

## Pull Requests

- Open pull requests against the `main` branch.
- Include a clear description of your changes.
- Keep pull requests focused and small when possible.

## Testing

- Run `cargo test --all-features` to ensure all tests pass before submitting.

## Code of Conduct

- Please be respectful and follow the LLVM [Code of Conduct](https://llvm.org/docs/CodeOfConduct.html).

---

If you have any questions, feel free to open an issue or discussion.

