# LLVM Buildbot Monitor

A Discord bot that monitors LLVM's buildbots and posts status updates and failure notifications. It also monitors the LLVM community calendar for office hours and sync-ups.

## Architecture

The project is a Rust workspace with two primary crates:
- `llvm_buildbot_monitor`: The main bot application.
- `calendar_check`: A utility library for fetching and parsing Google Calendar events.

## Development Conventions

### Error Handling
- Use `.context()` or `.with_context()` to add terse, but meaningful context to errors.

## Workflows

After validating your changes, ensure:

- `cargo fmt` is run
- `cargo clippy` produces no new diagnostics
- `cargo test` passes
