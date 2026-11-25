# Workflow Execution Context

## 1. Delivery & Git Invariants
- **Remote Invariant**: All working mutations (`git checkout`, `git commit`, `git push`, candidate PR creation) target `origin` (`atyeti-inc/trinodb`) ONLY. Upstream (`trinodb/trino`) is read-only during development.
- **Author Identity Invariant**: All git commits must strictly use:
  `Christopher Caldwell <Christopher.Caldwell@Atyeti.com>`
- **Contributor License Agreement (CLA) Invariant**: All contributions are licensed under Apache-2.0. Submitting any PR upstream to `trinodb/trino` strictly requires that Christopher Caldwell has an executed Trino Contributor License Agreement on file with `cla@trino.io`. No upstream submission may occur without verified CLA registration.
- **PR Delivery & Contributor Invariants**:
  - License Header: Every new source file must include the canonical Trino Apache-2.0 license header (`./mvnw license:format`).
  - Linear History: Linear commit history rebased onto latest upstream `master`. Merge commits are strictly prohibited (`action-merge: fail` in CI).
  - Atomic Bisectable Commits: Every individual commit on a PR branch must compile, pass tests, and pass airstyle/checkstyle independently (`check-commits-dispatcher` / `compile-commit`). Fixup or broken intermediate commits are prohibited.
  - Airlift Commit Structure: Commit messages must strictly follow the Airlift CI specification:
    - Subject Length: Target <= 50 characters; HARD MAXIMUM 60 characters (CI fails on > 60 chars).
    - Subject Syntax: Must begin with a capital letter (or subsystem prefix like `Component: ...`) and must NEVER end with a period (`.`).
    - Imperative Mood: Must use imperative verbs (`Add`, `Fix`, `Update`, `Remove`), never past-tense (`Added`, `Fixed`, `Updated`).
    - Body Formatting: Blank line separating subject and body. All body lines strictly wrapped at <= 72 characters explaining the motivation and behavioral change.
  - PR Description Contract: PR descriptions must adhere to `.github/pull_request_template.md`. Issue-closing references (e.g., `fixes #123`, `closes #123`) MUST be on bulleted list items starting with `- ` or `* ` (never embedded in paragraph prose) to ensure rich link preview rendering.
  - File Trailing Newline Invariant: Every text file (Java, XML, YAML, Markdown, scripts) must terminate with a newline character (`\n`). Upstream CI scans all files via `git grep` and fails on missing trailing newlines.
  - Plan-vs-Diff Containment: Strict plan containment. Zero opportunistic cleanup, unrelated reformatting, or out-of-scope edits.
  - Human Author Voice Mandate: All PR notes, descriptions, and commit explanations must sound like an experienced human engineer who contributes regularly. Zero AI tells: no em-dashes, no emojis, no verbose or dense phrasing, and no restating obvious engine mechanics that any reviewer already understands. Keep the cadence crisp, technical, and directly focused on the change.

## 2. Toolchain & Boundary Invariants
- **Primary Languages & Extensions**: Java 25 (source files: `*.java`), TypeScript/TSX for Web UI (`core/trino-web-ui`).
- **Directory Layout & Boundaries**:
  - `core/`: Core query engine, SPI, server, parser, and Web UI.
  - `plugin/`: Connectors and plugin implementations.
  - `lib/`: Shared utility and format libraries.
  - `client/`: CLI, JDBC driver, and client protocols.
- **Prohibited Tooling**: Mocking frameworks (Mockito/PowerMock) are strictly prohibited; hand-crafted test doubles and mocks must be written.

## 3. Verification & Test Harness Contract
- **Modular Scoping Invariant**: Never run root `./mvnw test` without module scoping (`-pl <module>`), as full repository test suites take 30–60+ minutes. All test and compile commands must be scoped to the touched module.
- **Inter-Module Dependency Invariant**: When targeting submodules in the reactor, always pass `-am` (`--also-make`) so reactor SNAPSHOT dependencies are compiled locally instead of failing on remote lookup.
- **Test Runner & Verification**:
  - Single test class: `./mvnw test -Dair.check.skip-all=true -pl <module> -am -Dtest=<TestClass>`
  - Module tests: `./mvnw test -Dair.check.skip-all=true -pl <module> -am`
- **Lint / Typecheck**:
  - Airstyle verification: `./mvnw airstyle:check -pl <module>`
  - Airstyle auto-format: `./mvnw airstyle:format -pl <module> -Dincludes=**/<FileName>.java`
  - Error Prone static analysis: `./mvnw clean test-compile -Dair.check.skip-all=true -P errorprone-compiler -pl <module> -am`
  - Static checks & Javadoc: `./mvnw verify -DskipTests -P ci -pl <module> -am`
  - License validation: `./mvnw license:check`
  - POM format check: `./mvnw sortpom:verify`
  - Frontend Web UI verification: `bun run verify` (in `core/trino-web-ui/src/main/resources/webapp`)
- **Test Isolation & Sandbox Requirements**: Write deterministic tests with hand-crafted mocks; use AssertJ for complex assertions and Airlift `Assertions` where applicable.
- **Pass Criteria**: Zero compilation errors, zero test regressions, clean airstyle/checkstyle checks, clean working tree.

## 4. Code Style & Design Invariants
- **Typing Discipline**: Strict Java 25 static typing. Use `var` only when the explicit type is obvious from a constructor initializer; avoid `var` when inferred types obscure clarity. Annotate SQL and regex parameters with `@org.intellij.lang.annotations.Language`.
- **Error Handling**: Throw `TrinoException` with an explicit `ErrorCode`. Never swallow exceptions or catch generic `Throwable`/`Exception` without rethrowing or translating into a typed `TrinoException`. Configuration validation must fail fast at coordinator startup using `@DefunctConfig`.
