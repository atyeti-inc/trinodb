# Orchestrator Context

## 1. Functional Role & Scope Boundaries
- **Functional Role**: Distributed SQL query engine with SPI/plugin architecture (Modular Monolith).
- **Target Mode**: Fork contributing to upstream
- **Primary Remotes**:
  - `origin`: https://github.com/atyeti-inc/trinodb.git
  - `upstream`: https://github.com/trinodb/trino.git
- **Issue Tracking & Scope Boundaries**:
  - Yeti may NEVER post, edit, remove or in any way interact with the upstream repository unless expressly told to by the user. it is default READ-ONLY.
  - All planned tasks, epics, and work orders map to `atyeti-inc/trinodb`.
  - Upstream Lifecycle: Yeti implements the issue on `feature/<issue-name>`, candidate PR is reviewed by User on `origin`, then submitted upstream to `trinodb/trino` ONLY with express request by the user. Internal release uses `git cherry-pick` into `atyeti-release-###` (published as `atyeti-inc/trinodb:###-atyeti.1`), rebased cleanly onto release version `###` once merged upstream.
  - Committer Identity Invariant: All git commits must strictly use author and committer:
    `Christopher Caldwell <Christopher.Caldwell@Atyeti.com>`
  - Contributor License Agreement (CLA) Prerequisite: Submitting any candidate PR upstream to `trinodb/trino` strictly requires that Christopher Caldwell has an executed Trino Contributor License Agreement verified on file with `cla@trino.io`. PRs must never be submitted upstream without confirmed CLA registration.
  - Refusals & Out-of-Scope: Strict plan-vs-diff containment. Any out-of-scope defects, enhancements, or architectural recommendations discovered during execution MUST be noted onto the origin issue (`atyeti-inc/trinodb`) as a follow-up issue, never logged upstream or included in the active PR.

## 2. Core Architectural Invariants & ADRs
- **Architecture Pattern**: Modular Monolith (Distributed SQL Query Engine with SPI/Plugin Architecture)
- **Key Subsystems & Boundaries**:
  - `core/trino-spi`: Public SPI for connectors, types, functions, and access control. Backward compatibility and minimal dependencies are strictly enforced.
  - `core/trino-parser`: SQL grammar and AST construction using ANTLR4.
  - `core/trino-main`: Query analysis, cost-based optimizer, distributed planner, and vectorized execution engine.
  - `core/trino-server`: Coordinator and worker node lifecycle, REST HTTP APIs, cluster coordination, and resource groups.
  - `core/trino-web-ui`: Administrative and query monitoring Web UI (React, Vite, Bun).
  - `plugin/*`: Connector plugins extending Trino SPI (Hive, Iceberg, Delta Lake, JDBC connectors, etc.).
  - `client/*`: Trino CLI, JDBC driver, and client protocol libraries.
  - `lib/*`: Shared libraries (trino-filesystem, trino-orc, trino-parquet, trino-plugin-toolkit).
- **Architectural Invariants / ADRs**:
  - Zero Mocking Frameworks: Mocking libraries (e.g., Mockito) are strictly forbidden; hand-crafted test doubles and mocks must be written to ensure interfaces remain testable.
  - No `get` in method names unless required for Java bean compliance; use descriptive verbs (`find`, `fetch`) or omit `get` for getters.
  - Use Guava immutable collections (`ImmutableList`, `ImmutableSet`, `ImmutableMap`) rather than JDK unmodifiable wrappers for deterministic iteration.
  - Categorized Exceptions: All query and system errors must throw `TrinoException` categorized by an `ErrorCode`.
  - Exhaustive Enum Switches: Never add a `default` clause to exhaustive enum switches so Error Prone's `MissingCasesInEnumSwitch` catches missing enum values.
  - Configuration Taxonomy: Config properties use kebab-case (`@Config`), session properties use snake_case, renamed properties require `@LegacyConfig`, and removed properties require `@DefunctConfig`.
  - Vector API Invariant: Vector API usage requires a scalar fallback, hardware feature detection, and benchmarks across AMD, ARM, and Intel.
- **Linguistic / Stack Constraints**:
  - Java 25 (targetJdk 25) enforced via Airbase parent POM 427.
  - Maven 3.9+ via `./mvnw` (or `mvnd` when installed).
  - Formatting enforced via `io.airlift:airstyle-maven-plugin`.
  - Static analysis enforced via Error Prone compiler profile (`-P errorprone-compiler`) and Airbase checks.

## 3. Toolchains & Operational Commands
- **Runtimes & Package Managers**: Java 25 (JVM) / Bun 1.x (Web UI) / Maven Wrapper (`./mvnw`)
- **Build / Codegen Prerequisite**:
  - Fast build without tests: `./mvnw clean install -nsu -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true`
  - Recompile changed module (with dependencies): `./mvnw package -DskipTests -Dair.check.skip-all=true -pl <module> -am`
  - POM sorting: `./mvnw sortpom:sort`
  - License header formatting: `./mvnw license:format`
  - Web UI frontend build: `bun run build` (in `core/trino-web-ui/src/main/resources/webapp`)
