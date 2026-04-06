# $name$

$description$

## Getting Started

### Prerequisites
- JDK $java_version$ (Temurin recommended)
- SBT $sbt_version$

### Build
```bash
sbt compile              # Compile with WartRemover + tpolecat
sbt test                 # Run all tests
sbt scalafmtCheckAll     # Check formatting
sbt scalafmtAll          # Auto-format
sbt scalafixAll --check  # Check lint rules
sbt scalafixAll          # Auto-fix
sbt coverage test coverageReport  # Run tests with coverage
```

### Doc Compressor
Regenerate compressed agent docs (requires `ANTHROPIC_API_KEY`):
```bash
sbt docGenerator/run
```

## Project Structure
```
$name$/
  src/main/scala/       # Application code
  src/test/scala/       # Tests
doc-generator/          # Doc compression utility
docs/                   # Full documentation
.claude/agent-docs/     # Compressed docs for agents
```

## Documentation
See `CLAUDE.md` for the agent routing guide and coding conventions.
See `docs/templates/PATTERNS_GUIDE.md` for the template index.
