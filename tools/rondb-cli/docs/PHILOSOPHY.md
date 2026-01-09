# RonDB CLI Philosophy

## The Problem We're Solving

It's 4am. Your distributed cache is misbehaving. You need answers now, not in 30 minutes after spinning up three different tools and wrangling YAML configs.

The rondb-cli exists to collapse debugging time from hours to minutes. One tool. Real commands. Immediate feedback.

## Core Principles

### 1. One CLI to Rule Them All

No separate setup wizard. No post-installation ceremony. No "now run this other tool to do that."

rondb-cli handles:
- **Setup**: Initialize connection, validate cluster topology
- **Control**: Start, stop, reset without ceremony
- **Inspection**: Interactive shell for live debugging
- **Integration**: Works with your existing workflows, doesn't demand special treatment

One binary. Four things it does well. Everything else stays out.

### 2. The 3-Minute Win

A developer should be able to:
1. Connect to the cluster (30 seconds)
2. Run a diagnostic command (30 seconds)
3. Make a decision about what's wrong (2 minutes)

Total: 3 minutes, armed with facts.

This means:
- Fast startup. No bloat initialization.
- Clear output. No parsing required.
- Direct access. No layers between you and the data.
- Smart defaults. Common queries work immediately.

If a task takes more than 3 minutes, we've failed.

### 3. UX Principles

**Beautiful Output, Not Decoration**
- Structured data by default (tables, JSON when needed)
- Color only where it conveys information
- Error messages that explain *why* and *what you can do*

**Fast Feedback Loops**
- Results in milliseconds, not seconds
- Streaming output for long operations
- Ctrl+C works everywhere

**Zero Friction**
- Minimal configuration
- Tab completion for common commands
- History that's actually useful

### 4. The Aha Moment

The real power: you can run Rondis commands (Redis protocol) *and* SQL queries against the same data.

```
rondb> SET user:123 '{"name":"Alice"}'
OK

rondb> SELECT * FROM redis_0.string_keys WHERE redis_key = 'user:123'
┌───────────┬──────────────────┐
│ redis_key │ value            │
├───────────┼──────────────────┤
│ user:123  │ {"name":"Alice"} │
└───────────┴──────────────────┘
```

Same cluster. Same moment in time. Different mental models, same answers.

### 5. What We Don't Do

No enterprise features in the default experience.

- No role-based access control in the CLI
- No audit logging
- No multi-cluster dashboards
- No ORM generation
- No data modeling tools

If you need that stuff, you're solving a different problem.

We stay in the debugging lane. We stay fast. We stay sharp.

## Design Constraints

1. **Silent is not successful.** Every command produces output.
2. **Parsing sucks.** Output is human-first, machine-readable, never cryptic.
3. **Composition over features.** Simple commands + shell = powerful.
4. **Fail loud.** If something doesn't work, we tell you why.
5. **No magic.** We're explicit about everything.

## The Contract

rondb-cli promises:
- **Reliability**: Doesn't modify data unless you explicitly command it
- **Clarity**: Every output tells you something useful
- **Speed**: Feedback in milliseconds
- **Composability**: Works with your tools, not against them
