# Error Investigation Protocol

When the user reports an error, follow these steps in order:

## 1. Read ALL project files

Do not rely on the error trace alone. Read every source file (.py, .md, configs)
to understand the full codebase before diagnosing.

## 2. Trace the execution path

Identify which function call triggered the error, in what order,
and what data was involved. Map the call chain from entrypoint to failure.

## 3. Understand the environment

Check: OS, Python version, library versions, relevant env vars,
and any contextual clues the user provides (e.g., "happens after Termux closes").

## 4. Search for the actual root cause

Do not stop at the surface error. Example: an
`[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed`
error could be caused by:
- Missing CA bundle on disk
- Reset env vars after process restart
- Corrupted state triggering an unexpected code path
- Time sync issues on device
- Library version incompatibility
- Race condition during environment re-initialization

Verify the cause with targeted diagnostics before proposing a fix.

## 5. Propose a fix based on full context

Address the underlying cause, not just the symptom.
Include defensive measures so the same class of error
is handled gracefully in the future.

## 6. Add startup diagnostics

Log relevant system state at startup so future errors
can be diagnosed without user interaction.
