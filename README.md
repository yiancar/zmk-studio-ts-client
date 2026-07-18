# zmk-studio-ts-client

Simple client library for the ZMK Studio RPC layer, with the message types generated directly from the ZMK Studio
protocol buffer files.

## API

The client exports the generated Studio message types, the low-level `call_rpc`
function, and typed helpers for the lighting subsystem:

- `get_lighting_capabilities()`
- `try_get_lighting_capabilities()` for optional support detection
- `get_lighting_state()`
- `set_lighting_preview_state()`
- `check_lighting_unsaved_changes()`
- `save_lighting_changes()`
- `discard_lighting_changes()`
- `get_lighting_notification()`

Lighting controls are capability-driven. Clients should use the ranges and
effect descriptors returned by `get_lighting_capabilities()` rather than
hard-coding firmware limits. `try_get_lighting_capabilities()` returns
`undefined` when connected to older firmware without the lighting subsystem,
allowing the rest of the Studio UI to remain available.
