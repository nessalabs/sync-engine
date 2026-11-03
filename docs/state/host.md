---
id: "host"
title: "Build a cache-first host"
kind: "guide"
status: "implemented"
summary: "The host owns schema interpretation, views, scheduling and permission policy. Reference transcript/task examples demonstrate composition without becoming production integrations."
parent: "sync"
sources: ["examples/two_device_lab.rs", "examples/local_apps.rs", "examples/history_lab.rs", "examples/catalogue_apps.rs", "docs/design/example-apps.md"]
sourceRevision: "f2a05ef24fcff66df9d55e508539cec718e2d805"
diagramLinks: {}
---

# Build a cache-first host

Start with `./scripts/verify-slice-1`: two memory receivers independently catch up from one source. For persistent local records, run `python3 scripts/verify-slice-2.py`. The transcript/task-board examples then compose loopback source, SQLite caches and local browser reads. Use their temporary fixture directories, not real source data.

| Host task | Core boundary | Host still owns |
| --- | --- | --- |
| Show a transcript now | Read verified local records/history progress | Payload fold, formatting, offline/stale UI |
| Refresh after reconnect | begin_pass/step or catalogue begin_or_resume | Timing, cancellation, backoff, policy refresh |
| Scroll older messages | fetch_older plus atomic install | View requests, bounded queue and delayed-plan disposal |
| Open an attachment | Manifest/availability and chunk transfer | Trusted paths, preview/content policy, user intent |
| Respond to revoked access | Typed Denied/Unverifiable/scope failures | Cache invalidation and product behavior |

All coordination is synchronous and explicitly driven. Cancel by stopping further scheduling and discarding uncommitted plans where appropriate; this does not undo a store commit already made. Long operations still need host resource ownership and timeouts.

The library does not interpret agent commands, authorize tool execution or sync Nessa's canonical conversation fold. Existing broader design documents describe targets beyond the implemented reference paths. Report current cache/freshness separately from those future integration promises.
