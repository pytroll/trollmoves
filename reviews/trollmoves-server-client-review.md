Trollmoves Server & Client Review

Location: reviews/trollmoves-server-client-review.md

Summary
- Target: analyze move_it Server and Client implementations in trollmoves/server.py and trollmoves/client.py.
- Focus: correctness, concurrency, message handling, error paths, configuration, and maintainability.

1) Common observations (server + client)

What works well
- Uses posttroll messaging (Message abstraction) consistently for announcing, requesting, and acknowledging transfers.
- Configuration parsing centralised (client.read_config) with defaults and parsing helpers for booleans, nameservers, and backup targets.
- Logging is pervasive and helpful for debugging restarts, timeouts and failures.
- Uses small, focused utilities in trollmoves.utils (clean_url, get_local_ips, gen_dict_extract/contains/translate) which simplifies message transformations.
- Movers architecture (trollmoves.movers) isolates protocol implementations (FileMover, FTP, SFTP, S3) behind a single move_it() API.

Potential risks and suggestions
- Thread-per-request and thread-per-reply: RequestManager.reply_and_send and many other places spawn Threads liberally. In very busy systems this can exhaust resources. Consider using a ThreadPoolExecutor or a bounded worker pool to limit concurrency and reduce thread churn.
- Long/blocking operations inside threads: move_it can perform network/file IO and is called synchronously in request handling (RequestManager._move_file). Ensure worker pool sizing or offloading to dedicated worker threads/processes so ZMQ/REQ/ROUTER sockets aren’t blocked.
- Sparse validation of incoming messages: Message creation is wrapped and MessageError handled, but request payloads are often trusted (e.g., message.data['destination'] assumed present). Add clearer validation and explicit error responses for malformed messages.
- Use of global mutable caches (file_cache, ongoing_transfers): properly locked, but caches have large fixed sizes (deque maxlen). Consider exposing max-size from config and add eviction policy documentation. Also unit tests for concurrent access would be useful.
- Exception handling: many broad except Exception blocks log and continue; where safe, prefer targeted exceptions or re-raising after adding context so calling code can react (especially for transfer failures).

2) Server-specific findings (trollmoves/server.py)

What works well
- RequestManager separates sockets for incoming requests and inproc replies (ROUTER + PULL) and uses a Poller to multiplex IO.
- Deleter thread handles delayed deletion robustly and tolerates missing files (ignores ENOENT).
- _validate_file_pattern uses trollsift.globify to ensure requested files match configured origin patterns.
- _collect_cached_files exposes an info interface to report server state (files, uptime).

Concerns & suggestions
- Address/threading model: RequestManager._process_request spawns a new Thread per message to call reply methods (pong/push/ack/info). Thread bursts for many concurrent requests risk resource exhaustion. Use a worker queue and a limited number of worker threads.
- Message send path: _send_multipart_reply creates a new PUSH socket on each reply. Reusing a pooled socket or keeping a persistent inproc socket would reduce object churn.
- _get_address_and_payload assumes multipart=3; it handles malformed messages but could return None and lose context. Add clearer validation and unit tests for various multipart shapes.
- _validate_requested_file is based on basename fnmatch only; if deeper path checks or symlink resolution needed, this may be bypassed. Consider resolving to absolute paths or using canonicalization when it's security-relevant.
- Deleter.add uses remove_delay from attrs; no max/min enforcement. Very large or negative values might cause odd behavior; validate config values.

3) Client-specific findings (trollmoves/client.py)

What works well
- Listener class handles subscription, heartbeat monitoring, and auto-restart logic and isolates the beat monitor.
- PushRequester implements retries and reconnection logic with jam detection (failures counter and jam flag), which gives resilience to unresponsive servers.
- Chain class manages publishers and listeners per-config chain; supports refreshing configuration and gracefully restarting only changed listeners.
- Transfer workflow: request_push -> _request_files -> unpack_and_create_local_message -> publish local message is well structured and modular.

Concerns & suggestions
- _is_message_already_handled relies on side-effect handlers (_handle_push_message/_handle_ack_message) that mutate global caches. This coupling is subtle; document the expected ordering and side effects. Consider making explicit checks + actions in clearer steps.
- add_request_push_timer uses CTimer threads for hot-spare behavior; these are unbounded threads per timer. Consider using a scheduled single timer wheel or executor for scalability.
- send_request and PushRequester.send_and_recv use small polling intervals and busy loops; fine for responsiveness but could be tuned (and made configurable) to reduce CPU use under high load.
- unpack_xrit and unpack_bzip call external commands and IO; ensure subprocess timeouts and robust error reporting are in place. check_output raises RuntimeError(output) with raw bytes — wrap to provide clearer context.
- create_local_dir: for S3 returns None; downstream code expects local_dir sometimes—ensure callers handle None consistently.

4) Mover & utility notes (trollmoves/movers.py, trollmoves/utils.py)

- move_it uses MOVERS mapping by URL scheme; missing scheme raises KeyError -> logged and re-raised. Consider mapping unknown schemes to explicit error with suggestion list.
- Mover.get_connection and active connection management use CTimer to auto-close; this is good, but relies on active_connections being thread-safe with active_connection_lock — ensure tests cover connection churn.
- Utilities gen_dict_extract/gen_dict_contains are recursive and iterate nested dict/list structures; fine for message shapes but could be expensive for very large nested payloads. Consider short-circuiting or depth limits if necessary.

5) Tests & coverage
- There are tests under trollmoves/tests, but add focused unit tests for:
  - RequestManager: multipart payload parsing variations and invalid messages
  - PushRequester: simulate timeouts and reconnection flows
  - Deleter: confirm delayed removal and ENOENT handling
  - Concurrency: concurrent add_to_ongoing_transfers and termination flows
  - move_it with mock movers to test error propagation and destination path rewriting

Actionable next steps (prioritized)
1. Replace Thread-per-request/reply with a bounded worker pool for RequestManager and reply handling.
2. Add unit tests for malformed messages and multipart shapes to ensure robust parsing in RequestManager._get_address_and_payload.
3. Make thread/timer creation for hot-spare behavior and push replies use a shared scheduled executor or thread pool.
4. Add explicit validation/error messages for missing keys (destination, request_address) in request handling code to make errors clearer to operators.
5. Make timeouts, retry counts, and cache sizes configurable via config file (document in README) and validate config bounds.

References (files inspected)
- trollmoves/server.py
- trollmoves/client.py
- trollmoves/movers.py
- trollmoves/utils.py


---

Notes for maintainers
- If helpful, next iteration can include a small PR converting thread-per-request to ThreadPoolExecutor in RequestManager and demonstrating unit tests covering the reconnection & reply flows.
- If desired, a companion performance test that simulates N concurrent requests and measures thread usage and latencies can help choose pool sizes and timeouts.

End of review.
