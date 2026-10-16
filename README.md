# peer-agents

**English** | [简体中文](README.zh-CN.md)

> A testbed for multi-agent communication: sub-agents talk to **each other** — and **decide for themselves whom to connect to**.

Most multi-agent frameworks today have a single orchestrator handing work down to subordinates, with no way for those subordinates to pass information sideways. This repo asks a simple question: what actually happens if you open up lateral communication?

The answer turns out to be quantifiable, and a little counterintuitive. The whole project has **zero dependencies** — standard library only.

```bash
python run_demo.py            # five topologies compared, runs in ~0.1s
python run_dirty_demo.py      # four dirty-environment configs: stale data / rumor / silent member
python run_llm_demo.py        # end-to-end LLM protocol run (bundles a local stub server)
python run_wire_demo.py       # a Node agent joins the same run over a socket (needs node >= 18)
python -m unittest discover -s tests    # 159 regression tests
```

---

## 1. The core result

Same incident-triage task. One variable changed: **can sub-agents talk to each other directly?**

| Configuration | Data msgs | Control msgs | Rounds | Lateral msgs | Hub load | Fidelity |
|---|---|---|---|---|---|---|
| Peer (full mesh) | **9** | 0 | **5** | 6 | **3** | 100% |
| Star relay (centralized) | 15 | 0 | 8 | 0 | 15 | 100% |
| Star relay + hub summarization | 15 | 0 | 8 | 0 | 15 | **50%** |
| Adaptive topology (budget 3) | **9** | 8 | 6 | 6 | **3** | 100% |
| Adaptive topology (budget 2) | **9** | 8 | 7 | 6 | **3** | 100% |

Three findings worth noting:

**1. A central hub becomes both an information bottleneck and a context bottleneck.**
Under star topology the hub handles **every single** data message in the system (`hub load == total data messages`). Under peer topology it handles 3. That number converts directly into context-window cost.

**2. The irreplaceable value isn't "faster" — it's "information that no middleman degrades."**
The third row simulates what a real LLM orchestrator actually does: it **paraphrases** rather than **forwards**. When I truncated relayed content to 38 characters, the decisive evidence in the final report — `status IN (6 values)` and `idx_order_items_status` — vanished on the spot, leaving a hollow shell of a conclusion. **Fidelity fell from 100% to 50%.**

**3. Topology can be grown, not planned.**
The last two rows start from only 3 links — **the database team is a literal island.** The agents use 8 negotiation messages to expand the network to 6 links (or 4), and data traffic ends up **matching** a peer architecture that an omniscient planner had fully connected from the start — with no node carrying global context. Those 8 messages are a one-time investment that buys you out of a real problem: at startup, nobody knows who to connect to.

One counterintuitive emergent result: tightening the link budget from 3 to 2 produced **fewer** final links (4), yet triggered 3 LRU evictions, while **average path length fell from 1.33 hops to 1.11** — with traffic and fidelity completely unchanged. The reason: under budget pressure, what gets evicted are the low-frequency connections, and what survives is the genuinely high-traffic backbone. The topology optimizes itself.

> ⚠️ **But that table is a "clean environment" result — do not extrapolate it.** It holds only because every topic has exactly one knowledgeable party, information is always fresh, and answers are always self-consistent. Real environments do not look like that. Section 4 deliberately dirties the environment, and the conclusion **inverts** — read that section before forming a judgement.

---

## 2. Architecture

```
peer-agents/
├── run_demo.py                 entry point for the five-configuration comparison
├── run_dirty_demo.py           dirty-environment entry point (full transcript + cognition ledgers)
├── run_llm_demo.py             end-to-end LLM protocol demo (switchable to a real model)
├── run_wire_demo.py            cross-language demo: the `deploy` member runs as a Node process over a socket
├── pyproject.toml              packaging + pytest/ruff/mypy config (zero runtime dependencies)
├── .github/workflows/ci.yml    CI: test matrix / zero-dep availability / cross-language interop / static checks
├── docs/
│   └── PROTOCOL.md             the wire protocol, v1: framing / frames / field permissions (one page)
├── tools/
│   ├── mock_llm_server.py      local LLM stub server (used to verify the protocol layer)
│   └── peer_agent.mjs          zero-dependency Node reference implementation of the wire protocol
├── peer_agents/
│   ├── message.py              immutable message model (addressable / forwardable / multi-hop)
│   ├── bus.py                  message bus: delivery + topology control + routing + link negotiation
│   ├── topology.py             dynamic communication graph: budget / LRU eviction / BFS shortest path
│   ├── blackboard.py           shared blackboard (service discovery + fact accumulation)
│   ├── agent.py                agent shell + pluggable brain
│   ├── runtime.py              event loop + convergence / circuit-breaker / fault-isolation guardrails
│   ├── facts.py                facts carrying metadata: confidence / timestamp / evidence / source
│   ├── wire.py                 wire protocol: NDJSON framing + the writable / reject / overwrite field boundary
│   ├── transport.py            synchronous, thread-free transport (host drives; one round-trip in flight)
│   ├── remote.py               mount a remote process as a brain (it only decides what to send; the host keeps the bus and topology)
│   ├── scenario_incident.py     scenario A: distributed triage of a production incident (clean)
│   ├── scenario_dirty.py       scenario B: the same incident, with the environment dirtied
│   ├── brain_dirty.py          dispositions: gullible / skeptical (with self-correction and retry)
│   ├── llm.py                  optional: swap the rule brain for an LLM brain
│   ├── report.py               console trace and HTML report
│   └── dirty_report.py         dirty-environment report: claim adjudication + cognition ledgers
└── tests/
    ├── test_peer_agents.py     30 tests: communication-mechanism regressions
    ├── test_dirty.py           24 tests: dirty-environment mechanism regressions
    ├── test_runtime.py         18 tests: runtime trustworthiness (failure visibility / fault isolation / re-entrancy / determinism)
    ├── test_wire.py            52 tests: the wire protocol's framing and field-permission boundary
    ├── test_remote.py          28 tests: a remote brain driving the framework over a socket
    └── test_interop.py          7 tests: a real Node process running the same scenario end to end
```

### Three design decisions

**1. Cognition is decoupled from communication.**
An agent owns only an identity and a mailbox; the decision logic lives in a pluggable `Brain`. So you can run **reproducible** experiments with the deterministic `RuleBrain`, or swap in an `LLMBrain` backed by a real model. **The maturity of the communication infrastructure and the intelligence of any individual agent are orthogonal concerns.**

**2. Topology is an infrastructure-layer policy, not something hard-coded into business logic.**
The bus honors exactly one function: `policy(src, dst) -> bool`. Change nothing in business code — change only that function, and the entire system goes from centralized to peer. Under star topology the bus does not silently drop packets; it hands them to the hub to relay and tags them with `relay_to`. That makes "everything gets paraphrased through the orchestrator" reproducible without a single line of cooperation from business code — which is what gives us a fair baseline to compare against.

**3. Control plane / data plane separation.**
In dynamic-topology mode, link negotiation uses **out-of-band addressing** (delivered straight to a directory address, unconstrained by the current topology), while data messages remain strictly topology-constrained. This mirrors the separation of routing protocols from data forwarding in real networks — otherwise you hit the chicken-and-egg problem of "you need a link before you can negotiate a link."

### Four guardrails you must add once communication is open

This is where implementations most often fall over. `runtime.py` has all four:

- **Convergence**: an agent cannot simply declare itself done. Quiescence requires a full round in which no agent received mail, **and the pending queue is empty**.
- **Infinite loops**: message dedup + "ask any given question only once" + a hop limit, stacked three deep. Note that the dedup signature must include `relayed_by`, or hub-relayed messages get misjudged as duplicates and dropped.
- **Controllability**: a total message budget that trips a circuit breaker so a runaway cannot burn through your API quota; a round cap backs it up. The result distinguishes the two via `exit_reason`.
- **Survivability**: one agent throwing no longer aborts the round — the fault is recorded, **in-flight messages go to a dead-letter queue**, and a member that hits the consecutive-fault threshold is removed. In a group that communicates autonomously, "this member is unavailable right now" is the normal case, not an accident.

---

## 3. The demo scenario

The root cause is scattered across four teams' private knowledge. No single agent holds the whole picture:

```
gateway      only knows "not my problem — the bottleneck is downstream, waiting on DB connections"
database     only knows "order_items is doing a full table scan"
release      only knows "v2.31 shipped, but it didn't touch DDL"
ordersvc     only knows "v2.31 rewrote the query predicate"
                    ↓ only assembled do you get
actual root cause: predicate changed → old index no longer applies → SeqScan → connection pool saturates → P99 degrades
```

The key point is that **the reasoning path is generated dynamically**: only after the DBA learns "this release contained no DDL" does it have grounds to ask "then did the query statement itself change?" That chain — where A's conclusion determines whom B should ask — is exactly what centralized orchestration struggles to express efficiently.

A run produces `out/comparison.html`, containing the topology-evolution diagram and the full message-by-message sequence.

---

## 4. Dirtying the environment: an experiment against myself

The scenarios above were **designed to match the conclusion**: one knowledgeable party per topic, always-fresh information, always-consistent answers. Under those assumptions "ask the right person" equals "get the truth," so peer communication wins by default.

Real environments do not look like that. So this section does the opposite: **it deliberately makes the environment hostile to the hypothesis that peer is better.**

Five kinds of dirt, injected:

| Dirt | Concrete form | What it attacks |
|---|---|---|
| Multiple knowledgeable parties per topic | `index_state` / `release_meta` / `rate_limit` each have 2 sources | "Ask the right one" no longer equals "get the truth" |
| High-confidence stale snapshot | The DBA's schema snapshot at `conf=0.92` — but 100 time units old | Any rule that checks only confidence will fall for it |
| Surface-level conflict | Release says "no DDL this time" vs audit says "there was one DDL, already rolled back" | Single-source information is always self-consistent; only cross-checking exposes disagreement |
| Low-confidence distractor | The gateway floats a rumor — "the rate-limit change might be the cause," `conf=0.60` | Skimp on verification and you get led astray |
| Unreliable member | Ordersvc is firefighting and ignores the first two questions | Forces out the retry mechanism, which almost never fires in the other demos |

Plus one lever **orthogonal** to topology — disposition:

- `gullible`: asks only the **first** source per topic, and believes whatever comes back
- `skeptical`: asks every source, compares confidence and timestamps, and **allows self-correction**

```bash
python run_dirty_demo.py     # four configurations, printing the full transcript + each agent's cognition ledger
```

Results:

| Disposition × topology | Rounds | Data msgs | Hub load | Lateral | Fidelity | Self-corrections | Stale accepted |
|---|---|---|---|---|---|---|---|
| skeptical × peer | 10 | 32 | 24 | 8 | **100%** | 1 | 0 |
| skeptical × star | 11 | 40 | 40 | 0 | **100%** | 1 | 0 |
| gullible × peer | 10 | 23 | 19 | 4 | **50%** | 0 | 1 |
| gullible × star | 11 | 27 | 27 | 0 | **50%** | 0 | 1 |

**The conclusion is the opposite of sections 1–3, and that is the whole point:**

**1. Fidelity is determined by epistemic discipline, not by topology.**
The gullible disposition scores 50% under both peer and star; the skeptical disposition scores 100% under both. On all three contested topics, the skeptical strategy sided with the authoritative record every time, and the gullible strategy got it wrong every time — and **switching to a peer topology does nothing to save an undefended mind.**

**2. Topology determines cost, not correctness.**
Star pays 25% more traffic than peer, and all of that overhead lands on the hub (hub load 40 vs 24).

**3. Skepticism is not free.**
It asks more and cross-checks more: `32` data messages vs gullible's `23`. But it buys one critical self-correction — it first accepted the release platform's "no DDL," then reversed itself once the audit's per-statement execution log arrived. In the gullible strategy that number is `0`: **it never even got the chance to change its mind.**

So the more honest statement is: **the value of peer communication is not being "more correct" — it is being "cheaper," and letting critical information bypass the centre's context.**

A run produces `out/dirty.html` (contested-claim adjudication panel + each agent's cognition ledger) and `out/dirty.json`.

---

## 5. Verification status (please read this section first)

**Verified** (covered by tests, `python -m unittest discover -s tests`):

- Topology budget, LRU eviction, BFS shortest path
- All three delivery paths: star relay / direct peer / multi-hop routing
- All three topologies reach the same root cause; hub relaying loses detail
- An adaptive topology self-organizes from a sparse start and matches full-mesh efficiency
- The pending queue correctly blocks the convergence decision
- **Dirty environment**: high-confidence stale snapshots and low-confidence rumors are rejected; a higher-confidence arrival triggers self-correction; a silent member is retried until it answers; retry messages are not wrongly caught by dedup; the hub does not swallow pass-through messages
- **Fidelity is independent of topology** (the core dirty-environment result): under both gullible and skeptical dispositions, peer and star produce identical fidelity
- **Boundary defence on model output**: `to` is normalized into a list of strings (it cannot be split into ghost recipients), `kind` is restricted to the data-plane whitelist (control-plane types are refused), and the bus's `send()` rejects control-plane messages
- **The `LLMBrain` protocol layer**: prompt rendering is complete, the HTTP request takes shape, response parsing is fault-tolerant (fenced code blocks / explanatory prose mixed in / missing fields), and a full protocol run yields a **final report and round count byte-identical to the rule brain** — meaning the transport layer introduces no distortion
- **Runtime trustworthiness** (`test_runtime.py`): failures are reported explicitly (`completed` / `ok` / `exit_reason` / `unresolved`); `describe_outcome()` distinguishes "converged / budget-tripped / round-capped" and names what each agent is missing; one member crashing does not abort the round, and a member hitting the consecutive-fault threshold is removed, with in-flight messages going to the dead-letter queue rather than evaporating; re-running the same instance is explicitly refused rather than yielding a fake success; the dedup table is bounded (5,000 messages still leaves only a window's worth); and two runs of the same configuration produce byte-identical message sequences, with the first ID fixed at `m00000`
- **The wire protocol** (`test_wire.py`, 52 cases): framing round-trips, blank lines are skipped as heartbeats, half a line followed by EOF is a protocol error rather than "nothing arrived", a reader refuses to buffer past 1 MiB, version mismatch is refused instead of limped through, and unknown capabilities are rejected rather than silently granted. Every field a peer may write has a test; so does every field it may not — `sender`, `relayed_by`, `origin_kind` reject the whole frame, and `relay_to` is accepted only from a peer the host has designated as the hub, with `relayed_by` still derived by the host and never read off the wire. `to` given as a bare string is **refused, not split**
- **Remote brains really drive the framework** (`test_remote.py`, 28 cases): a brain on the far side of a socket produces messages that carry the framework's identity and ID scheme; the peer cannot send a control-plane kind or forge `relayed_by` without the exception reaching `RunResult.faults`; a declared capability really results in the question being forwarded over the wire, and an undeclared one is answered by the host's default policy; a broken link negotiation is a *rejection*, not a crash; and a remote that dies is isolated, evicted at the fault threshold, its in-flight messages dead-lettered, and the missing premise named in `unresolved`
- **Cross-language interop is real** (`test_interop.py`): a genuine Node process (`tools/peer_agent.mjs`, zero npm dependencies) completes the handshake, answers a `think` frame, decides link requests when it declares `accepts_link`, and drives a full run to completion — with the final report, round count, message count, fidelity and topology evolution all identical to the all-Python run. A node process that receives an unknown frame exits with a distinct code rather than pretending nothing happened

**Not verified** (PRs welcome):

- ⚠️ **`LLMBrain` has never been connected to a real model.** The protocol layer is verified against the local stub server (`python run_llm_demo.py`), but that proves "the plumbing works," not "the model is good." Will a real model proactively ask the right party? Will it state its conclusion clearly? Will it pick the right neighbour in the topology? A stub server cannot answer those questions and has no business pretending it can. This is the part I most want someone else to verify first.
- ⚠️ **`DirtyBrain` is a hard-coded rule, not real cognition.** It proves that the mechanism — confidence + freshness + cross-verification — can intercept dirty data. It does not prove a real LLM would behave that way. Swapping `DirtyBrain` for an LLM brain that reads `Fact` metadata is the single most valuable next experiment for this repo.
- The demo scenarios are **synthetic and rule-driven**. They demonstrate a communication mechanism, not "what real LLM team collaboration looks like."
- Scheduling is single-threaded round-robin, so that traces are fully reproducible. **There is no concurrency**, and races under concurrency are untested.
- Node counts are only 5–6. Routing and convergence at tens or hundreds of agents are untested.
- The "fidelity" metric in `run_demo.py` / `run_dirty_demo.py` is a keyword hit rate — crude, but usable as a proxy.
- ⚠️ **Only Python ↔ Node has ever interoperated.** "Any language can implement this" is *inferred from the spec*, not a verified fact — writing a third implementation and wiring it into `test_interop.py` is what would make it one. The wire layer is plaintext, unauthenticated and loopback-only; identity is decided by whoever connects to the seat first, so any process on the machine can impersonate a member. Treat it as a research transport, not as something to expose on a network.
- The wire layer handles **one peer per session** (the host's `accept()` is synchronous), and there is no reconnect — a dropped connection is recorded as a fault and the member is evicted at the threshold.
- The remote brain is a "answer what you're asked" reference implementation: it does not reason, follow up, or conclude. Handing it a role like `dba` (which must chase a chain of inference) will not produce a root cause. That is a limit of the reference implementation, not of the protocol.

**Not applicable / do not over-extrapolate**:

- **This is a research prototype, not a production-ready library.** It now has a `pyproject.toml`, CI (`.github/workflows/ci.yml`: test matrix / zero-dep availability / cross-language interop / static checks), and static gates from `ruff` + `mypy` — but **engineering completeness is not epistemic reliability**. None of the key premises from earlier sections (single-threaded scheduling, synthetic scenarios, 5–6 nodes) have changed. Several **silent failures** that were empirically confirmed have been fixed and pinned by `test_runtime.py`:
  - `Runtime.run()` now **refuses re-entry** — a second call raises immediately and points you at `build()` for a fresh instance, instead of returning a fake success where round 1 reports a copied message count;
  - `MessageBus._seen` became a **bounded sliding window** (default 4096), so long runs no longer grow memory without limit;
  - **Fault isolation + a dead-letter queue** were added — one agent throwing no longer aborts the round, and in-flight messages go to dead letters rather than evaporating;
  - Results now carry explicit **failure signals**: `completed` / `ok` / `exit_reason` (converged / budget-tripped / round-capped) plus per-agent `unresolved`, instead of forcing you to guess from `final_report == ""`;
  - `msg_id` is now a **deterministic sequence** (starting at `m00000`), so two runs of the same configuration are byte-for-byte diffable.
- This does **not** mean "peer is always better than centralized." Where strong auditing, strong consistency, or strong controllability matter, centralization has clear advantages. This repo quantifies exactly one dimension: **communication efficiency and information fidelity.**
- Scenario A is deliberately built around "islands of private knowledge" — the case where peer communication pays off most. When information is already concentrated in one place, a lateral channel is worthless.
- Scenario B demonstrates the opposite side: **once the environment is dirty, what decides the outcome is each agent's information discipline, not the communication topology.** So "open up lateral communication" is neither sufficient nor necessary — it only lowers the cost when **information is genuinely dispersed and everyone is willing to verify.**
- An experiment at 5–6 nodes and a few dozen messages should not be extrapolated directly to production systems.

---

## 6. Connecting a real model

`run_llm_demo.py` starts a **local stub server that speaks the OpenAI protocol** by default, so you can verify the protocol layer without any credentials:

```bash
python run_llm_demo.py            # runs against the stub; no API key needed
```

To switch to a real model, no code changes required:

```bash
export OPENAI_API_KEY=sk-xxx
export OPENAI_BASE_URL=https://api.openai.com/v1   # optional
export OPENAI_MODEL=gpt-4o-mini                     # optional
python run_llm_demo.py --real
```

Or use `build(brain_mode="llm")` from `scenario_incident.py` in your own pipeline — the communication layer needs no changes at all.

`LLMBrain` requires the model to return a strict JSON array, like so:

```json
[{"to": ["dba"], "kind": "QUERY", "topic": "slow_query", "content": "..."}]
```

Before model output enters the framework it is normalized in `_sanitize` — **the single entry point, and a permission boundary**:

- `to` is normalized into a list of strings. Models often write `"to": "dba"`, and a naive `tuple()` over that produces five ghost recipients — `a/u/d/i/t` — while the one agent who should have received it gets nothing. **No exception, no log; it just looks like "they didn't answer me."**
- `kind` must fall inside the **data-plane whitelist** (`DATA_KINDS`). Control-plane types (`LINK_REQUEST` and friends) are dropped outright — otherwise the model could forge a link request and rewrite the topology past the framework.
- Any parse failure (fenced code blocks, mixed-in prose, missing fields, wrong types) degrades uniformly to "send nothing this round" — **in a group that communicates autonomously, a member going mute beats a member broadcasting bad instructions.**

The same boundary is enforced a second time at the **bus**: `MessageBus.send()` outright rejects any control-plane message. Permissions cannot rest on the brain's good behaviour alone.

---

## 7. Talking to agents written in another language

For most of this project's life, "agents talk to each other" meant one Python object being
appended to another Python list. That is the easiest possible case of the problem this repo
is about: no serialization, no partial reads, no peer disappearing, no version skew. Move the
boundary onto a socket and all four show up at once.

So there is now a wire protocol — [`docs/PROTOCOL.md`](docs/PROTOCOL.md), one page, complete
enough to implement from — plus a zero-dependency Node reference implementation in
[`tools/peer_agent.mjs`](tools/peer_agent.mjs):

```bash
python run_wire_demo.py     # needs node >= 18
```

### The one design decision that matters

A remote process does **not** get its own message bus, its own view of the topology, or a vote
on the framework. It is **a brain on the far side of a socket**: it sees a mailbox, and it says
what it wants to send. Nothing else.

That single choice is what keeps the whole thing verifiable:

| It can | It cannot |
|---|---|
| choose recipients (`to`) | choose who it **is** — `sender` comes from the session |
| choose a data-plane `kind` | send a control-plane message (`LINK_REQUEST` and friends) |
| choose what to say (`topic` / `content`) | claim "I was already relayed by the hub" (`relayed_by`) |
| `reply_to` a message | choose its own position in the trace (`id`) |

A remote agent therefore inherits **exactly** the privileges of `LLMBrain` — not one line of the
framework had to be loosened. Topology is still grown and pruned by `Topology` and `MessageBus`
on the host side. The most a peer can do is answer the question "would you accept a direct link
to me?" — and even then the link budget and LRU eviction stay with the host, so answering
"yes" does not buy it a bigger budget.

### The security model falls straight out of §9

Two entries in the pitfall list are the entire reason the field whitelist exists:

- **#1**: `relayed_by == hub` is the fast path that bypasses topology completely. If a peer
  could set `relayed_by`, it would be holding a topology bypass in a single field.
- **#12**: a model that can emit `{"kind": "LINK_REQUEST"}` can rewrite the topology past the
  framework.

Both apply at full strength once the sender is an arbitrary process. So the fields of a `say`
frame are split three ways — **writable / reject the whole frame / silently overwritten** —
with one test per field in `test_wire.py`. The rule is deliberately *not* "ignore anything
unexpected", because silently ignoring a privilege escalation is how the peer ends up believing
it succeeded — which is precisely the failure class §9 exists to hunt. The one exception is
`relay_to`, and it is conditional: only a peer the host has designated as hub may set it, and
even then `relayed_by` is *derived* by the host from the peer's identity, never read off the
wire.

`to` given as a bare string is **refused, not split**. `tuple("audit")` is `('a','u','d','i','t')`
— five ghost recipients, and the one agent who should have received the message gets nothing.
No exception, no log, just "they never got back to me". Pitfall #11, now on the wire.

### What the demo actually proves

`run_wire_demo.py` does not print "it worked". It runs the incident-triage scenario **twice** —
once all-Python, once with the **`deploy` member replaced by a Node process** — and then compares
them item by item:

```
[✓] final report identical          [✓] both runs actually completed
[✓] round count identical           [✓] no faults in either run
[✓] data-message count identical    [✓] topology evolution identical
[✓] fidelity identical              [✓] message sequence identical, line by line
```

`deploy` was not picked at random. In this scenario it does exactly one thing: when asked about
`release_meta`, it answers. The Node reference implementation does exactly that and nothing more,
so the two are **behaviourally equivalent** — any difference can only come from "it went over a
socket", never from "it did a different job". Swapping `dba` instead would not work: `dba` has to
follow a chain of inference, and the reference implementation deliberately contains no reasoning.
Comparing those two would measure a difference in *decision-making* rather than in *communication*,
which would prove nothing.

Two smaller decisions worth naming. First, the transport layer uses **no threads at all** — the
host drives and every round-trip is synchronous, which keeps the "no concurrency, races untested"
boundary in §5 exactly where it was instead of quietly building on top of it. Second, CI runs the
interop suite as its own job with `PEER_AGENTS_REQUIRE_NODE=1`, so a missing node is a **failure**
rather than a skip. A skipped interop test is an interop test that stopped existing.

---

## 8. Roadmap

- [x] **A wire protocol, so agents can be written in other languages** — see §7
- [ ] **A third language implementation** (Go / Rust / anything), wired into `tests/test_interop.py`
      so "any language can join" stops being an inference and becomes a verified fact
- [ ] **Peer authentication**, so the wire layer is no longer loopback-only
- [ ] **Many peers per session** (today the host's `accept()` is synchronous and handles one)
- [ ] **Get a real LLM running end-to-end**, and compare it against the rule brain (highest priority)
- [ ] **Replace `DirtyBrain` with an LLM brain that reads `Fact` metadata**: today's "skepticism" is a hard-coded rule, and I want to see whether a real model will compare confidence and timestamps on its own
- [ ] Link-value estimation: replace LRU with "communication frequency × information gain," so topology optimization becomes more proactive
- [ ] Cross-session topology memory: right now the topology regrows from scratch on every run, which barely counts as memory
- [ ] An asyncio concurrent version, plus race tests under concurrency. Note this is about the
      *scheduler*: the wire layer was deliberately kept synchronous and thread-free so that this
      work has exactly one place to happen
- [ ] Introduce trust and reputation, so that "who is worth connecting to" becomes a dynamic game
- [ ] Let information **decay naturally** over time (rather than via a preset old timestamp), and watch how a team re-reaches consensus
- [ ] Larger-scale stress tests for convergence and routing overhead

---

## 9. Known pitfalls (scars earned)

Left here for whoever comes next. Every one of these is the kind of bug that makes a system **fail silently** — no exception, no error, just a result that quietly goes wrong.

**Infrastructure layer**

1. **`relayed_by == hub` degenerates to `None == None` — vacuously true — when `hub is None`**, causing every message to bypass the topology and go direct, which nullifies dynamic topology entirely. It must be written as `self.hub is not None and msg.relayed_by == self.hub`.
2. **The convergence check forgot the pending queue.** Originally `active == 0` alone declared quiescence, so messages waiting on link negotiation were quietly abandoned at the moment of silence, and final-report fidelity dropped straight to 0.
3. **Link requests lost their original semantics.** `LINK_REQUEST` needs to carry `origin_kind`; without it the receiver cannot tell "this is a business question" from "this is reporting a conclusion," and rejects connections it should have accepted.
4. **Putting "persisting the deliverable" into the brain instead of the framework.** The rule brain happened to write its conclusion to the blackboard; the LLM brain only "speaks." Swap in the LLM and the final report vanishes into thin air (fidelity 0%) — while the message flow looks perfectly normal. Lesson: **a brain decides *what to say*; only the framework decides *what to keep*.** 
5. **The dedup signature omitted `attempt`, so a retry was treated as a duplicate.** A retry — "asked, got no answer, ask again later" — has a body identical to the first request character for character, so dedup swallowed it outright. The symptom: "the system is very quiet, but the problem isn't solved." Dedup should catch *a message looping back around*, not *me trying again*.
6. **An absolute-scale freshness model lies to itself.** Originally `as_of=0` meant "now" and `now` was the round number, so by round 60 even freshly produced information counted as stale. Changed to `as_of=None` meaning live, **stamped to an absolute scale only at the moment it goes out on the wire** — a statement about "current conditions" has no meaningful timestamp until the moment it's spoken.

**Cognition layer**

7. **A dead zone between "silence detection" and "retry wait."** This is the subtlest one. `has_pending_work` originally returned `True` only once "2 rounds have passed since the last question, so retry is allowed." But during the waiting round the agent isn't woken at all, so the runtime sees `active == 0`, declares convergence, and calls it a day — **the system declared itself finished before the retry window even opened.** Lesson: test for "is there an unresolved question?" rather than "is now the right moment to retry?" **"Still owing something" is itself a reason not to converge.**
8. **An empty mailbox should not stop the brain.** `Agent.step` originally returned `[]` immediately when `batch` was empty, while the retry logic lives in `think()`. So the scheduler woke the agent because "the brain says it still has work," and `step` — seeing an empty mailbox — sent it straight back to sleep. `think` was never called at all. Two modules, each individually "correct," broken the moment you plug them together.
9. **The hub role must actually forward.** `DirtyBrain` initially didn't handle `relay_to`, so messages blocked by the topology and handed to the hub **silently disappeared at the hub.** From the outside you only see "downstream isn't getting the information," and it is extremely hard to trace back to "the relay ate the pass-through message."
10. **`dict.get(key, [])` hands you a detached empty list.** Using its length as an attempt number yields a non-contiguous sequence like `0, 2, 3` (the first attempt reads as 0 rather than 1). Take the list that `setdefault` actually stored instead.

**Boundaries and permissions**

11. **No type validation on model output — `"to": "audit"` got split into a sequence of characters.** `tuple("audit")` yields `('a','u','d','i','t')`, and the message was delivered to five non-existent recipients, with **the real `audit` receiving nothing at all.** No exception, no log; it just looks like "they didn't answer me." This class of silent misdelivery is far harder to trace than a crash.
12. **The model could forge control-plane messages and rewrite the topology past the framework.** With no whitelist on `kind`, a model only needed to emit `{"kind":"LINK_REQUEST"}` to trigger the bus's link negotiation and successfully establish a link that should never have existed. **The sentence "the model decides whom to talk to and what to say, but cannot modify the framework" has to be enforced by code, not by writing "please don't" in the prompt.** The fix guards two places: normalization in the LLM parse layer, plus the bus refusing control-plane messages in `send()`.
13. **`assert` is not error handling.** `_deliver_control` originally read `assert self.topology is not None`, and `assert` is stripped entirely under `python -O` — turning a branch that should have exited gracefully into a landmine that only detonates in production. If you mean to return early, return early.

**Engineering (making it trustworthy)**

14. **A re-entrant runtime equals fake success.** `Runtime.run()` could once be called a second time: the agents' brains, the blackboard, the bus, and the topology are all stateful, and a second run produced "round 1, message count copied" — **no exception, every number present, and nothing actually flowing** (dedup ate it all). This class of fake success is far harder to detect than a crash. The fix is a `_ran` guard whose error message tells you to `build()` a new instance.
15. **"Didn't close out" must not be left for the caller to guess.** When requirements could never be satisfied, the framework used to quietly emit an empty report that you could only detect via `final_report == ""` — and an empty string could mean failure *or* simply "hasn't got there yet." Results now carry `completed` / `exit_reason`, plus a `describe_outcome()` that states plainly whether it closed out and, if not, where it stalled.
16. **"Still owes something" and "crashed" must be reported separately.** Failure is not the same as an exception; likewise, "did nothing" is not the same as a fault. Results describe these on two independent channels — `unresolved` (what premises are missing) and `faults` (who threw what) — so a quiet empty result isn't mistaken for "there was nothing to do."
17. **One member crashing should not take down the whole team.** In a group that communicates autonomously, "this member is unavailable right now" is the normal case. `Agent.process` now sits inside an exception boundary: record the fault, remove the member once consecutive faults hit the threshold, and let everyone else keep running.
18. **In-flight messages must not evaporate.** When a fault occurs, that batch of messages **has already been taken out of the mailbox** — so simply `continue`-ing past it makes them disappear, and "messages vanishing into thin air" is the single hardest class of bug in this project to trace. The fix: route them to the dead-letter queue and report the count in the result.
19. **Unbounded memory is a memory leak.** The dedup table `_seen` originally accumulated without limit — 5,000 messages meant 5,000 resident records. Dedup exists to catch loops *within a short window*, so a sliding window is entirely sufficient — trading **bounded memory** for **no leak**. The cost is that older signatures outside the window get evicted (deliberately: an agent must never be permanently barred from saying the same thing again).
20. **uuid makes a trace reproducible but not diffable.** Same content, different IDs, and you cannot use `diff` to find which message two runs diverged on. Changed to a process-local incrementing deterministic ID (starting at `m00000`), zeroed when a `Runtime` is constructed — and the zeroing point must precede **the first message this experiment produces**, which is why it lives in `__init__` rather than `run()` (the kickoff happens before `run()`).

**Building on top of the thing you said you had not verified**

21. **A skipped test is a test that stopped existing.** The cross-language interop suite needs node. The obvious implementation — `@skipUnless(node, ...)` — means that on any machine without node the file quietly turns into decoration while the suite still reports green. "We have cross-language tests" and "cross-language has not been verified in months" would look exactly the same from the outside. The fix is an env switch: `PEER_AGENTS_REQUIRE_NODE=1` turns "node is missing" from a skip into a **failure**, and CI sets it. The general lesson is worth more than the specific case: *a conditional test needs something that can fail, or the condition becomes the escape hatch.*
22. **Fault isolation covered `think()` and nothing else.** `Runtime` wraps `agent.process()` in a try/except precisely so that one broken member cannot take down the team. But a brain has two other methods the framework calls — `has_pending_work` and `accepts_link` — and both sat **outside** that boundary. A brain raising in `has_pending_work` would throw straight through `run()` and kill the whole experiment; one raising in `accepts_link` would do the same, since `bus.send()` is called on a line the guard does not reach. The pattern here is subtle: the guard was written for the method that was obvious, and the two optional methods were added later, by a protocol (`AdaptiveBrain`) that *deliberately* does not require every brain to implement them. **A protection that covers "the" entry point usually covers only one of them.** All three call sites are now guarded, and `retry_pending()` — which also re-enters the delivery path — with them.
23. **`TimeoutError` is an `OSError`, and one line of ordering decided whether a hiccup looked like a corpse.** When a peer times out, the connection is fine and the next round may succeed; when a peer's socket errors, the link is gone for good. Those need different responses, so the transport tracks a `broken` flag — set it wrong and a single slow round marks a healthy peer as dead. The bug: `TimeoutError` **subclasses `OSError`** in Python 3.10+, so `except OSError: self.broken = True` swallowed the timeout too. The fix is `except TimeoutError: raise` *before* `except OSError`, and there is now a test asserting that a timeout leaves `broken` false. **Subclass order in an `except` chain is load-bearing, and it reads like formatting.**

---

## License

MIT, see [LICENSE](LICENSE).
