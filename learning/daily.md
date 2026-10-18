# Daily template

Sequence matters more than clock times. Shift anchors freely.

| Block | Hours | Content | Why this order |
|---|---|---|---|
| **Block 1 — concepts** (deep) | 2h | New material: Kafka now, thesis papers later | Hardest cognitive work first, on a fresh focus budget |
| **Real break** | — | Walk, food, no screens | Consolidation is literal — the brain files Block 1 here |
| **Block 2 — build** (deep) | 2h | mini-PeerDB, low AI | Retention cheat code: Block 1's concepts get applied hours later, same day |
| **Light block** | 2h | ~60–90 min blog writing + AI-assisted setup (docker, benchmark harness, repo hygiene) | Drafting only here. Deep blocks never get stolen by the blog |

## Session endings

- **Block 1 ends:** 10–15 min brain dump, at the desk. Copy `dumps/TEMPLATE.md`.
- **Block 2 ends:** 5-min dump — what broke, next step.
- **Every session ends:** write the exact next action before closing the
  laptop. Startup cost tomorrow drops to near zero.

## Efficiency rules

1. **Question-first entry.** Never open a source without writing the question
   you're answering. Reading with a question is filtering; without one, hoarding.
2. **One source per session.** Everything else is lookup, triggered by a
   specific question.
3. **Dump before you stand up.** Retrieval within minutes of learning is worth
   several times retrieval the next day. Non-negotiable.
4. **Same-day application.** Block 1's material must be touched by Block 2.
   At concept level: through the build. At API level (learning a library):
   through the drills — see "API-drill mode" below. Can't apply it? Gap list.
5. **AI rule.** AI scaffolds the periphery. The core path (replication loop,
   batching, offset handling) is typed and explainable by you.
6. **Tired → downgrade the task, not the slot.** Swap Block 1 for light work
   (run benchmarks, read logs, tidy notes). Never skip the slot entirely.
7. **Stop at a written next step.**

## API-drill mode (when learning a library, e.g. franz-go)

Greenfield building is not the same-day application at this stage. The ladder is:

| Step | Drill | Notes |
|---|---|---|
| 1 | **Read** the example | Understand what and why |
| 2 | **Blank re-code** | Close everything, reproduce from memory, diff against the original. **The diff IS the lesson.** ~10 min |
| 3 | **Modify** | Change one behavior: manual commit, print lag, handle rebalance callback |
| 4 | **Compose** | Combine two+ examples into a program no single example covers (lag-watcher, at-least-once round-trip, transactional flow) |
| 5 | **Build** | The composed program becomes part of mini-PeerDB |

Distinguish: **inventable knowledge** (concepts — must be retained) vs
**lookup knowledge** (API names — looked up on demand, godoc is fine).
Never blame yourself for not recalling lookup knowledge; only for not being
able to *use* it after seeing it. Read the package's godoc top comment once
for the map; after that, knowing the question is the skill, not the answer.
