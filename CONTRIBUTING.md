# 🛠️ Git Contribution & Commit Guidelines

To keep the project history clean, structured, and readable for both humans and automated tools, this repository strictly follows a semantic naming convention combined with a **Squash-and-Merge** workflow.

---

## 📊 Document & Roadmap Task Statuses
When updating the project roadmap (`roadmap.md`), architectural decision logs, or tracking documentation, always prefix tasks with one of the following universal visual anchors to ensure immediate scannability:

* **`Done ✅`** — The task is fully implemented, verified via tests, merged into `main`, and meets the Definition of Done.
* **`Pending ⏳`** — The task is either in active development, awaiting code review, or currently sitting in the backlog queue.
* **`Cancelled ❌`** — The task was evaluated, but explicitly dropped or decoupled due to scope adjustments, architectural redesigns, or MVP constraints.

---

## 🔀 Branching Strategy
* Always create a dedicated branch for your tasks.
* Use explicit, short, and descriptive task-based naming conventions using hyphens:
  * `feature/accounting-balance-topup`
  * `feature/orchestrator-message-idempotency`
  * `test/core-domain-testing`
  * `fix/compliance-unique-constraint`

---

## 📌 Branch Development (Internal Commits)
While working inside your isolated feature or test branch, you are completely free to use any convenient local commit message format (e.g., `added logging`, `wip context verification`). These internal messages will be squashed during the Pull Request merge process and will not pollute the main repository history.

---

## 🌟 Final Commit Specification (Squash & Merge)

To keep the main branch history clean and self-documenting, we focus strictly on semantic context based on the application architecture. When merging your branch into `main` via a Pull Request, you **must squash** all intermediate commits into a single final message matching this exact format:

```text
<scope>: <Short lowercase description of what was achieved>
```

The `<scope>` value must map directly to the feature context or service name (e.g., `accounting`, `compliance`, `ledger`, `orchestrator`, `gateway`, `infra`, `test`).

### 🧬 Examples of Clean Production Commits:
* `orchestrator: implement asynchronous stateful saga execution`
* `accounting: add secure environment-gated bulk data seeding API`
* `compliance: refactor custom sql execution to reuse db connection`
* `test: establish foundational unit and local database integration tests`
* `infra: configure pure yarp api gateway forwarding filters`

---

## 🔗 Automated Issue Linking (GitHub Magic Keywords)
To prevent tracking synchronization issues and keep contributors free from manual overhead, **do not hardcode Issue numbers into the Git commit messages.** 

Instead, leverage GitHub's automatic tracking linkage by adding a special keyword inside the **Pull Request Description** text box before merging.

### How to Link Your Work:
When creating a Pull Request, explicitly state which backlog task it addresses in the description layout:
```text
Closes #12
```
*Supported keywords include: `Closes`, `Fixes`, `Resolves`.*

### Why We Use This Pattern:
1. **Zero Friction for External Contributors:** Outside developers don't need to know or guess internal issue sequences to submit a valid Pull Request.
2. **Automated Lifecycle Management:** GitHub will automatically close the linked Issue and inject cross-reference hyperlinks into both the issue timeline and the final squash commit history (appending `(#PR_NUMBER)` automatically).
