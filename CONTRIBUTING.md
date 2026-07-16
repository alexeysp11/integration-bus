# 🛠️ Git Contribution & Commit Guidelines

To keep the project history clean, structured, and readable for both humans and automated tools, this repository strictly follows the **Conventional Commits** specification combined with a **Squash-and-Merge** workflow.

---

## 🔀 Branching Strategy
* Always create a dedicated branch for your tasks.
* Use short, descriptive task-based naming conventions:
  * `feature/issue-1` (or simply `issue-1` for minimalist setups).
  * `fix/issue-5`
  * `chore/issue-9`

---

## 📌 Branch Development (Internal Commits)
While working inside your isolated task branch, you are free to use any convenient local commit message format (e.g., `issue-1: added xml comments`). These internal messages will be squashed later and will not pollute the main history.

---

## 🌟 Final Commit Specification (Squash & Merge)

To keep the repository history readable and clean, we avoid over-engineered commit styles and focus strictly on task-based context. When merging your branch into `main` via a Pull Request, squash all intermediate commits into a single message matching this exact minimalist format:

```text
issue-<number>: Short description of what was achieved
```

### 🧬 Examples of Clean Commits:
* `issue-1: setup base mass transit infrastructure`
* `issue-4: implement failure compensation integration tests`
* `issue-5: resolve concurrency race condition via redlock`
* `issue-6: configure debezium cdc and pipeline to clickhouse`

*Note: GitHub will automatically append the Pull Request number `(#PR_NUMBER)` to the end of the commit message upon merging. This automatically links the commit to the code review history.*
