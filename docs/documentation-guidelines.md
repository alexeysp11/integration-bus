# Documentation Guidelines

This document establishes strict constraints for AI models when creating, updating, or refactoring project documentation. Follow these guidelines to ensure production-ready quality.

---

### 1. Present-State Focus (No Historical Narratives)
* **Rule:** Describe only how the system works **right now**. 
* **Constraint:** Do not include historical context, implementation logs, or development timelines. 
* **Prohibited:** Phrases like *"First we tried Option A, but faced performance issues during load testing, so we switched to Option B"* are strictly forbidden. Present only the final, approved, and active architectural decision.

### 2. Engineering Objectivity (No Subjectivity or Chat Artifacts)
* **Rule:** Write in a formal, technical, and authoritative engineering style. 
* **Constraint:** Eliminate all internal reflections, thoughts, and conversational filler. 
* **Prohibited:** Phrases expressing uncertainty or colloquialisms (e.g., *"It seems to me there might be an issue..."*, *"This looks messy..."*, *"We think that..."*) must be completely removed. The text must read as an official reference manual, not a personal blog or chat response.

### 3. Strict Relative Linking
* **Rule:** Every cross-link to another file within the repository must use **strict relative paths** calculated from the current file's location.
* **Constraint:** Never use absolute paths, temporary local machine paths, or Windows-style backslashes (`\`). Use only standard forward slashes (`/`).

### 4. No Broken or Ghost References
* **Rule:** Do not reference documents, architectural diagrams, roadmaps, or configuration files that do not physically exist in the current repository structure. 
* **Constraint:** If information about a referenced file is missing or obsolete, remove the link or replace it with a valid destination. Always verify the current repository tree before generating links.

### 5. Strict Internationalization & Language Separation
* **Rule:** Maintain clean boundaries between localized versions of documentation.
  * English master files must use a plain `.md` extension (e.g., `README.md`).
  * Russian localized files must strictly use the `.ru.md` suffix before the extension (e.g., `README.ru.md`).
  * Bi-directional language links must be placed at the top of paired files for quick switching (e.g., `[English](README.md) | [Русский](README.ru.md)`).
* **Cross-Language Constraint & Exceptions:**
  * **English Files:** An English file (`.md`) must **never** link to a Russian (`.ru.md`) version of any document. If the target document only exists in Russian, you must first create its English version, and then link to it.
  * **Russian Files:** A Russian file (`.ru.md`) must link to the Russian version of the target document. 
  * **The Fallback Exception:** If a Russian file needs to link to a document that **does not have a Russian translation**, it is officially allowed to link to the English (`.md`) version of that document. However, if a Russian translation exists, linking to the English version is a violation.

### 6. Actionable Troubleshooting Over Bug History
* **Rule:** If edge cases, failures, or critical bugs discovered during system testing need to be documented, do not present them as a retrospective story of past errors.
* **Constraint:** Convert all past issues into **"Troubleshooting"**, **"Resilience Configuration"**, or **"Runbooks"** sections. Provide clear, actionable recipes, bash commands, or configuration snippets showing how the user can prevent or resolve these conditions in a production environment.
