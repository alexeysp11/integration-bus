# 🛠️ Integration Bus: C# Development & Testing Standards

This document establishes immutable engineering guidelines and code generation constraints for the `integration-bus` project. All components must strictly adhere to these practices to ensure codebase predictability, compiler optimization, and high maintainability.

---

## 💻 1. Core C# Architecture & Style Guidelines

### Language & Documentation
* **English Only:** All source code entities—including namespaces, classes, methods, fields, local variables, and inline comments—must be written exclusively in English.

### Type Declarations & Inheritance Control
* **Immutable Inheritance Control:** Every class or record that does not intentionally serve as a base entity for inheritance **must** be declared with the `sealed` modifier. This optimizes JIT compilation, enables devirtualization, and prevents architectural degradation.
* **Explicit Typing Over `var`:** Avoid the `var` keyword for local variable declarations. Explicitly declare target types combined with target-typed `new()` expressions where possible.
* **Namespace Cleanliness:** Avoid using fully qualified namespace prefixes within execution scopes (e.g., use `SimpleMessageScreen` instead of `Project.Domain.SimpleMessageScreen`).

```csharp
// ❌ WRONG
var manager = new TransactionManager();
var screen = new Project.UI.SimpleMessageScreen();

//  RIGHT
TransactionManager manager = new();
SimpleMessageScreen screen = new();
```

### Collections & Array Initialization
* **Collection Expressions:** Prefer using modern C# collection expressions (square brackets `[]`) for initializing arrays, lists, spans, and collections over verbose traditional initializers.

```csharp
// ❌ WRONG
List<string> topics = new List<string> { "balance", "ledger" };
int[] codes = new int[] { 200, 400 };

//  RIGHT
List<string> topics = ["balance", "ledger"];
int[] codes =;
```

### Inline Comments & Refactoring Safety
* **No Step Numbering:** Do not use incremental numeric prefixes (e.g., `// 1. Load`, `// 2. Process`) inside inline code comments. Numbered comments break during refactoring and introduce cognitive friction. Use clean, descriptive narrative comments instead.

```csharp
// ❌ WRONG
// 1. Load the transaction entity from PostgreSQL
// 2. Mutate cache state inside Redis cluster

//  RIGHT
// Load the transaction entity from PostgreSQL
// Mutate cache state inside Redis cluster
```

---

## 🧪 2. Automated Testing Ecosystem & Architecture

### Technology Stack
All test suites must be built using the following unified ecosystem:
* **Framework:** `xunit`
* **Assertions:** `FluentAssertions`
* **Mocking:** `Moq`
* **Data Generation:** `AutoFixture`
* **Infrastructure Orchestration:** `Testcontainers`

### FluentAssertions Style & Chaining Rules
* **Mandatory Explanations:** Every assertion method must include the `because` parameter explaining the business or technical rationale behind the check.
* **Multi-Line Formatting:** Chain verification steps on separate lines to maximize scanability.
* **Fluid Assertion Chaining:** Leverage the `.And` constraint prefix to perform multiple validations against a single structural object inside one unified execution flow.

```csharp
// ❌ WRONG
deserializedEditControl.Command.Should().NotBeNull("because polymorphic MongoDB mapping should correctly bind embedded nested commands objects");
deserializedEditControl.Command.Should().BeOfType<CancelTasksCommand>("because MongoDB type discriminators must resolve concrete plugin command entities");

//  RIGHT
deserializedEditControl.Command
    .Should()
    .NotBeNull("because polymorphic MongoDB mapping should correctly bind embedded nested commands objects")
    .And.BeOfType<CancelTasksCommand>("because MongoDB type discriminators must resolve concrete plugin command entities");
```
