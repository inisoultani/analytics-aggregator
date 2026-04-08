# Engineering Standard: Testing Workflow & SDLC Integration

Welcome to the team! This document outlines our standard operating procedure for designing, writing, and reviewing tests. 

**Our Philosophy:** We do not enforce strict Test-Driven Development (TDD) because it can slow you down. However, we do enforce **Test-Driven Design**. We believe that defining *what* you are going to test before you code is non-negotiable, but *writing* the test should be fast, automated, and frictionless. 

This guide covers the end-to-end workflow from picking up a Linear ticket to merging your Pull Request.

---

## Phase 1: Planning & The "Technical AC"

Product Managers write requirements based on user or system outcomes (Behavioral). Engineers must translate these into technical edge cases (Unit). 

Before writing a single line of code, you must define the **Technical Acceptance Criteria (AC)**.

### Step-by-Step Workflow in Linear:
1.  **Read the Product AC:** Understand the behavioral goal.
    * *Example Product AC:* "The pipeline must enrich the event with IP data and save it to the database."
2.  **Define Technical Sub-Tasks:** Create bullet points or sub-tasks in Linear mapping the Product AC to technical edge cases. These will become your Unit Tests.
    * *Technical AC 1 (Happy Path):* Given a valid IP, mock the external API to return success, ensure event is passed to the batch channel.
    * *Technical AC 2 (Timeout):* Given the external API times out 4 times, ensure the event is routed to the Dead Letter Channel (DLC) with reason `max_retries`.
    * *Technical AC 3 (Backpressure):* Given the worker pool channel is saturated, ensure the assignment context cancels properly.
    * *Technical AC 4 (Panic Recovery):* Given the external enricher panics, recover gracefully and route to DLC.

**Rule of Thumb:** If you cannot list your technical edge cases in plain English on the ticket, you are not ready to write code.

---

## Phase 2: Skeleton Generation & Development

Do not write test boilerplate by hand. Let the tooling do the heavy lifting so you can focus on the logic.

### 1. Write the Function Signature
Draft the empty implementation of your function first.
```go
func (w *EnricherWorker) DataEnricherProcess(ctx context.Context, wg *sync.WaitGroup) {
    // implementation pending
}
```

### 2. Generate the Test Skeleton
Use your IDE's built-in tools (like `gotests` in VSCode or GoLand) or an AI assistant (GitHub Copilot/Gemini) to generate the table-driven test skeleton based on your Technical AC from Phase 1.

**Prompting Copilot/Gemini:**
> *"Generate a table-driven test skeleton in Go for `DataEnricherProcess`. Include these 4 test cases in the table: [Paste your 4 Technical ACs]. Create a mock struct for the `DataEnricher` interface."*

### 3. Open a "WIP" Pull Request (Optional but Recommended)
If your logic is highly complex, push your generated test skeleton and open a Draft PR immediately. Tag your Tech Lead or a peer. It takes them 30 seconds to review a list of test case names and tell you if you missed an architectural edge case *before* you spend 3 days coding.

---

## Phase 3: Writing the Tests (Our Standards)

We strictly separate Behavioral (Integration) Tests from Unit Tests.

### A. Unit Tests (Internal State & Logic)
Unit tests verify the internal state machine: channels, context cancellations, retries, and math. 
* **Format:** Strictly Table-Driven Tests (`tests := []struct{}`).
* **Mocks:** Use handwritten interface mocks or `testify/mock`. Define the interface where it is *consumed*, not where it is implemented.
* **Assertions:** Use `testify/require` for fatal checks (e.g., `require.NoError`) and `testify/assert` for equality checks.

**The Unit Test Standard Example:**
```go
func TestEnricherWorker_DataEnricherProcess(t *testing.T) {
	tests := []struct {
		name              string
		inputIP           string
		setupMock         func(m *MockEnricher)
		cancelAfter       time.Duration 
		expectedErrReason string
		expectInBatch     bool
	}{
		{
			name:    "Happy Path - Enriches and batches", // Maps to Tech AC 1
			inputIP: "1.2.3.4",
			setupMock: func(m *MockEnricher) {
				m.On("EnrichIp", mock.Anything, "1.2.3.4").Return([]byte(`{"ok":true}`), nil).Once()
			},
			expectInBatch: true,
		},
		{
			name:    "Timeout - Max Retries Reached", // Maps to Tech AC 2
			inputIP: "10.0.0.1",
			setupMock: func(m *MockEnricher) {
				m.On("EnrichIp", mock.Anything, "10.0.0.1").Return(nil, errors.New("timeout")).Times(4)
			},
			expectedErrReason: "EnrichWithRetry_reach_max_attempt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. Arrange
			me := new(MockEnricher)
			tt.setupMock(me)
			// ... setup worker and channels ...

			// 2. Act
			go worker.DataEnricherProcess(ctx, &wg)
            // ... trigger context cancellation or send events based on tt config ...

			// 3. Assert
			if tt.expectInBatch {
                // assert event is in batchChan
			}
			if tt.expectedErrReason != "" {
				// assert event is in DLC with correct reason
			}
			me.AssertExpectations(t)
		})
	}
}
```

### B. Behavioral / Integration Tests (The Product AC)
Behavioral tests verify that the system actually talks to external dependencies correctly.
* **Scope:** Does the SQL query actually work against Postgres? Does it parse the actual Kafka payload?
* **Rules:** Do not mock the database. Use tools like `Testcontainers` to spin up ephemeral Postgres/Redis/Kafka instances during the test run.
* **Goal:** These tests prove the Product AC is satisfied.

---

## Phase 4: Code Review & Merging

We do not rely on managers to enforce quality; we rely on tooling and peer accountability.

### 1. CI Pipeline Gates
Your PR will be blocked by the CI pipeline if it fails the **Patch Coverage Requirement**. 
* We do not care if the overall repo coverage is 60%.
* We **do** require that *any new lines of code you added in this specific PR* have **>85% test coverage**. If you add a new `if/else` block, it must be executed by a test.

### 2. The PR Template Checklist
When opening a PR, you must complete the testing section of the template. Reviewers are instructed to instantly reject PRs that leave this blank.

**Excerpt from PR Template:**
```markdown
### Testing Strategy
- [ ] I have included Table-Driven Unit Tests for the internal logic.
- [ ] I have included Behavioral/Integration tests for external dependencies (DB, APIs).
- [ ] My PR passes the CI Patch Coverage gate.

**Key Edge Cases Covered:**
1. (e.g., Worker pool channel saturation)
2. (e.g., Invalid JSON payload from external API)
3. ...
```

### 3. Reviewer Responsibilities
When reviewing a peer's code, follow this exact order:
1.  **Read the PR description & Linear ticket.**
2.  **Read the `_test.go` files FIRST.** If the tests do not make sense, or the test table doesn't cover the edge cases defined in the PR description, reject the PR without reviewing the implementation code.
3.  **Review the implementation code.**

---
*By following this workflow, we ensure our code remains resilient, our CI pipelines stay green, and our engineers spend their time solving hard problems—not chasing bugs in production.*