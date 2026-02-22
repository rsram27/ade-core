# VIDEO 1: "I Run 3 AI Agents Every Morning Before Coffee"
## The Autonomous Data Engineer — Episode 1

**Format:** ~12 minutes | Intro on camera → Screen recording + facecam
**Goal:** Hook the audience, show the real workflow, establish credibility and channel tone

---

## INTRO — ON CAMERA (0:00 - 0:40)

> Every morning I open three terminals. Each one connects to a different AI agent session. Each session picks up exactly where I left off yesterday — different project, different platform, different task.
>
> By the time I've had my coffee, my agents have already analyzed a data pipeline, refactored a Databricks notebook, and started mapping a legacy architecture.
>
> I'm not a prompt engineer. I'm a data engineer. And this is how I actually work — every single day.
>
> My name is Roberto, and this is The Autonomous Data Engineer.

*[Title card / intro animation — 3 seconds]*

---

## PART 1 — THE SETUP (0:40 - 2:30)

*[Screen recording: terminal open, clean desktop]*

> Let me show you my actual setup. No slides, no theory — this is what my screen looks like on a Monday morning.
>
> I use the native terminal — not VS Code, not a fancy IDE. Just the terminal. I've found it's more stable for long agent sessions, and honestly, it's all I need.
>
> I have preconfigured profiles that launch Claude Code with the right model, pointed at the right repository. One click and I'm in.
>
> But here's the thing that makes this actually work. The agent doesn't start from scratch every time. It has context. And that's the piece most people are missing when they try to use AI for real data engineering work.

*[Show terminal launching with profile]*

> There are three layers of context that my agent loads:
>
> First — an operations file. Think of it as the strategic brief for each project. What matters, what the architecture looks like at a high level, what the conventions are.
>
> Second — session notes and a backlog. This is the operational memory. What we did yesterday, what's left to do, what decisions were made.
>
> Third — and this is the core — a knowledge graph. I built a framework called ADE that automatically maps entire data platforms — Databricks, Fabric, Power BI, Tableau, legacy systems. Every table, every pipeline, every dependency, connected in a graph that the agent can query through an MCP server.
>
> So when I say "pick up where we left off on the pipeline refactoring" — the agent actually knows what I'm talking about. Not because it memorized our chat. Because it has structured, queryable context about the entire architecture.

---

## PART 2 — TERMINAL ONE: PIPELINE REFACTORING (2:30 - 5:30)

*[Screen recording: first terminal, zoom on commands]*

> Alright. Terminal one. Let's say I'm working on a data platform built on Databricks. There's a pipeline that needs refactoring — the logic is outdated, the structure doesn't follow current conventions, and downstream reports are waiting on this.
>
> I start the session like this:

*[Type/show the conversational prompt]*

> "Pick up the context from yesterday's session. We were refactoring the ingestion process for the sales domain. Today I want to review the entire flow and verify data correctness."
>
> Watch what happens. The agent loads the session notes, checks the knowledge graph for the pipeline structure, understands what was already done, and picks up exactly where we stopped.

*[Show agent working — reading context, analyzing pipeline]*

> Now it's analyzing the pipeline. It sees the source tables, the transformations, the target schema. It knows which notebooks are involved because ADE mapped them.
>
> It starts writing the refactored notebook. New structure, clean naming, proper error handling. I'm watching, but I'm not typing code.

*[Show notebook being created/refactored]*

> This is the part people don't believe until they see it. The agent writes the notebook, and then deploys it directly to Databricks. It has the credentials, it has the API access.
>
> Now — and I want to be honest about this — I typically run the notebook myself. Execution and validation is where I stay in control. The agent is incredibly good at writing and deploying, but the run-and-verify loop on remote clusters can get tricky. So I launch it, I check the output.
>
> That boundary shifts over time. Three months ago I was also writing the code myself. Now I'm just validating the execution. The human role keeps moving upstream.

---

## PART 3 — TERMINAL TWO: POWER BI MODEL UPDATE (5:30 - 8:00)

*[Switch to second terminal]*

> Terminal two. Different task, same morning. This one is about a Power BI semantic model that needs to be updated to support new sales reporting requirements.

*[Type/show the prompt]*

> "Load the context for the reporting workstream. We need to update the data model to support the new sales KPIs. Check what currently exists and propose the changes."
>
> The agent queries ADE's knowledge graph — it knows the current Power BI model structure, the relationships, the measures. It cross-references with the underlying Databricks tables we just refactored in terminal one.

*[Show agent analyzing Power BI model through ADE]*

> It proposes the changes: new calculated columns, updated relationships, new measures for the KPIs. I review the proposal, say "go ahead," and it pushes the updates.
>
> Two things happening in parallel. Two different platforms. One person.

---

## PART 4 — TERMINAL THREE: LEGACY MAPPING (8:00 - 10:00)

*[Switch to third terminal]*

> Terminal three. This is the one that usually takes consultants weeks. Legacy architecture mapping.
>
> Imagine you inherit a platform with components spread across Talend, Cloudera, Tableau, and some custom scripts nobody documented. Your job is to map everything before a migration.

*[Type/show the prompt]*

> "Today's objective: extract the complete mapping of legacy flows. Talend jobs, Cloudera pipelines, Tableau workbooks. Produce a structured Excel with all objects listed by system, including dependencies."
>
> The agent goes to work. It uses ADE's parsers to extract metadata from each platform. It builds the inventory systematically — every job, every table, every report, cross-referenced with lineage information where available.

*[Show agent extracting and mapping]*

> By the time I finish reviewing the Power BI changes in terminal two, terminal three has an Excel file ready with hundreds of objects catalogued across four systems.
>
> This used to be a two-week task for a senior consultant. The agent did the first pass in a couple of hours. I still need to review and validate, but the heavy lifting is done.

---

## PART 5 — THE HONEST PART (10:00 - 11:00)

*[On camera or facecam more prominent]*

> Now let me be real with you, because I think the AI content space has too much hype and not enough honesty.
>
> This workflow is powerful, but it's not magic. Here's what actually happens:
>
> The agents work with high autonomy, but I supervise. Long sessions can lose context — the conversation gets compressed, and the agent starts drifting from the original plan. You need to watch for that.
>
> Execution on remote systems is still the weak point. Writing and deploying code? Very reliable. Running and debugging on live clusters? That's where I step in.
>
> And the whole thing only works because of the context layer. If I dropped Claude Code into a random project with no knowledge graph, no session notes, no operations file — it would be just another chatbot guessing at your architecture.
>
> The context is the product. The agent is the engine. The human is the pilot.

---

## OUTRO — ON CAMERA (11:00 - 12:00)

> That's my morning. Three terminals, three different tasks, three platforms. By lunch, I've moved three projects forward in ways that used to take entire teams entire weeks.
>
> This is what I call autonomous data engineering. Not because the AI works alone — but because with the right context, it works with a level of autonomy that fundamentally changes what one person can deliver.
>
> This channel is about showing you exactly how this works. Real workflows, real tools, real limitations. No hype.
>
> If you work with Databricks, Fabric, Power BI, or any complex data platform — this is going to change how you think about your job.
>
> I'm Roberto. Subscribe if you want to see the next one, where I'll deep dive into how the knowledge graph actually gets built.
>
> ADE is open source — link in the description if you want to try it yourself.
>
> See you next time.

---

## PRODUCTION NOTES

### Demo Environment Setup
- **CRITICAL: Use demo/synthetic data only. No real client names, schemas, or data.**
- Create a demo Databricks workspace with generic project names (e.g., "Acme Corp", "manufacturing demo")
- Generic Power BI model with sample sales data
- Sample legacy components (Talend XML exports, sample Tableau workbooks)
- All table names, column names, and business terms should be generic

### Screen Recording Strategy
- Pre-run the workflows once to know timing and identify best moments
- Record clean versions — the agent sometimes takes time to process; you can speed up wait times in editing
- Zoom in on key moments: the prompt, the agent loading context, the output being created
- Keep terminal font size large enough for YouTube (16pt minimum)

### What to Show vs. What to Say
- SHOW: Terminal, agent working, outputs being created, notebooks, Excel files
- SAY: The concepts, the reasoning, why it matters
- NEVER SHOW: Real credentials, real client data, real project names, real architecture details
- The demo environment should look realistic but be entirely synthetic

### Facecam Moments
- Full camera: Intro (0:00-0:40) and Outro (11:00-12:00)
- Prominent facecam: "The Honest Part" (10:00-11:00)
- Small facecam corner: All screen recording sections

### Pacing
- Speed up agent "thinking" time in editing (nobody wants to watch a spinner)
- Keep transitions between terminals snappy
- The Honest Part should feel slower, more personal — contrast with the fast-paced demo sections

### Music
- Light background music during screen recording (lo-fi or ambient tech)
- No music during on-camera sections
- Subtle transition sounds between terminals

### Thumbnail Concept
- Split screen showing 3 terminals
- Text overlay: "3 AI Agents. Every Morning."
- Your face with a "watching it work" expression
