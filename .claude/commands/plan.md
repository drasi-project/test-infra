# Plan Command

Create a structured work plan based on the provided prompt and save it to the .plans folder WITHOUT executing any tasks.

## Usage
/plan <filename> <prompt>

Example: `/plan refactor-auth "Refactor the authentication system to use JWT tokens instead of sessions"`

## IMPORTANT: DO NOT EXECUTE ANY TASKS
This command should ONLY create a plan file. Do NOT:
- Execute any code
- Make any changes to the codebase
- Run any commands
- Use any tools except Read (for understanding context) and Write (to save the plan)

## Instructions

1. Parse the provided prompt to understand what needs to be planned
2. Use Read tool to gather context about relevant files/systems if needed
3. Create a comprehensive work plan with clear, actionable tasks based on the prompt
4. ONLY save the plan to `.plans/<filename>.md` with the following structure:

```markdown
# Work Plan: <Title>

## Overview
Brief description of the goal and scope

## Tasks

### 1. <Task Name>
- **Description**: What needs to be done
- **Actions**: 
  - Specific step 1
  - Specific step 2
  - ...
- **Success Criteria**: How to verify completion
- **Dependencies**: Prerequisites or related tasks

### 2. <Next Task>
...

## Execution Order
1. Task 1
2. Task 2 (depends on Task 1)
...

## Notes
- Any special considerations
- Potential challenges
- Required tools or resources
```

5. Ensure the `.plans` directory exists (create it if needed)
6. Save the plan to `.plans/<filename>.md`
7. Respond with: "Plan saved to .plans/<filename>.md" and a brief summary of what was planned

DO NOT:
- Use the TodoWrite tool (that's for the /work command)
- Start working on any tasks
- Execute any part of the plan
- Analyze unrelated context - focus only on the provided prompt

The plan should be written in a way that's clear and executable by Claude Code when using the /work command later.