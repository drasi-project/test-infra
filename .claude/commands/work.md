# Work Command

Execute a work plan from the .plans folder.

## Usage
/work [filename]

Note: If filename is not provided, the command will attempt to use the currently open plan file (if it's in the .plans directory).

## Instructions

1. Determine which plan file to use:
   - If filename is provided: use `.plans/<filename>.md`
   - If no filename provided:
     - Check if there's a currently open file in the `.plans` directory
     - If no open plan file found, list available plans in `.plans` and ask user to specify

2. Ensure the `.plans` directory exists:
   - Check if `.plans` exists in the current directory
   - If not, inform user to create a plan first using `/plan <filename>`

3. Read the work plan file:
   - If file doesn't exist, suggest using `/plan <filename>` first
   - Validate the plan follows the expected structure

4. Parse the plan structure:
   - Extract overview section for context
   - Parse all tasks from "## Tasks" section
   - Extract execution order from "## Execution Order" section
   - Note any dependencies and special considerations from "## Notes"

5. Validate plan format:
   - Ensure required sections exist (Overview, Tasks, Execution Order)
   - Verify each task has Description, Actions, and Success Criteria
   - Handle missing or malformed sections gracefully

6. Use the TodoWrite tool to create a task list from the plan:
   - Create one todo item for each task in the plan
   - Set priorities based on execution order

7. Execute each task in the specified order:
   - Show progress: "Starting Task X of Y: <Task Name>"
   - Mark the current task as "in_progress"
   - Perform all actions listed for the task
   - Verify success criteria are met
   - Mark the task as "completed"
   - Display completion percentage
   - Move to the next task

8. Handle any errors or blockers:
   - If a task cannot be completed, keep it as "in_progress"
   - Create a new task describing what needs to be resolved
   - Ask the user for guidance if needed

9. Provide regular status updates:
   - After each task: "✓ Completed: <Task Name> (X/Y tasks done - XX%)"
   - Show any important outputs or results

10. When all tasks are complete, provide a summary:
   - List all completed tasks
   - Note any tasks that remain in_progress
   - Summarize key outcomes

## Execution Guidelines
- Follow the exact actions specified in the plan
- Use all available tools as needed (Read, Write, Edit, Bash, etc.)
- Test and verify each step before marking it complete
- If the plan references specific files or commands, execute them exactly as written
- Maintain the context from the original plan throughout execution
- Respect task dependencies - don't start a task if its dependencies aren't complete