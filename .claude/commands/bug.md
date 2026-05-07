# Bug Command

Generate a bug report for Claude Code based on the current context.

## Usage
```
/bug <scope>
```

Where `<scope>` describes the area or functionality where the bug occurred (e.g., "file editing", "git operations", "tool usage").

## What this command does:

1. Creates a `.bugs` directory if it doesn't exist
2. Analyzes the current context including:
   - Git status and recent changes
   - Working directory
   - Recent tool usage
   - Error messages or issues in the conversation
3. Generates a bug report using the Claude Code bug template
4. Saves the report to `.bugs/bug-YYYY-MM-DD-HH-MM-SS.md`

## Example:
```
/bug "Edit tool failing with whitespace preservation"
```

This will generate a comprehensive bug report based on the current session context.