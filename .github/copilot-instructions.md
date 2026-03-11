# COPILOT EDITS OPERATIONAL GUIDELINES
                
## PRIME DIRECTIVE
	Avoid working on more than one file at a time.
	Multiple simultaneous edits to a file will cause corruption.
	Be chatting and teach about what you are doing while coding.

## LARGE FILE & COMPLEX CHANGE PROTOCOL

### MANDATORY PLANNING PHASE
	When working with large files (>300 lines) or complex changes:
		1. ALWAYS start by creating a detailed plan BEFORE making any edits
            2. Your plan MUST include:
                   - All functions/sections that need modification
                   - The order in which changes should be applied
                   - Dependencies between changes
                   - Estimated number of separate edits required
                
            3. Format your plan as:
## PROPOSED EDIT PLAN
	Working with: [filename]
	Total planned edits: [number]

### MAKING EDITS
	- Focus on one conceptual change at a time
	- Show clear "before" and "after" snippets when proposing changes
	- Include concise explanations of what changed and why
	- Always check if the edit maintains the project's coding style

### Edit sequence:
	1. [First specific change] - Purpose: [why]
	2. [Second specific change] - Purpose: [why]
	3. Do you approve this plan? I'll proceed with Edit [number] after your confirmation.
	4. WAIT for explicit user confirmation before making ANY edits when user ok edit [number]
            
### EXECUTION PHASE
	- After each individual edit, clearly indicate progress:
		"✅ Completed edit [#] of [total]. Ready for next edit?"
	- If you discover additional needed changes during editing:
	- STOP and update the plan
	- Get approval before continuing

## LANGUAGE & FRAMEWORK SPECIFIC GUIDELINES

### React & TypeScript
	- Prioritize functional components and hooks.
	- Maintain strict typing; avoid `any`.
	- Review all Data Types before they are added to the codebase.
	- Changes to existing datatypes require a review with full context: 
	  Explain why it is happening, what alternatives exist, and why alternatives were rejected.

### Python
	- Follow PEP 8 style strictly.
	- Ensure functions are single-purpose and highly readable.
	- Use descriptive naming and type hinting for all parameters and return types.

## ARCHITECTURAL PRINCIPLES

### Error Handling & Visibility
	- AVOID fallbacks or silent failures (e.g., empty catch blocks or default returns that hide issues).
	- Errors must always be visible. Output a warning or error message, then raise the exception if needed.
	- Do not hide underlying issues with "safe" defaults.

### Logic & Data Integrity
	- Use DRY (Don't Repeat Yourself) principles: unify any code pathways that can be merged.
	- Avoid "Parallel Data": Do not allow two functions/structures to produce or maintain nearly identical data that must stay in sync. Unify the source of truth.
                
### REFACTORING GUIDANCE
	When refactoring large files:
	- Break work into logical, independently functional chunks
	- Ensure each intermediate state maintains functionality
	- Consider temporary duplication as a valid interim step
	- Always indicate the refactoring pattern being applied
                
### RATE LIMIT AVOIDANCE
	- For very large files, suggest splitting changes across multiple sessions
	- Prioritize changes that are logically complete units
	- Always provide clear stopping points
            
## General Requirements
	Use modern technologies as described below for all code suggestions. Prioritize clean, maintainable code with appropriate comments.
            
### Accessibility
	- Ensure compliance with **WCAG 2.1** AA level minimum, AAA whenever feasible.
	- Always suggest:
	- Labels for form fields.
	- Proper **ARIA** roles and attributes.
	- Adequate color contrast.
	- Alternative texts (`alt`, `aria-label`) for media elements.
	- Semantic HTML for clear structure.
	- Tools like **Lighthouse** for audits.
        
## Browser Compatibility
	- Prioritize feature detection (`if ('fetch' in window)` etc.).
        - Support latest two stable releases of major browsers:
	- Firefox, Chrome, Edge, Safari (macOS/iOS)
        - Emphasize progressive enhancement with polyfills or bundlers (e.g., **Babel**, **Vite**)

## Larger change sets
  - For changes that require edits to more than 3 files, or more than 10 edits in total, you MUST have 4 additional subagents review the proposed changes before implementation. THese subagents will have the following personalities:
    - Detail-Oriented Debbie: Focuses on catching small errors and ensuring consistency. 
    - Devil's Advocate Dave: Challenges assumptions and looks for potential edge cases or unintended consequences, ensuring robustness and wants code to be well tested.
    - Big Picture Bob: Evaluates the overall architecture and design implications following the principles outlined above.
    - User-Centric Uma: Assesses changes from the end-user perspective, ensuring usability and accessibility. 