---
name: docs-checker
description: Check documentation and docstrings for grammar, punctuation, and style errors. Use when the user asks to verify, review, or fix grammar/style in docs, or before committing documentation changes.
allowed-tools: Bash, Read, Edit, Grep, Glob
argument-hint: "[file-or-glob-pattern]"
---

# Documentation Grammar & Style Checker

Check documentation files for grammar, punctuation, and style errors. Minimize changes: only fix actual errors. Do not rewrite or restructure text, do not add content, do not remove blocks of text.

## Selecting files to check

- **With arguments** (`$ARGUMENTS`): check only the specified files or glob patterns.
- **Without arguments**: check `.rst` and `.py` files changed on the current branch vs `main`. Run:
  ```
  git diff main...HEAD --name-only -- '*.rst' '*.py'
  ```
  If there are no changes vs main (e.g., on main itself), ask the user which files to check.

## What to check

Only check **prose text**: skip code blocks, directive parameters, RST markup syntax, and inline code literals. For `.py` files, only check docstrings (triple-quoted strings), not comments or code.

### 1. Contractions (expand to full words)

Find and expand all contractions. Common ones:

| Wrong | Correct |
|-------|---------|
| don't | do not |
| doesn't | does not |
| didn't | did not |
| can't | cannot |
| won't | will not |
| shouldn't | should not |
| wouldn't | would not |
| couldn't | could not |
| isn't | is not |
| aren't | are not |
| wasn't | was not |
| weren't | were not |
| haven't | have not |
| hasn't | has not |
| hadn't | had not |
| it's (it is) | it is |
| let's | let us |
| that's | that is |
| there's | there is |
| we're | we are |
| they're | they are |
| you're | you are |
| I'm | I am |
| we've | we have |
| they've | they have |

### 2. Common misspellings

| Wrong | Correct |
|-------|---------|
| authentification | authentication |
| ceritificate, certifcate | certificate |
| supplimentary | supplementary |
| temporaily | temporarily |
| withing | within |
| exessive | excessive |
| unexistent | nonexistent |
| outcoming | outgoing |
| inbetween | in between |
| loose (meaning fail) | lose |
| Persistant | Persistent |
| Datenshutz | Datenschutz |
| namepsace | namespace |
| resouce | resource |
| occured | occurred |
| succeded | succeeded |
| seperate | separate |
| neccessary | necessary |
| dependancy | dependency |
| compatability | compatibility |

### 3. Incorrect idioms and phrases

| Wrong | Correct |
|-------|---------|
| so as (meaning "as well as") | as well as |
| allows to \<verb\> | allows \<verb\>ing |
| might be not | might not be |
| can be also | can also be |
| diverts from (meaning deviates) | diverges from |
| more on that | moreover |
| not only it is | not only is it |
| does not do any attempts | does not make any attempts |
| cleanup (as a verb) | clean up (verb is two words) |

### 4. Passive voice → active voice

Convert passive to active where it reads more naturally. Use appropriate subjects:

- **Kopf** when referring to the framework itself
- **the operator** when referring to operators built with Kopf
- **the handler** when referring to handler functions
- **the user** or **you** when referring to the developer using Kopf

Examples:
- "It is used for ..." → "Kopf uses it for ..."
- "The state is stored in ..." → "Kopf stores the state in ..."
- "Multiple namespaces can be served" → "The operator can serve multiple namespaces"

Do not force active voice when passive reads more naturally or when the actor is genuinely unknown.

### 5. Articles and agreement

- Check for missing articles (a, an, the) before singular countable nouns
- Check subject-verb agreement ("the handler run" → "the handler runs")
- Check pronoun agreement ("Handlers ... its" → "Handlers ... their")

### 6. Em dashes

Use `---` (triple-minus) for em dashes in RST files. Replace any unicode em dash (`—`, U+2014) or double-minus (`--`) used as an em dash with `---`.

| Wrong | Correct |
|-------|---------|
| `—` (unicode U+2014) | `---` |
| `--` (used as em dash) | `---` |

### 7. RST-specific issues

- Markdown-style links in `.rst` files: `[text](url)` should be `` `text <url>`_ ``
- Incorrect capitalization of abbreviations: "RsT" → "RST", "yaml" → "YAML", "json" → "JSON"

### 8. American English consistency

Use American English spelling when the file already uses it:

| British | American |
|---------|----------|
| behaviour | behavior |
| colour | color |
| synchronised | synchronized |
| capitalised | capitalized |
| organisation | organization |
| favour | favor |
| honour | honor |

## Process

1. Determine which files to check (from arguments or git diff).
2. Read each file fully.
3. Scan prose text (skip code blocks and RST directives) for issues from the checklist above.
4. For each issue found, apply a fix using the Edit tool.
5. After fixing all files, report a summary of changes made per file.

## Important rules

- **Minimize changes.** Only fix actual errors. Do not improve wording that is already correct.
- **Do not touch code blocks**, inline code, directive parameters, or RST/Markdown structural markup.
- **Do not remove or restructure text.** Keep the original meaning and structure intact.
- **Do not add content** such as new paragraphs, comments, docstrings, or annotations.
- **Preserve the author's voice.** Only fix what is objectively wrong or inconsistent.
