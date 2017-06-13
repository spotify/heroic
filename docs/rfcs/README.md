# RFCs for Heroic

This document outlines how to write and work with RFCs for Heroic.

RFCs for Heroic are design documents for bigger changes and features in Heroic. RFCs are used for
discussing these changes before making bigger architectural changes, and to document decisions;
both accepted and rejected.

## Filename and directory structure

All RFCs should be written as single documents in the /docs/rfcs directory. The filename should 
`<ID>-<title>.md`, where ID is a 4 digit, zero-padded RFC ID and the title is a short lowercase, 
dash-separated title. E.g. `0042-the-meaning-of-life.md`. The ID should not be assigned until the
RFC gets either accepted or rejected; while in draft the ID should be 0000.

## Structure and content
RFC documents should be in valid Markdown and contain a short and descriptive title, a short
description of the problem the RFC tried to address, and a number of state fields.

### State fields

* **Status** Should be one of `Draft`, `Accepted`, `Rejected` or `Implemented`.
  * `Draft` means the RFC is still being discussed.
  * `Accepted` means the RFC has been accepted but not implemented yet.
  * `Rejected` means the RFC was rejected. Note that the RFC stays in the master branch to capture
    discussion and the decision.
  * `Implemented` means that RFC have been fully implemented in the master branch. And RFC can just
    straight from `Draft` to `Implemented`, if it gets fully implemented and the implementation gets
    merged into the master branch. In this case the merge counts as an implicit acceptance, and the
    RFC should be updated immediately. 
* **PR** Link back to the PR discussing this RFC. This field can obviously not be set in the field
commit, but should be set before the PR is merged.
* **Issue** Link to a relevant issue, if such exists. Relevant issues are either issues, which
spawned the RFC or issues which contains a discussion for the RFC. This field is _optional_ and only
meant as a convenience.
* **Implemented in** Link to a commit or branch, which implements this RFC. This field is _optional_
 and only meant as a convenience.

### Document template
Please use the following template for creating new RFCs. Remove any unused state fields.

```markdown
# <title>

<Brief description of the problem, which this RFC tries to solve>

* **Status** Draft
* **PR** <Link back to PR, which works with this RFC>
* **Issue** <Optional link to issue>
* **Implemented in** <Link to branch or commit, which implements this change>

# Suggestion

<Content of the RFC>

```

## Working with RFC
The workflow for working with RFC for Heroic is designed to use PRs and diffs; just like with the
code. The workflow works as follows:

1. Commit the initial RFC in a separate branch with the status `Draft`.
1. Open an PR for merging the RFC into master
1. Use the PR for discussing the RFC. Keep pushing changes to the RFC until it either gets
   _accepted_, _rejected_ or _implemented_.
1. Update the RFC with the new status, and make sure it links back to the PR.
1. Rename the RFC document to have the next available
1. Merge the PR into the master branch.
