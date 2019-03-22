# RFCs for Heroic

This document outlines how to write and work with RFCs for Heroic.

RFCs for Heroic are design documents for bigger changes and features in Heroic. RFCs are used for
discussing these changes before making bigger architectural changes, and to document decisions;
both accepted and rejected.

## Structure and content

All RFCs should be opened as issues tagged with `type:rfc` and an initial status of `rfc:draft`. The name of the issue should
`RFC: -<title>`.

RFC documents should be in valid Markdown and contain a short and descriptive title and a short
description of the problem the RFC tried to address. Any related issues should be linked to. Discussion around the RFC and proposed solution should live in the RFC and not the linked issues.

Once an RFC is accepted it should be tagged with `note:accepted`. If it is rejected, it should be tagged with `note:wontfix`. RFCs that are either implemented (with at least one linked PR) or rejected must be closed out.

### Template

```
# Problem
<explanation of what this RFC will solve>

# Suggestion
<how to solve the problem, example code welcome>

# Related issues
<optional list of issues affected by this RFC>
```
