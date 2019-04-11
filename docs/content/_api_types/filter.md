---
type_name: Filter
layout: api-type-structure
fields:
  - name: "and"
    type_json: '["and", &lt;filter&gt;, ...]'
    purpose: |
      A boolean 'and' operation, evaluates to <code class="language-json">true</code> if <em>all</em> of the nested <code class="language-json">&lt;filter&gt;</code>'s are <code class="language-json">true</code>, otherwise <code class="language-json">true</code>.
  - name: "or"
    type_json: '["or", &lt;filter&gt;, ...]'
    purpose: |
      A boolean 'or' operation, evaluates to <code class="language-json">true</code> if <em>any</em> of the nested <code class="language-json">&lt;filter&gt;</code>'s are <code class="language-json">true</code>, otherwise <code class="language-json">false</code>.
  - name: "not"
    type_json: '["not", &lt;filter&gt;]'
    purpose: |
      A boolean 'not' operation, evaluates to <code class="language-json">true</code> if the nested <code class="language-json">&lt;filter&gt;</code> is <code class="language-json">false</code>.
---
Java Class: {% include github-link.html module='heroic-component' name='filter.Filter' %}

The structure of a filter is explained in detail in
the [Query Language](docs/query_language) documentation.
This will only be a brief, syntactical overview.
