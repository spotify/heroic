---
type_name: ShardedResultGroup
fields:
- required: true
  name: "type"
  type_json: '"points"|"events"|"spreads"|"groups"'
  purpose: Type of the result group.
- required: true
  name: "hash"
  type_json: '&lt;string&gt;'
  purpose: A string uniquely identifying this result group.
- required: true
  name: "shard"
  type_json: '{&lt;string&gt;: &lt;string&gt;}'
  purpose: The shard that the result group came from.
- required: true
  name: "cadence"
  type_json: '&lt;number&gt;'
  purpose: The cadence at which a client can expect samples in this group to adhere to.
- required: true
  name: "values"
  type_json: '[[&lt;timestamp&gt;, &lt;sample&gt;], ..]'
  purpose: |
    A list of values of the given <code>type</code>.
    <ul>
      <li>
        <code class="language-json">"series"</code> indicates that the
        <code class="language-json">&lt;sample&gt;</code> is a <code class="language-json">&lt;number&gt;</code>.
      </li>
      <li>
        <code class="language-json">"events"</code> indicates that the
        <code class="language-json">&lt;sample&gt;</code> is an <code class="language-json">{&lt;string&gt;: &lt;any&gt;}</code> object.
      </li>
    </ul>
- required: true
  name: "tags"
  type_json: '{&lt;string&gt;: &lt;string&gt;}'
  purpose: The set of tags that have a single value.
- required: true
  name: "tagCounts"
  type_json: '{&lt;string&gt;: &lt;number&gt;}'
  purpose: The set of tags that have more than a single value, the number is the number of distinct tags.
---
Java Class: {% include github-link.html module='heroic-component' name='metric.ShardedResultGroup' %}
