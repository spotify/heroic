---
layout: sidebar
---
## {{ page.title }}

The following [fault tree analyses](https://en.wikipedia.org/wiki/Fault_tree_analysis) were done to give an idea of the reliability of different Heroic components. There are many variables that make them up, so much like performance benchmarks they can change drastically depending on the specific setup.

### Fault Tree Analyses

{%- assign ftas = site.fault_tree | sort: 'name' | reverse %}
{%- for item in ftas %}
#### [{{ item.title }}]({{ item.url | relative_url }})
{%- endfor %}
