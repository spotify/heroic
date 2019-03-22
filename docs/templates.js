angular.module('_pages/docs.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs.ngt',
    '<div class="row">\n' +
    '  <div class="col-md-3">\n' +
    '    <ul class="nav">\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.overview">Overview</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.getting_started.index">Getting Started</a>\n' +
    '\n' +
    '        <ul class="nav">\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.getting_started.compile">Compile Heroic</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.getting_started.installation">Install Dependencies</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.getting_started.configuration">Configure Cluster</a>\n' +
    '          </li>\n' +
    '        </ul>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.config.index">Configuration</a>\n' +
    '\n' +
    '        <ul class="nav">\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.cluster">Cluster</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.metrics">Metrics</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.metadata">Metadata</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.suggest">Suggestions</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.consumer">Consumers</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.elasticsearch_connection">Elasticsearch Connection</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.shell_server">Shell Server</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.features">Features</a>\n' +
    '          </li>\n' +
    '\n' +
    '          <li ui-sref-active="active">\n' +
    '            <a ui-sref="docs.config.query_logging">Query logging</a>\n' +
    '          </li>\n' +
    '        </ul>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.data_model">Data Model</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.query_language">Query Language</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.api.index">API</a>\n' +
    '\n' +
    '        <ul class="nav endpoints">\n' +
    '          <li ui-sref-active="active" ng-repeat="e in endpoints" class="endpoint-{{e.method | lowercase}}">\n' +
    '            <a ui-sref="docs.api.{{e.sref}}">{{e.method}}: {{e.url}}</a>\n' +
    '          </li>\n' +
    '        </ul>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.aggregations">Aggregations</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.shell">Shell</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.profiles">Profiles</a>\n' +
    '      </li>\n' +
    '\n' +
    '      <li ui-sref-active="active">\n' +
    '        <a ui-sref="docs.federation">Federation</a>\n' +
    '      </li>\n' +
    '    </ul>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="col-md-9" ui-view></div>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_pages/index.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/index.ngt',
    '<div class="jumbotron">\n' +
    '  <div class="container">\n' +
    '    <center><img src="images/logo_on_light.256.png" /></center>\n' +
    '    <p class="lead">The Heroic Time Series Database</p>\n' +
    '  </div>\n' +
    '</div>\n' +
    '\n' +
    '<div class="row">\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.overview">Overview</a></h4>\n' +
    '    <p>\n' +
    '      Start here, the 10k overview of Heroic.\n' +
    '    </p>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.getting_started.index">Getting Started</a></h4>\n' +
    '    <p>\n' +
    '      Land on the ground running.\n' +
    '    </p>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.data_model">Data Model</a></h4>\n' +
    '    <p>\n' +
    '      Dimensional, semantic from the source time series.\n' +
    '    </p>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.query_language">Rich Query and Filtering Language</a></h4>\n' +
    '    <p>\n' +
    '      Leverage multi-dimensional time series using a comprehensive and\n' +
    '      expressive query language.\n' +
    '    </p>\n' +
    '  </div>\n' +
    '</div>\n' +
    '\n' +
    '<div class="row">\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.api.index">API Documentation</a></h4>\n' +
    '    <p>\n' +
    '      Learn how to interact with Heroic\'s API\n' +
    '    </p>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="col-md-3">\n' +
    '    <h4><a ui-sref="docs.aggregations">Aggregations</a></h4>\n' +
    '    <p>\n' +
    '      Learn how to perform powerful aggregations on your data\n' +
    '    </p>\n' +
    '  </div>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_pages/docs/aggregations.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/aggregations.ngt',
    '<h2>Aggregations</h2>\n' +
    '\n' +
    '<p>\n' +
    '  An aggregation is responsible for providing analysis of a larger dataset, to make it more manageable.\n' +
    '  While it would be possible to plot millions of samples in a single graph, it is simply not practical.\n' +
    '  Aggregations give the mean to further aggregate the samples stored in the database.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  An aggregation can be defined either in JSON, or HQL.\n' +
    '  For each aggregation, both forms will be displayed below.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref="{\'#\': \'min\'}">Minimum Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'max\'}">Maximum Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'average\'}">Average Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'sum\'}">Sum Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'chain\'}">Chain Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'group\'}">Group Aggregation</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'filtering\'}">TopK/BottomK/AboveK/BelowK Aggregation</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<h3>Size and Extent</h3>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>size</em> of an aggregation determines the frequency that data occurs in the resulting aggregation.\n' +
    '  So a size of <em>two minutes</em> would cause an aggregation to output a series that has a sample, every two minutes.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>extent</em> of an aggregation determines how wide a single sample will load data in time.\n' +
    '  So an extent of <em>one hour</em> would cause each sample to be the result of aggregating the last hour of data.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Combining <em>size</em> and <em>extent</em>, we now have a flexible system for describing how to build a dataset suitable for plotting.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    'The following graphics represents what data will be sampled to generate the sample at point <code>2</code>.\n' +
    '  <span style="color: #648cff">The blue bar</span> is the <em>extent</em>, and <span style="color: #ff8282;">the red bar</span> is the <em>size</em>.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/size_extent.svg"></img>\n' +
    '\n' +
    '<p>\n' +
    '  The next point we\'ll sample for is <code>3</code>.\n' +
    '  This applies the same principle as above.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/size_extent_3.svg"></img>\n' +
    '\n' +
    '<h3 id="min">Min Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "min", "sampling": {"unit": &lt;unit&gt;, "size": &lt;number&gt;, "extent": &lt;number&gt;}}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'min(size=&lt;duration&gt;, extent=&lt;duration&gt;)\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>Description</h5>\n' +
    '\n' +
    '<p>\n' +
    '  The min aggregation picks the <em>smallest</em> numerical value seen in the given extent.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_min.svg"></img>\n' +
    '\n' +
    '<h3 id="max">Max Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "max", "sampling": {"unit": &lt;unit&gt;, "size": &lt;number&gt;, "extent": &lt;number&gt;}}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'max(size=&lt;duration&gt;, extent=&lt;duration&gt;)\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>Description</h5>\n' +
    '\n' +
    '<p>\n' +
    '  The max aggregation picks the <em>largest</em> numerical value seen in the given extent.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_max.svg"></img>\n' +
    '\n' +
    '<h3 id="average">Average Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "average", "sampling": {"unit": &lt;unit&gt;, "size": &lt;number&gt;, "extent": &lt;number&gt;}}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'average(size=&lt;duration&gt;, extent=&lt;duration&gt;)\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>Description</h5>\n' +
    '\n' +
    '<p>\n' +
    '  The average aggregation takes all samples in a given extent, and calculates the average value over them.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_average.svg"></img>\n' +
    '\n' +
    '<h3 id="sum">Sum Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "sum", "sampling": {"unit": &lt;unit&gt;, "size": &lt;number&gt;, "extent": &lt;number&gt;}}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'sum(size=&lt;duration&gt;, extent=&lt;duration&gt;)\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>Description</h5>\n' +
    '\n' +
    '<p>\n' +
    '  The sum aggregation sums the values of all points in a given extent.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_sum.svg"></img>\n' +
    '\n' +
    '<h3 id="chain">Chain Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "chain", "chain": [&lt;aggregation&gt;, ..]}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    '&lt;aggregation&gt; | ..\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  A chain aggregation applies the specified aggregations in order.\n' +
    '  The result of the first aggregation is fed into the second, and so forth.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="group">Group Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "group", "of": [&lt;string&gt;, ..], "each": &lt;aggregation&gt;}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    '&lt;aggregation&gt; by *\n' +
    '&lt;aggregation&gt; by &lt;string&gt;\n' +
    '&lt;aggregation&gt; by [&lt;string&gt;, ..]\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>Description</h5>\n' +
    '\n' +
    '<p>\n' +
    '  Takes a set of tags, and groups the sources which have the same values for those tags.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The result below is what would happen when grouping the data on the <b>host</b> tag.<br />\n' +
    '  Assume that the red points come from the host <span style="color: #ff0000;"><b>database1</b></span>,\n' +
    '  the green points come from the host <span style="color: #00ff00;"><b>api1</b></span>,\n' +
    '  and that the blue points come from the host <span style="color: #0000ff;"><b>load-balancer3</b></span>\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_group.svg"></img>\n' +
    '\n' +
    '<p>\n' +
    '  Group aggregations are special, they don\'t have a <em>size</em> or <em>extent</em>.\n' +
    '  Instead they can accept a child aggregation, which will be applied to every matching sub-group.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The below example is the <a ui-sref="{\'#\': \'average\'}">average</a> aggregation applied to each group.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/aggregation_group_average.svg"></img>\n' +
    '\n' +
    '<h3 id="filtering">TopK/BottomK/AboveK/BelowK Aggregation</h3>\n' +
    '\n' +
    '<h5>JSON</h5>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{"type": "topk", "k": &lt;number&gt;}\n' +
    '{"type": "bottomk", "k": &lt;number&gt;}\n' +
    '{"type": "abovek", "k": &lt;number&gt;}\n' +
    '{"type": "belowk", "k": &lt;number&gt;}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h5>HQL</h5>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'topk(&lt;number&gt;)\n' +
    'bottomk(&lt;number&gt;)\n' +
    'abovek(&lt;number&gt;)\n' +
    'belowk(&lt;number&gt;)\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  These are a set of filtering aggregations.\n' +
    '  A filtering aggregation reduces the number of result groups according to some\n' +
    '  criteria.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>TopK - Picks the largest time series</li>\n' +
    '  <li>BottomK - Picks the smallest time series</li>\n' +
    '  <li>AboveK - Picks the time series that has values above the given threshold</li>\n' +
    '  <li>BelowK - Picks the time series that has values below the given threshold</li>\n' +
    '</ul>\n' +
    '');
}]);

angular.module('_pages/docs/api.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api.ngt',
    '<h1>API</h1>\n' +
    '\n' +
    '<h2>Endpoints</h2>\n' +
    '\n' +
    '<div ng-repeat="e in endpoints">\n' +
    '  <h4><a ui-sref="^.{{e.sref}}">{{e.method}} {{e.url}}</a></h4>\n' +
    '  <p>{{e.help}}</p>\n' +
    '</div>\n' +
    '\n' +
    '<h2>Types</h2>\n' +
    '\n' +
    '<div ng-repeat="t in types">\n' +
    '  <h4><a ui-sref="^.type-{{t.id}}">{{t.name}}</a></h4>\n' +
    '  <p ng-if="!!t.help">{{t.help}}</p>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_pages/docs/architecture.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/architecture.ngt',
    '');
}]);

angular.module('_pages/docs/config.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config.ngt',
    '<h2>Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  This section will go through the available configuration options in detail.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic is divided up into a few components, each whose configuration will\n' +
    '  affect the service in different ways.\n' +
    '  Each component has its own sub-section, which are the following.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.cluster">Cluster</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section documenta how to configure clustering and federation to support\n' +
    '  global interfaces.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.metrics">Metrics</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section describes how to configure persistent storage of metrics.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.metadata">Metadata</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section documents how metadata backends are configured.\n' +
    '  Metadata backends are responsible for indexing the <em>active</em> time\n' +
    '  series to support filtering.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.suggest">Suggest</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section documents how suggestion backends are configured.\n' +
    '  Suggestions is a feature that provides the user with feedback on which tags\n' +
    '  and time series are available.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.elasticsearch_connection">Elasticsearch Connections</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section documents how common Elasticsearch connections are configured.\n' +
    '  They are typically used in metadata and suggestion backends that uses\n' +
    '  Elasticsearch.\n' +
    '</p>\n' +
    '\n' +
    '<h4><a ui-sref="^.query_logging">Query logging</a></h4>\n' +
    '\n' +
    '<p>\n' +
    '  This section documents how to configure query logging. I.e. detailed logging of internal Heroic state at different stages of query processing.\n' +
    '</p>\n' +
    '\n' +
    '<p class="next">\n' +
    '  In the next section we will talk about the <a ui-sref="^.^.data_model">data model of Heroic</a>.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/data_model.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/data_model.ngt',
    '<h2>Data Model</h2>\n' +
    '\n' +
    '<p>\n' +
    '  A series is identified by <em>key</em>, and a unique set of <em>tags</em>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Keys, tag keys, and tag values can contain any valid unicode string,\n' +
    '  internally they are stored in UTF-8.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A series represents <em>something</em> over time, where <em>something</em> is\n' +
    '  currently either a set of data points, or events.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="data-points">Points</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Java Class:\n' +
    '  <a git-href-java="heroic-component">com.spotify.heroic.metric.Point</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Each data point stores the timestamp at which they were sampled, and the\n' +
    '  value which they carry.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>timestamp</em> is stored as a 64-bit number (long), which\n' +
    '  represents the number of milliseconds since the unix epoch.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>value</em> is stored as a 64-bit floating point number (double).\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A data point is then typically represented as a JSON array, with two elements.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="json">\n' +
    '[&lt;timestamp&gt;, &lt;value&gt;]\n' +
    '</pre></code>\n' +
    '\n' +
    '<h3 id="events">Events</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.Event</a>\n' +
    '</p>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>Experimental</h4>\n' +
    '  <p>\n' +
    '    Events are currently experimental and not supported by the Cassandra\n' +
    '    backend.\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<p>\n' +
    '  Each event stores the <em>timestamp</em> at which they were sampled, and a\n' +
    '  JSON payload (preferrably small).\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>timestamp</em> is stored as a 64-bit number (long), which\n' +
    '  represents the number of milliseconds since the unix epoch.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>payload</em> is a JSON payload stored as an arbitrarily sized\n' +
    '  binary blob.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  An event is typically represented as a JSON array, with two elements.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="json">\n' +
    '[&lt;timestamp&gt;, {&lt;payload&gt;}]\n' +
    '</pre></code>\n' +
    '\n' +
    '<h3 id="series">Semantic Series</h3>\n' +
    '\n' +
    '<p>\n' +
    '  We strongly encourage the concept of semantic series.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The idea behind semantic series is to move away from obscure identifiers and\n' +
    '  introduce metrics that are structured in a way that makes it easier for a\n' +
    '  human and a computer to reason about.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  So a series like the idle cpu utilization for a host could be identified as the\n' +
    '  following.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="json">\n' +
    '{\n' +
    '  "key": "system",\n' +
    '  "tags": {\n' +
    '    "host": "database.example.com",\n' +
    '    "site": "lon",\n' +
    '    "what": "cpu-idle-percentage",\n' +
    '    "system-component": "cpu",\n' +
    '    "cpu-type": "idle",\n' +
    '    "unit": "%"\n' +
    '  }\n' +
    '}\n' +
    '</pre></code>\n' +
    '\n' +
    '<p>\n' +
    '  This can also be represented in a more compact, human readable format as\n' +
    '  below.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="ts">\n' +
    'system { host=database.example.com, site=lon, what=cpu-idle-percentage, ... }\n' +
    '</pre></code>\n' +
    '\n' +
    '<p>\n' +
    '  The need for semantic metrics becomes more apparent when you start to reason\n' +
    '  about <em>how to model</em> series for certain use cases using a traditional,\n' +
    '  hierarchical model.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Assume that the above series was stored in a hierarchical time series\n' +
    '  database like the following.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code>database.example.com.system.cpu-idle-percentage</code></pre>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>\n' +
    '    The lack of <em>keys</em> makes deciphering a hierarchy challenging.\n' +
    '  </li>\n' +
    '  <li>\n' +
    '    The growth in the number of branches in the hierarchy becomes an\n' +
    '    organizational burden.\n' +
    '  </li>\n' +
    '  <li>\n' +
    '    Growth in the number of series limits discovery.\n' +
    '  </li>\n' +
    '  <li>\n' +
    '    The structure of the hierarchy determines <em>how</em> things are discovered.\n' +
    '  </li>\n' +
    '  <li>\n' +
    '    The filtering, or selecting of series is limited (e.g. wildcard).\n' +
    '  </li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  By promoting the use of tags, and a convention over which tags should be used\n' +
    '  <em>how</em>, the problem becomes more manageable.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Instead of a strict hierarchy where discovery and expression is limited, you\n' +
    '  can have a multi-dimensional system that enables strong correlations and\n' +
    '  natural groupings.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Conversely, if a given convention is followed, an administrator learning what\n' +
    '  a specific tag like <code>what</code> <em>means</em> will find it easier to\n' +
    '  navigate unknown contexts where that tag is used.\n' +
    '</p>\n' +
    '\n' +
    '<h3>References</h3>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a href="http://metrics20.org">Metrics 2.0 "An emerging set of conventions, standards and concepts around timeseries metrics metadata" by Dieter Plaetinck</a></li>\n' +
    '</ul>\n' +
    '');
}]);

angular.module('_pages/docs/federation-tail.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/federation-tail.ngt',
    '<p>\n' +
    '  A client querying any heroic node in a federation will cause it to fan out to\n' +
    '  all known <em>shards</em> and merge the result.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/sharding.svg"></img>\n' +
    '\n' +
    '<p>\n' +
    '  Federations tries to be as transparent as possible in the face of problems.\n' +
    '  Each request that fans out to a shard has the potential to fail, preventing that data to become unavailable.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  In the face of errors, successful shards will be returned as normal.\n' +
    '  The failing shards will be specifically reported as such, and it is left to\n' +
    '  the client to decide what to do next.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/errors.svg"></img>\n' +
    '');
}]);

angular.module('_pages/docs/federation.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/federation.ngt',
    '<h2>Federated Clusters</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic has support for federating requests, which allows multiple independent\n' +
    '  clusters to serve clients through a single, global interface.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  In a federated cluster, requests are routed to <em>shards</em>, and each\n' +
    '  shard is responsible for a distinct chunk of the available data.\n' +
    '  If all nodes in a single shard become unavailable, the data for that shard is\n' +
    '  unavailable.\n' +
    '  Shards are identified by a set of tags, two nodes are said to belong to the\n' +
    '  same shard if their cluster tags are identical. You can see more about this\n' +
    '  in the <a ui-sref="^.config.cluster">cluster documentation</a>.\n' +
    '</p>\n' +
    '\n' +
    '<ng-include src="\'_pages/docs/federation-tail.ngt\'"></ng-include>\n' +
    '');
}]);

angular.module('_pages/docs/getting_started.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/getting_started.ngt',
    '<h2>Getting Started</h2>\n' +
    '\n' +
    '<p>\n' +
    '  This section of the documentation will cover how to <a ui-sref="^.compile">compile</a>, <a ui-sref="^.installation">install</a>, and <a ui-sref="^.configuration">configure</a> Heroic.\n' +
    '  It will take us from nothing, to a running <em>experimental</em> system.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic as a project is still in an <b>Alpha</b> stage, use at your own risk,\n' +
    '  and if you find the time to, please <a git-href>contribute</a>.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/index.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/index.ngt',
    '<h2>Documentation</h2>\n' +
    '\n' +
    '<p>\n' +
    '  I don\'t know how to do index pages, just go to the <a ui-sref="docs.overview">next section</a>.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/overview.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/overview.ngt',
    '<h2>Overview</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic is an open-source monitoring system originally built at\n' +
    '  <a href="https://spotify.com">Spotify</a> to address the problems that were\n' +
    '  facing with large scale gathering and near real-time analysis of metrics.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic\'s main features are:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>An architecture that scales with you, large or small.</li>\n' +
    '  <li>Indefinite retention, as long as you have the hardware spend.</li>\n' +
    '  <li>A rich <a ui-sref="^.query_language">query and filtering language</a>, driven by Elasticsearch.</li>\n' +
    '  <li><a ui-sref="^.federation">Federation support</a> to connect multiple Heroic clusters into a global interface.</li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic uses a small set of components which are responsible for very specific things.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Consumers</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Consumers are the component responsible for <em>consuming</em> metrics.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  We support consuming over <a ui-sref="^.config.consumer({\'#\': \'kafka\'})">Kafka</a>, the <a ui-sref="^.config.consumer({\'#\': \'collectd\'})">collectd protocol</a>, and over <a ui-sref="^.api({\'#\': \'post-write\'})">HTTP</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Out of these options, Kafka is the preferred one since it allows for a discovery-free architecture due to it being a pub-sub system.\n' +
    '  It has proven to be horizontally scalable and resilient towards failures.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following tutorials are available for configuring consumers:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref="tutorial.kafka_consumer">Consuming metrics over Kafka</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<h3>Metrics</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Metric storage can be handled by either Google Cloud Bigtable or Apache Cassandra.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a> is a\n' +
    '  fully-managed petabyte-scale NoSQL database. It\'s the same database that\n' +
    '  powers many core Google services, including Search, Analytics, Maps, and\n' +
    '  Gmail. Bigtable has inspired the design of many other NoSQL databases, such as\n' +
    '  Apache HBase and Apache Cassandra.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The technique for storing time series data in Cassandra is described in\n' +
    '  <a href="http://www.datastax.com/dev/blog/advanced-time-series-with-cassandra">Advanced Time Series with Cassandra</a>.\n' +
    '  It was inspired by how it\'s implemented in\n' +
    '  <a href="http://kairosdb.github.io/">KairosDB</a>.\n' +
    '  Using Cassandra, you can store almost an indefinite amount of data as long as\n' +
    '  you are willing to <a href="http://techblog.netflix.com/2011/11/benchmarking-cassandra-scalability-on.html">spend the hardware</a> on it.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  For more information, see the section detailing how to configure <a ui-sref="^.config.metrics">metrics backends</a>.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Metadata</h3>\n' +
    '\n' +
    '<p>\n' +
    '  We use Elasticsearch to store and make metadata available to a heroic\n' +
    '  cluster. It is the primary component that drives Heroic\'s <a ui-sref="^.query_language">Query Language</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Elasticsearch has been <a href="https://aphyr.com/posts/323-call-me-maybe-elasticsearch-1-5-0">shown to not be reliable in terms of data safety</a>.\n' +
    '  Because of this, Heroic uses Elasticsearch in a way so that it is not the primary storage and can rapidly be rebuilt.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  For more information, see the section detailing how to configure <a ui-sref="^.config.metadata">metadata backends</a>.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Suggestions</h3>\n' +
    '\n' +
    '<p>\n' +
    '  When building Heroic it was quickly realized that navigating millions, or hundreads of millions of time series without context is hard.\n' +
    '  To address this, a specialized Elasticsearch backend was built to handle <a ui-sref="^.config.suggest">suggestions</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Suggestions provide information about what tags are available for a specific context.\n' +
    '  Assuming you are interested in which <code>what</code> tags exist for a given role, you could do the following:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ curl -H "Content-Type: application/json" &lt;url&gt;/metadata/suggest-tag -d \\\n' +
    '  \'{"filter": ["=", "role", "heroic"], "key": "what", "value": "us"}\'\n' +
    '</codeblock>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{\n' +
    '  "suggestions": [\n' +
    '    {"score": "1.0", "key": "what", "value": "cpu-usage"},\n' +
    '    {"score": "1.0", "key": "what", "value": "disk-used-percentage"}\n' +
    '  ]\n' +
    '}\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Take note on the partial matching of <code>us</code> in <code>cpu-usage</code> and <code>disk-used-percentage</code>.\n' +
    '  This is the point of suggestions, to provide the user with the most relevant matches for a specific input.\n' +
    '  Especially partial ones.\n' +
    '  A typical use-case would be to fill the content of drop-down box as the user is typing.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Suggestions are different from Metadata since it uses more intensive indexing techniques to analyze your tags.\n' +
    '  Having it as a separate module is useful since they are not critical to the operation of Heroic.\n' +
    '  Or to be more specific, its <a ui-sref="^.query_language">Query Language</a>.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Clustering and Federation</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Main article: <a ui-sref="^.federation">Federated Clusters</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic has support for federating requests, which allows multiple independent\n' +
    '  Heroic clusters to serve clients through a single global interface.\n' +
    '  This can be used to reduce the amount of geographical traffic by allowing one\n' +
    '  cluster to operate completely isolated within its zone.\n' +
    '</p>\n' +
    '\n' +
    '<ng-include src="\'_pages/docs/federation-tail.ngt\'"></ng-include>\n' +
    '');
}]);

angular.module('_pages/docs/profiles.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/profiles.ngt',
    '<h2>Profiles</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Profiles are small configuration units which can be merged into the overall\n' +
    '  configuration.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following is an example of starting heroic with an in-memory configuration\n' +
    '  using the shell, and the <code>memory</code> profile:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$> tools/heroic-shell --server -P memory\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  A list of all available profiles and their options can be seen with the\n' +
    '  <code>--help</code> switch.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Note: <code>-X elasticsearch.configure</code> will cause the temporary\n' +
    '  elasticsearch instances to be automatically configured.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Playing with the <code>memory</code> profile</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Using the <code>load-generated</code> command, you can generate some random\n' +
    '  data for the last week to play around with.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    'heroic&gt; load-generated\n' +
    '...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  You can also manually write some data into the the local instance:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    'heroic&gt; write --series \'{"key": "test", "tags": {"host": "a", "what": "foo"}}\' -p 00:00=1 -p 00:01=2\n' +
    'heroic&gt; write --series \'{"key": "test", "tags": {"host": "a", "what": "bar"}}\' -p 00:00=3 -p 00:01=4\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  We can fetch the data back out:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    'heroic&gt; fetch --series \'{"key": "test", "tags": {"host": "a", "what": "bar"}}\' --start 00:00 --end 00:02\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  We can also query the data:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    'heroic&gt; query average from points(1d) where $key = test;\n' +
    'heroic&gt; query average from points(1d) where $key = test and what=bar;\n' +
    '</codeblock>\n' +
    '');
}]);

angular.module('_pages/docs/query_language.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/query_language.ngt',
    '<h2>Heroic Query Language</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic uses a JSON-based language to define queries.\n' +
    '  This can also be expressed using an experimental DSL called HQL\n' +
    '  (Heroic Query Language).\n' +
    '</p>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>Experimental</h4>\n' +
    '  <p>\n' +
    '    The HQL should currently be considered experimental and might be subject to future changes.\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<p>\n' +
    '  Queries have the following structure.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    '&lt;aggregation&gt;\n' +
    '  [from &lt;source&gt;]\n' +
    '  [where &lt;filter&gt;]\n' +
    '  [as &lt;key&gt;=&lt;value&gt;[,&lt;key&gt;=&lt;value&gt;]]\n' +
    '  [with &lt;key&gt;=&lt;value&gt;[,&lt;key&gt;=&lt;value&gt;]];\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  References have the following structures.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'let $&lt;name&gt; = &lt;query&gt;;\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  <em>Complex Queries</em> are queries referencing other queries through a\n' +
    '  reference.\n' +
    '  The following is an example of this\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'let $a = average by host | sum by site;\n' +
    '\n' +
    '$a / shift($a, -7d) - 1.0\n' +
    '  where what = cpu-usage and role = heroic;\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  The following is a complete example of a HQL-based query:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    'average by host | sum by site\n' +
    '  from points(1d)\n' +
    '  where role=heroic and what=cpu-idle\n' +
    '  with size=5m\n' +
    '</codeblock>\n' +
    '\n' +
    '<h3 id="json-dsl">JSON vs HQL</h3>\n' +
    '\n' +
    '<p>\n' +
    '  JSON is typically used when a query is built programatically because the structure is unambigious in terms of precedence and escaping.\n' +
    '  There is also a ton of language support for it, and it meshes well with restful APIs.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The HQL was developed to make it easier for humans to express queries or filters in a manner which is more convenient.\n' +
    '  The language is infix, and simple strings do not have to be escaped (e.g. <code language="hql">host</code> vs. <code language="hql">&quot;host&quot;</code>)\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A primary Goal of the HQL is that it should act as a complement to the JSON queries.\n' +
    '  Any query can be expressed either in JSON or HQL.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following is an example filter expressed both in a JSON, and in the HQL.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="hql">\n' +
    '$key = "hello kitty" and host = foo.example.com\n' +
    '</code><code language="json">\n' +
    '["and", ["$key", "hello kitty"], ["=", "host", "foo.example.com"]]\n' +
    '</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  To test this principle you can fire up the <a ui-sref="^.shell">Heroic Shell</a> and run the following commands:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ tools/heroic-shell\n' +
    '\n' +
    'heroic> parse-query --no-indent "average by host | sum by site"\n' +
    '{"aggregation":{"type":"chain","chain":[{"type":"group","of":["host"],"each":{"type":"sum"}},{"type":"average"}]}}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h3>Aggregations</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Main Article: <a ui-sref="^.aggregations">Aggregations</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Aggregations are expressed as function calls, or typed documents.\n' +
    '  See the following example using the <a ui-sref="^.aggregations({\'#\': \'sum\'})">sum</a> aggregation.\n' +
    '</p>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <td>\n' +
    '      <codeblock language="hql">\n' +
    '      sum(3m)\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      <codeblock language="json">\n' +
    '      {"type": "sum", "size": "3m"}\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<p>\n' +
    '  There is also syntactic sugar for the special\n' +
    '  <a ui-sref="^.aggregations({\'#\': \'group\'})">group</a> and\n' +
    '  <a ui-sref="^.aggregations({\'#\': \'chain\'})">chain</a>\n' +
    '  aggregations.\n' +
    '</p>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <td>\n' +
    '      <codeblock language="hql">\n' +
    '      empty by host\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      <codeblock language="json">\n' +
    '      {"type": "group", "of": ["host"], "each": {"type": "empty"}}\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td>\n' +
    '      <codeblock language="hql">\n' +
    '      average(size=5m) | sum(size=10m)\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      <codeblock language="json">\n' +
    '      {\n' +
    '        "type": "chain",\n' +
    '        "chain": [\n' +
    '          {"type": "average", "size": "5m"},\n' +
    '          {"type": "sum", "size": "10m"}\n' +
    '        ]\n' +
    '      }\n' +
    '      </codeblock>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h3>Filters</h3>\n' +
    '\n' +
    '<p>\n' +
    '  A filter reduces the number of selected time series, if no filter (or the <a ui-sref="{\'#\':\'filter-true\'}">true filter</a>) is used, then it is implied that <em>all</em> time series in the database is selected.\n' +
    '</p>\n' +
    '\n' +
    '<img style="width: 100%;" src="images/filter-dsl.svg"></img>\n' +
    '\n' +
    '<h3>Available Filters</h3>\n' +
    '\n' +
    '<h4>Boolean Operators</h4>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <th></th>\n' +
    '    <th>Description</th>\n' +
    '    <th>Parsed</th>\n' +
    '    <th>JSON</th>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">And</th>\n' +
    '    <td>Matches if all child statements match. <code language="hql">true</code> if empty.</td>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; and &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["and", &lt;a&gt;, &lt;b&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Or</th>\n' +
    '    <td>Matches if any child statements match. <code language="hql">false</code> if empty.</td>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; or &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["or", &lt;a&gt;, &lt;b&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Not</th>\n' +
    '    <td>Matches if child statement does not match.</td>\n' +
    '    <td><pre><code language="hql">!&lt;a&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["not", &lt;a&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h4>Operators</h4>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <th></th>\n' +
    '    <th>Parsed</th>\n' +
    '    <th>JSON</th>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Matches</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; = &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["=", &lt;a&gt;, &lt;b&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Does Not Match</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; != &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["not", ["=", &lt;a&gt;, &lt;b&gt;]]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Starts With</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; ^ &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["^", &lt;a&gt;, &lt;b&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Does Not Start With</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; !^ &lt;b&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["not", ["^", &lt;a&gt;, &lt;b&gt;]]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Key Matches</th>\n' +
    '    <td><pre><code language="hql">$key = &lt;a&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["key", &lt;a&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Key Does Not Match</th>\n' +
    '    <td><pre><code language="hql">$key != &lt;a&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["not", ["key", &lt;a&gt;]]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Exists</th>\n' +
    '    <td><pre><code language="hql">+&lt;a&gt;</code></pre></td>\n' +
    '    <td><pre><code language="json">["+", &lt;a&gt;]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag In</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; in [&lt;b&gt;, ..]</code></pre></td>\n' +
    '    <td><pre><code language="json">["or", ["=", &lt;a&gt;, &lt;b&gt;], ..]</code></pre></td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Tag Not In</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; not in [&lt;b&gt;, ..]</code></pre></td>\n' +
    '    <td><pre><code language="json">["not", ["or", ["=", &lt;a&gt;, &lt;b&gt;], ..]]</code></pre></td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h3>The HQL Language</h3>\n' +
    '\n' +
    '<h4>Primitives</h4>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <th scope="row">Simple String</th>\n' +
    '    <td>\n' +
    '      String made up of a limited set of characters for convenience.\n' +
    '      It must not match a <a href="#parsed-keyword">parsed keyword</a>.\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      <pre><code language="hql">hello.world</code></pre>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Quoted String</th>\n' +
    '    <td>String which is quoted to support <em>any</em> set of characters. Supports the same <a href="https://docs.oracle.com/javase/tutorial/java/data/characters.html">escape sequences as Java</a>.</td>\n' +
    '    <td>\n' +
    '      <pre><code language="hql">"hello world"</code></pre>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h4>Arithmetic Expressions</h4>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <th scope="row">Addition</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; + &lt;b&gt;</code></pre></td>\n' +
    '    <td>\n' +
    '      Valid operands are:\n' +
    '      <code language="hql">&lt;string&gt; + &lt;string&gt;</code>,\n' +
    '      and <code language="hql">&lt;number&gt; + &lt;number&gt;</code>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <th scope="row">Subtraction</th>\n' +
    '    <td><pre><code language="hql">&lt;a&gt; - &lt;b&gt;</code></pre></td>\n' +
    '    <td>\n' +
    '      Valid operands are:\n' +
    '      <code language="hql">&lt;number&gt; - &lt;number&gt;</code>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h4 id="special-variables">Special Variables</h4>\n' +
    '\n' +
    '<div class="table-responsive">\n' +
    '<table class="table table-bordered">\n' +
    '  <tr>\n' +
    '    <td><code language="hql">$key</code></td>\n' +
    '    <td>Can be used in most places a tag is expected. It indicates that the given expression should match the special field <em>key</em> instead of a tag.</td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <td><code language="hql">$now</code></td>\n' +
    '    <td>Expands to the current (server-side) timestamp in milliseconds.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '</div>\n' +
    '\n' +
    '<h4 id="duration">Durations</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Durations are represented as a numeric component with a suffix, like <code language="hql">3H</code>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Valid suffixes are:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><b>ms</b> - for milliseconds</li>\n' +
    '  <li><b>s</b> - for seconds</li>\n' +
    '  <li><b>m</b> - for minutes</li>\n' +
    '  <li><b>H</b> - for hours</li>\n' +
    '  <li><b>d</b> - for days</li>\n' +
    '  <li><b>w</b> - for weeks</li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  Durations support arithmetic expressions with each other and numbers.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following is a valid expression:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="hql">\n' +
    '$now - (1d + 1H)\n' +
    '</codeblock>\n' +
    '');
}]);

angular.module('_pages/docs/shell.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/shell.ngt',
    '<h2>Heroic Shell</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic Shell is the administration tool of heroic.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  An heroic instance can be configured to listen for shell connections.\n' +
    '  This will allow an administrator to connect and run <em>tasks</em> on the server.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Tasks are small actions indended to help an administrator manage the heroic instances.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  You can configure an heroic instance to enable <em>local</em> shell connections by adding the following snippet to your <a ui-sref="docs.config">heroic.yml</a>.\n' +
    '  For more details, see <a ui-sref="^.config.shell_server">Shell Server Configuration</a>.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'shellServer: {}\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  The shell can be run in two different modes; <a ui-sref="{\'#\': \'remote-shell\'}"><em>remote</em></a>, and <a ui-sref="{\'#\': \'standalone-shell\'}"><em>standalone</em></a>.\n' +
    '  It is also possible to activate <a ui-sref="{\'#\': \'profiles\'}"><em>profiles</em></a>, which effects the configuration of the service.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  In <em>remote</em> mode, you connect to an already running heroic instance which is <a ui-sref="^.config.shell_server">configured to accept shell connection</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  In <em>standalone</em> mode, you can setup a new heroic instance on-the-fly with a given configuration.\n' +
    '  The shell will then be connected to that instance, as-if you were running in remote mode.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The shell can be started using the <code>com.spotify.heroic.HeroicShell</code> class.\n' +
    '  Assuming you have heroic in your class path, the following is an example of how to do that.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ java com.spotify.heroic.HeroicShell --help\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  From now on we will be referring to starting the shell from the heroic project directory, which includes the helper script <code language="bash">tools/heroic-shell</code>.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="remote-shell">Remote Shell</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Assuming you are in the heroic project directory, you can invoke the shell like the following.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ tools/heroic-shell --connect &lt;host&gt;[:port]\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  At this point you should be connected to the heroic instance, and can invoke any tasks available in it.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="standalone-shell">Standalone Shell</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Standalone shells includes the heroic service running in the background and the shell is directly connected to this instance.\n' +
    '  This is typically useful when trying out specific heroic features, specifically in combination with the <a ui-sref="{\'#\': \'memory-profile\'}">memory profile</a>\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="profiles">Profiles</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Profiles are small pieces of configuration which can be activated with the <code language="bash">-P &lt;name&gt;</code> switch.<br />\n' +
    '  Extra options can be passed into profiles using the <code language="bash">-X &lt;option&gt;[=&lt;value&gt;]</code> switch.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  All available set of profiles and options can be found by at the end of the help command (<code language="bash">--help</code>).\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ tools/heroic-shell --help\n' +
    '... skip documentation about switches\n' +
    '\n' +
    'Available Extra Parameters:\n' +
    '  configure\n' +
    '      Automatically configure all backends.\n' +
    '  datastax.configure\n' +
    '      Automatically configure the datastax backend\n' +
    '  elasticsearch.configure\n' +
    '      Automatically configure the Elasticsearch backend\n' +
    '  elasticsearch.configure\n' +
    '      Automatically configure the Elasticsearch backend\n' +
    '\n' +
    'Available Profiles (activate with: -P &lt;profile&gt;):\n' +
    '  generated - Configures a metric backend containing generated data (does \n' +
    '  not support writes)\n' +
    '\n' +
    '  memory - Configures in-memory backends for everything (useful for \n' +
    '  integration/performance testing)\n' +
    '\n' +
    '  cassandra - Configures a metric backend for Cassandra\n' +
    '    cassandra.type=&lt;type&gt;\n' +
    '        Type of backend to use, valid values are: legacy, ng\n' +
    '    cassandra.seeds=&lt;host&gt;[:&lt;port&gt;][,..]\n' +
    '        Seeds to use when configuring backend\n' +
    '</codeblock>\n' +
    '\n' +
    '<h4 id="memory-profile">Memory Profile</h4>\n' +
    '\n' +
    '<p>\n' +
    '  The <em>memory</em> profile is special, since it configures a complete service instance <em>in memory</em>.\n' +
    '  It\'s a great way for testing out heroic.\n' +
    '  You can start the memory profile with the following command.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="bash">\n' +
    '$ tools/heroic-shell --server -P memory -X configure\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  <code language="bash">--server</code> means that the shell will start heroic in server mode, which will accept http requests on port 8080.<br />\n' +
    '  <code language="bash">-X configure</code> will cause all backends to be automatically configured, saving you the need to do it yourself with the <code>configure</code> task.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/tutorial/index.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/tutorial/index.ngt',
    '<h2>Tutorials</h2>\n' +
    '\n' +
    '<p>\n' +
    '  The following articles are tutorials.\n' +
    '  They should act as a zero to complete guide of how to accomplish a certain task.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following tutorials are available:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref="^.kafka_consumer">Consuming metrics over Kafka</a></li>\n' +
    '</ul>\n' +
    '');
}]);

angular.module('_pages/tutorial/kafka_consumer.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/tutorial/kafka_consumer.ngt',
    '<h2>Consuming metrics over Kafka</h2>\n' +
    '\n' +
    '<blockquote cite="http://kafka.apache.org/">\n' +
    '  <p>Kafka has a modern cluster-centric design that offers strong durability and\n' +
    '  fault-tolerance guarantees.</p>\n' +
    '  <footer><cite title="Source Title"><a href="http://kafka.apache.org/">http://kafka.apache.org/</a></cite></footer>\n' +
    '</blockquote>\n' +
    '\n' +
    '<p>\n' +
    '  Kafka is an excellent choice for transporting metrics.\n' +
    '  It\'s not a simple system, so you will find yourself digging through the\n' +
    '  <a href="http://kafka.apache.org/documentation.html">official documentation</a>\n' +
    '  from time-to-time.\n' +
    '  It is resilient towards the failure of individual nodes, and it supports\n' +
    '  <a href="">log retention</a> that could potentially give you some breathing\n' +
    '  room in the face of problems with other components without worrying about the\n' +
    '  permanent loss of metrics.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A Kafka cluster consists of the following parts:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>A <a href="https://zookeeper.apache.org/">ZooKeeper</a> cluster</li>\n' +
    '  <li>A <a href="http://kafka.apache.org/">Kafka</a> cluster</li>\n' +
    '  <li>(Strongly Recommended) <a href="https://github.com/yahoo/kafka-manager">Kafka Manager</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  You should follow the <a href="http://kafka.apache.org/documentation.html#introduction">Kafka introduction</a> for getting started.\n' +
    '  On top of this, it is important that you configure the <a href="http://kafka.apache.org/documentation.html#brokerconfigs">num.partitions</a> option on the broker to be a larger number, like <code>100</code>.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock>\n' +
    '# server.properties\n' +
    'num.partitions=100\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  The exact number is not important, but the number of partitions used for a particular topic is the limiting factor for distributing load.\n' +
    '  So, if you have a topic with only <em>two</em> partitions, this would limit the number of active Heroic consumers you have to two as well.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Configuring Heroic Consumers</h3>\n' +
    '\n' +
    '<p>\n' +
    '  The following is a complete example configuration for a Kafka consumer.\n' +
    '  Take note of <code>group.id</code> below.\n' +
    '  Two consumers belonging to the same <code>group.id</code> will balance the responsibility between them.\n' +
    '  Therefore you can operate as many consumers as you need to support your desired throughput and redundancy with the same configuration.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '# heroic.yaml\n' +
    '\n' +
    'port: 8080\n' +
    '\n' +
    'consumers:\n' +
    '  - type: kafka\n' +
    '    schema: com.spotify.heroic.consumer.schemas.Spotify100\n' +
    '    topics:\n' +
    '      - "metrics-pod1"\n' +
    '    config:\n' +
    '      group.id: heroic-consumer\n' +
    '      zookeeper.connect: zookeeper1.example.com,zookeeper2.example.com,zookeeper3.example.com/heroic\n' +
    '      auto.offset.reset: smallest\n' +
    '      auto.commit.enable: true\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Using the above configuration as skeleton you need to fill in at least the <a ui-sref="docs.config.metrics">metric</a>, and <a ui-sref="docs.config.metadata">metadata</a> backends.\n' +
    '  At this point, you now have a consumer configuration that can be used to spawn one or more Heroic instances faithfully consuming your metrics.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Configuring ffwd-java</h3>\n' +
    '\n' +
    '<p>\n' +
    '  <a href="https://github.com/spotify/ffwd-java">ffwd-java</a> is a metrics forwarding agent developed at Spotify.\n' +
    '  It has first-class support for sending metrics into Kafka, and the following will detail how this is configured.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Kafka uses partitions to distribute load, each producer decides which partition a particular message should be sent to.\n' +
    '  ffwd-java supports partitioning (see <a href="http://kafka.apache.org/documentation.html#introduction">"Topics and Logs" in the Kafka documentation</a>) per-host using the following output plugin:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '# ffwd.yaml\n' +
    '\n' +
    'attributes:\n' +
    '  host: database.example.com\n' +
    '  pod: pod1\n' +
    '\n' +
    'output:\n' +
    '  plugins:\n' +
    '    - type: "kafka"\n' +
    '      flushInterval: 10000\n' +
    '      serializer:\n' +
    '        type: spotify100\n' +
    '      router:\n' +
    '        type: attribute\n' +
    '        attribute: pod\n' +
    '      producer:\n' +
    '        metadata.broker.list: "kafka1.example.com,kafka2.example.com,kafka3.example.com"\n' +
    '        request.required.acks: 1\n' +
    '        request.timeout.ms: 1000\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  The above will instruct ffwd-java to send metrics to kafka, the topic will be determined (routed) to the <code>metrics-&lt;pod&gt;</code> topic, where <code>&lt;pod&gt;</code> is the <code>pod</code> attribute in the metric.\n' +
    '  A host-based partitioner by default, so metrics sent from a single given host will all end up on the same partition.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/api/accept-metadata-query-body.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/accept-metadata-query-body.ngt',
    '<api-accept curl-data=\'{"filter": ["and", ["key", "system"], ["=", "role", "database"]]}\'>\n' +
    '  <api-type>\n' +
    '    <api-field required="true" name="filter" type-href="Filter">\n' +
    '      A filter to use when quering for tags.\n' +
    '    </api-field>\n' +
    '  </api-type>\n' +
    '\n' +
    '  <h4>Example Request</h4>\n' +
    '\n' +
    '  <codeblock language="json">\n' +
    '  {"filter": ["and", ["key", "system"], ["=", "role", "database"]]}\n' +
    '  </codeblock>\n' +
    '</api-accept>\n' +
    '');
}]);

angular.module('_pages/docs/api/accept-series.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/accept-series.ngt',
    '<api-accept curl-data=\'{"key": "foo", "tags": {"site": "sto"}}\'>\n' +
    '  <api-type>\n' +
    '    <api-field name="key" type-json="<string>">\n' +
    '      The key of the series.\n' +
    '    </api-field>\n' +
    '\n' +
    '    <api-field name="tags" type-json="{<string>: <string>}">\n' +
    '      The tags of the series.\n' +
    '    </api-field>\n' +
    '  </api-type>\n' +
    '\n' +
    '  <h4>Example Request</h4>\n' +
    '\n' +
    '  <codeblock language="json">\n' +
    '  {"key": "foo", "tags": {"site": "sto"}}\n' +
    '  </codeblock>\n' +
    '</api-accept>\n' +
    '');
}]);

angular.module('_pages/docs/api/delete-metadata-series.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/delete-metadata-series.ngt',
    '<api-endpoint path="/metadata/series" method="DELETE">\n' +
    '  <p>\n' +
    '    Delete all series metadata matching the given filter.\n' +
    '  </p>\n' +
    '\n' +
    '  <div ng-include="\'_pages/docs/api/accept-metadata-query-body.ngt\'"></div>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    [\n' +
    '      {"key": "foo", "tags": {"site": "lon"}},\n' +
    '      {"key": "foo", "tags": {"site": "sto"}}\n' +
    '    ]\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/get-status.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/get-status.ngt',
    '<api-endpoint path="/status" method="GET">\n' +
    '  <p>\n' +
    '    Query for the status of an instance.\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    The status code <span class="status">503</span> is used to indicate to load balancers that a service is\n' +
    '    not available for requests right now.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept empty="true"></api-accept>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "ok": true,\n' +
    '      "service": {\n' +
    '        "name": "The Heroic Time Series Database",\n' +
    '        "version": "master (git: aaccddee)",\n' +
    '        "id": "api"\n' +
    '      },\n' +
    '      "consumers": {\n' +
    '        "ok": true,\n' +
    '        "available": 0,\n' +
    '        "ready": 0,\n' +
    '        "errors": 0,\n' +
    '        "consumingThreads": 1,\n' +
    '        "totalThreads": 1\n' +
    '      },\n' +
    '      "backends": {\n' +
    '        "ok": true,\n' +
    '        "available": 0,\n' +
    '        "ready": 0\n' +
    '      },\n' +
    '      "metadataBackends": {\n' +
    '        "ok": true,\n' +
    '        "available": 0,\n' +
    '        "ready": 0\n' +
    '      },\n' +
    '      "cluster": {\n' +
    '        "ok": true,\n' +
    '        "onlineNodes": 1,\n' +
    '        "offlineNodes": 0\n' +
    '      }\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '\n' +
    '  <api-response status="503">\n' +
    '    <h4>Example <span class="status">503</span> Response</h4>\n' +
    '\n' +
    '    <p>\n' +
    '      A <span class="status">503</span> will still return a response body, however the root key <code>.ok</code> will have the value <code>false</code>.\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      The below document shows that there is something wrong with the configured consumers, in this case it is because the number of <code>consumingThreads</code> is not the same as the <code>totalThreads</code>\n' +
    '    </p>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "ok": false,\n' +
    '      "consumers": {\n' +
    '        "ok": false,\n' +
    '        "available": 0,\n' +
    '        "ready": 0,\n' +
    '        "errors": 0,\n' +
    '        "consumingThreads": 0,\n' +
    '        "totalThreads": 1\n' +
    '      },\n' +
    '      ...\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-key-suggest.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-key-suggest.ngt',
    '<api-endpoint path="/metadata/key-suggest" method="POST">\n' +
    '  <p>\n' +
    '    Search for key suggestions.\n' +
    '    This endpoint is intended for use with auto-completion.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept>\n' +
    '    <api-type>\n' +
    '      <api-field name="key" type-json="<string>">\n' +
    '        Key to help limit down suggestions, this is a free text value that is\n' +
    '        analyzed by the search engine.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="filter" type-href="Filter">\n' +
    '        Filter to apply\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="range" type-href="QueryDateRange">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="limit" type-json="<number>">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="match" type-href="MatchOptions">\n' +
    '        Temporary match options to apply, only used for experiments.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "key": "foo",\n' +
    '      "filter": ["=", "role", "heroic"],\n' +
    '      "limit": 10\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <api-type>\n' +
    '      <api-field name="errors" type-array-href="RequestError">\n' +
    '        Errors that occured during the request.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="suggestions" required="true" type-json="[{score: <number>, key: <string>}, ...]">\n' +
    '        Suggest key values with scores.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "errors": [],\n' +
    '      "suggestions": [\n' +
    '        {"score": 1.5, "key": "foobar"},\n' +
    '        {"score": 0.9, "key": "folly"}\n' +
    '      ]\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-keys.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-keys.ngt',
    '<api-endpoint path="/metadata/keys" method="POST">\n' +
    '  <p>\n' +
    '    Use to query for keys.\n' +
    '  </p>\n' +
    '\n' +
    '  <div ng-include="\'_pages/docs/api/accept-metadata-query-body.ngt\'"></div>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "result": ["foo", "bar"],\n' +
    '      "sampleSize": 5\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-series-count.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-series-count.ngt',
    '<api-endpoint path="/metadata/series-count" method="POST">\n' +
    '  <p>\n' +
    '    Count the number of time series matching a given filter.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept curl-data=\'{"filter": ["and", ["key", "system"], ["=", "role", "database"]]}\'>\n' +
    '    <api-type>\n' +
    '      <api-field required="false" name="filter" type-href="Filter">\n' +
    '        A filter to use when counting series.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field required="false" name="range" type-href="QueryDateRange">\n' +
    '        Range for which to count series.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {"filter": ["and", ["key", "system"], ["=", "role", "database"]]}\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "errors": [],\n' +
    '      "limited": false,\n' +
    '      "count": 10001\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-series.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-series.ngt',
    '<h2>Endpoints</h2>\n' +
    '\n' +
    '<api-endpoint path="/metadata/series" method="POST">\n' +
    '  <p>\n' +
    '    Get all series metadata matching the given filter.\n' +
    '  </p>\n' +
    '\n' +
    '  <div ng-include="\'_pages/docs/api/accept-metadata-query-body.ngt\'"></div>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    [\n' +
    '      {"key": "foo", "tags": {"site": "lon"}},\n' +
    '      {"key": "foo", "tags": {"site": "sto"}}\n' +
    '    ]\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-tag-suggest.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-tag-suggest.ngt',
    '<api-endpoint path="/metadata/tag-suggest" method="POST">\n' +
    '  <p>\n' +
    '    Search for tag suggestions.\n' +
    '    This endpoint is intended for use with auto-completion.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept>\n' +
    '    <api-type>\n' +
    '      <api-field name="key" type-json="<string>">\n' +
    '        Tag key to help limit down suggestions, this is a free text value that\n' +
    '        is analyzed by the search engine.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="value" type-json="<string>">\n' +
    '        Tag value  to help limit down suggestions, this is a free text value\n' +
    '        that is analyzed by the search engine.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="filter" type-href="Filter">\n' +
    '        Filter to apply\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="range" type-href="QueryDateRange">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="limit" type-json="<number>">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="match" type-href="MatchOptions">\n' +
    '        Temporary match options to apply, only used for experiments.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "key": "role",\n' +
    '      "value": "hero con",\n' +
    '      "filter": ["=", "site", "lon"],\n' +
    '      "limit": 10\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <api-type>\n' +
    '      <api-field name="errors" type-array-href="RequestError">\n' +
    '        Errors that occured during the request.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="suggestions" required="true" type-json="[{score: <number>, key: <string>, value: <string>}, ...]">\n' +
    '        Suggested tag key-value combinations\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "errors": [],\n' +
    '      "suggestions": [\n' +
    '        {"score": 1.5, "key": "role", "value": "heroic-consumer"}\n' +
    '        {"score": 0.9, "key": "role", "value": "heroic-api"},\n' +
    '      ]\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-tag-value-suggest.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-tag-value-suggest.ngt',
    '<api-endpoint path="/metadata/tag-value-suggest" method="POST">\n' +
    '  <p>\n' +
    '    Search for tag values.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept>\n' +
    '    <api-type>\n' +
    '      <api-field name="key" type-json="<string>">\n' +
    '        Key for where to search for tag values.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="filter" type-href="Filter">\n' +
    '        Filter to apply\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="range" type-href="QueryDateRange">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="limit" type-json="<number>">\n' +
    '        The time range for which to get suggestions.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "key": "role",\n' +
    '      "filter": ["=", "site", "lone"],\n' +
    '      "limit": 10\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <api-type>\n' +
    '      <api-field name="errors" type-array-href="RequestError">\n' +
    '        Errors that occured during the request.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="values" required="true" type-json="[<string>, ...]">\n' +
    '        Values of the given tag.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="limited" required="true" type-json="<boolean>">\n' +
    '        If the result is limited or not.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "errors": [],\n' +
    '      "values": [\n' +
    '        "heroic-api",\n' +
    '        "heroic-consumer"\n' +
    '      ],\n' +
    '      "limited": false\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-metadata-tags.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-metadata-tags.ngt',
    '<api-endpoint path="/metadata/tags" method="POST">\n' +
    '  <p>\n' +
    '    Query for available tag combinations.\n' +
    '  </p>\n' +
    '\n' +
    '  <div ng-include="\'_pages/docs/api/accept-metadata-query-body.ngt\'"></div>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "result": {\n' +
    '        "role": ["database", "webfrontend"],\n' +
    '        "host": ["database.example.com", "webfrontend.example.com"]\n' +
    '      },\n' +
    '      "sampleSize": 2\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-query-batch.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-query-batch.ngt',
    '<api-endpoint path="/query/batch" method="POST">\n' +
    '  <p>\n' +
    '    Run multiple metrics query in a batch.\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    This accepts a JSON document where all keys are expected to map up to a\n' +
    '    Query.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept curl-data=\'{"a": <query>, "b": <query>}\'>\n' +
    '    <api-type>\n' +
    '      <api-field required="true" name="*" type-href="Query">\n' +
    '        Queries to run.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "queries": {\n' +
    '        "a": {\n' +
    '          "range": {"type": "relative", "unit": "HOURS", "value": 2},\n' +
    '          "filter": ["and", ["key", "foo"], ["=", "foo", "bar"], ["+", "role"]],\n' +
    '          "aggregation": {\n' +
    '            "type": "group",\n' +
    '            "of": ["site"],\n' +
    '            "each": {\n' +
    '              "type": "sum"\n' +
    '            }\n' +
    '          }\n' +
    '        },\n' +
    '        "b": {\n' +
    '          "range": {"type": "relative", "unit": "HOURS", "value": 2},\n' +
    '          "filter": ["and", ["key", "baz"], ["=", "foo", "bar"], ["+", "role"]],\n' +
    '          "aggregation": {\n' +
    '            "type": "group",\n' +
    '            "of": ["site"],\n' +
    '            "each": {\n' +
    '              "type": "sum"\n' +
    '            }\n' +
    '          }\n' +
    '        }\n' +
    '      }\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response>\n' +
    '    <api-type>\n' +
    '      <api-field name="*" type-href="QueryResponse">\n' +
    '        Responses to each query run.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "a": {\n' +
    '        "errors": [],\n' +
    '        "result": [\n' +
    '          {\n' +
    '            "hash": "deadbeef",\n' +
    '            "tags": {"foo": "bar"},\n' +
    '            "values": [[1300000000000, 42.0]]\n' +
    '          },\n' +
    '          {\n' +
    '            "hash": "beefdead",\n' +
    '            "tags": {"foo": "baz"},\n' +
    '            "values": [[1300000000000, 42.0]]\n' +
    '          }\n' +
    '        ],\n' +
    '        "range": {\n' +
    '          "end": 1469816790000,\n' +
    '          "start": 1469809590000\n' +
    '        },\n' +
    '        "statistics": {}\n' +
    '      },\n' +
    '      "b": {\n' +
    '        "errors": [],\n' +
    '        "result": [\n' +
    '          {\n' +
    '            "hash": "deadbeef",\n' +
    '            "tags": {"foo": "bar"},\n' +
    '            "values": [[1300000000000, 42.0]]\n' +
    '          },\n' +
    '          {\n' +
    '            "hash": "beefdead",\n' +
    '            "tags": {"foo": "baz"},\n' +
    '            "values": [[1300000000000, 42.0]]\n' +
    '          }\n' +
    '        ],\n' +
    '        "range": {\n' +
    '          "end": 1469816790000,\n' +
    '          "start": 1469809590000\n' +
    '        },\n' +
    '        "statistics": {}\n' +
    '      }\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-query-metrics.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-query-metrics.ngt',
    '<api-endpoint path="/query/metrics" method="POST">\n' +
    '  <p>\n' +
    '    Query and aggregate metrics.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept curl-data=\'{"range": {"type": "relative", "unit": "HOURS", "value": 2}, "filter": ["and", ["key", "foo"], ["=", "foo", "bar"], ["+", "role"]], "groupBy": ["site"]}\'>\n' +
    '    <api-type name="Query">\n' +
    '      <api-field required="true" name="range" type-href="QueryDateRange">\n' +
    '        The range in time for which to query\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="filter" type-href="Filter">\n' +
    '        <p>\n' +
    '          A statement used to filter down the selected time series.\n' +
    '          Each individual filtering field (<code>filter</code>, <code>key</code>, <code>tags</code>, and <code>hasTags</code>) may be empty.\n' +
    '          At least one must be specified to make up a valid filter.\n' +
    '        </p>\n' +
    '\n' +
    '        <p>\n' +
    '          See <a ui-sref="docs.query_language({\'#\': \'filtering\'})">filtering</a> for details on how to build a filter.\n' +
    '        </p>\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="aggregation" type-href="Aggregation">\n' +
    '        Aggregation to use when down-sampling.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="groupBy">\n' +
    '        <p>\n' +
    '          Will create several result groups from the given keys.\n' +
    '          Series will be aggregated on a per-group basis.\n' +
    '        </p>\n' +
    '\n' +
    '        <h5>Example 1.</h5>\n' +
    '\n' +
    '        Given time series <code language="json">{"a": 1, "b": 2}</code> and <code language="json">{"b": 3, "c": 4}</code>, and a <code>groupBy</code> of <code language="json">["a", "b"]</code>\n' +
    '        the returned result groups would be <code language="json">[{"a": 1, "b": 2}, {"a": null, "b": 3}]</code>.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="features" type-json="[<string>, ...]">\n' +
    '        <p>\n' +
    '          Enable or disable a feature on a per-query basis.\n' +
    '          See <a ui-sref="docs.config.features">Features Configuration</a> for more details.\n' +
    '        </p>\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "range": {"type": "relative", "unit": "HOURS", "value": 2},\n' +
    '      "filter": ["and", ["key", "foo"], ["=", "foo", "bar"], ["+", "role"]],\n' +
    '      "groupBy": ["site"]\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response>\n' +
    '    <api-type name="QueryResponse">\n' +
    '      <api-field name="range" type-json="{start: <number>, end: <number>}">\n' +
    '        The range in time for which to query\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="errors" type-array-href="RequestError">\n' +
    '        Potential errors returned either from different shards or for specific\n' +
    '        time series. The presence of an error does not cause the entire query\n' +
    '        to fail, instead it is up to the client to use this information to\n' +
    '        decide if the response is reliable enough.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="result" type-array-href="ShardedResultGroup">\n' +
    '        An array of result groups.\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="statistics" type-href="Statistics">\n' +
    '        Statistics about the current query.\n' +
    '        This field should be inspected for errors which will have caused the result\n' +
    '        to be inconsistent.\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "errors": [\n' +
    '        {\n' +
    '          "type": "node",\n' +
    '          "nodeId": "abcd-efgh",\n' +
    '          "nodeUri": "http://example.com",\n' +
    '          "tags": {"site": "lon"},\n' +
    '          "error": "Connection refused",\n' +
    '          "internal": true\n' +
    '        },\n' +
    '        {\n' +
    '          "type": "series",\n' +
    '          "tags": {"site": "lon"},\n' +
    '          "error": "Aggregation too heavy, too many rows from the database would have to be fetched to satisfy the request!",\n' +
    '          "internal": true\n' +
    '        }\n' +
    '      ],\n' +
    '      "result": [\n' +
    '        {\n' +
    '          "hash": "deadbeef",\n' +
    '          "tags": {"foo": "bar"},\n' +
    '          "values": [[1300000000000, 42.0]]\n' +
    '        },\n' +
    '        {\n' +
    '          "hash": "beefdead",\n' +
    '          "tags": {"foo": "baz"},\n' +
    '          "values": [[1300000000000, 42.0]]\n' +
    '        }\n' +
    '      ],\n' +
    '      "range": {\n' +
    '        "end": 1469816790000,\n' +
    '        "start": 1469809590000\n' +
    '      },\n' +
    '      "statistics": {}\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/post-write.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/post-write.ngt',
    '<api-endpoint path="/write" method="POST">\n' +
    '  <p>\n' +
    '    Used for writing data into heroic directly.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-accept curl-data=\'{"series": {"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}}, "data": {"type": "points", "data": [[1300000000000, 42.0], [1300001000000, 84.0]]}}\'>\n' +
    '    <api-type>\n' +
    '      <api-field name="series" required="true" type-href="Series">\n' +
    '        <p>Time series to write data to.</p>\n' +
    '      </api-field>\n' +
    '\n' +
    '      <api-field name="data" required="true" type-href="MetricCollection">\n' +
    '        <p>Collection of metrics to write.</p>\n' +
    '      </api-field>\n' +
    '    </api-type>\n' +
    '\n' +
    '    <h4>Example Request</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "series": {"key": "foo", "tags": {"site": "lon", "host": "www.example.com"}},\n' +
    '      "data": {"type": "points", "data": [[1300000000000, 42.0], [1300001000000, 84.0]]}\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-accept>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '    <codeblock language="json">\n' +
    '    {\n' +
    '      "ok": true\n' +
    '    }\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/put-metadata-series.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/put-metadata-series.ngt',
    '<api-endpoint path="/metadata/series" method="PUT">\n' +
    '  <p>\n' +
    '    Write the given series metadata.\n' +
    '  </p>\n' +
    '\n' +
    '  <div ng-include="\'_pages/docs/api/accept-series.ngt\'"></div>\n' +
    '\n' +
    '  <api-response status="200">\n' +
    '    <h4>Example <span class="status">200</span> Response</h4>\n' +
    '\n' +
    '    <codeblock language="json">\n' +
    '    [\n' +
    '      {"key": "foo", "tags": {"site": "lon"}},\n' +
    '      {"key": "foo", "tags": {"site": "sto"}}\n' +
    '    ]\n' +
    '    </codeblock>\n' +
    '  </api-response>\n' +
    '</api-endpoint>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-aggregation.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-aggregation.ngt',
    '<api-type name="Aggregation">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.aggregation.Aggregation</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    An aggregation is responsible for analysing and sampling a larger dataset into a smaller, more manageable one.\n' +
    '    For details on all available aggregations, see the <a ui-sref="^.aggregations">Aggregations Section</a>.\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    This object tells the distance to the point in the past.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"relative"\'>\n' +
    '    Type of the aggregation.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="sampling" type-href=\'SamplingQuery\'>\n' +
    '    Sampling to use with aggregation.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-event.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-event.ngt',
    '<api-type name="Event" kind="structural">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.Event</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    See the <a ui-sref="^.data_model({\'#\': \'events\'})">Events section in Data Model</a>\n' +
    '    for more details about <em>what</em> an event is.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="datapoint" type-json=\'[<timestamp>, {<payload>}]\'>\n' +
    '    <p>\n' +
    '      A single event.\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      The <code language="json">&lt;timestamp&gt;</code> is the number of milliseconds since unix epoch.\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      The <code language="json">&lt;payload&gt;</code> is the sample payload.\n' +
    '    </p>\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-filter.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-filter.ngt',
    '<api-type kind="structural" name="Filter">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.filter.Filter</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    The structure of a filter is explained in detail in\n' +
    '    the <a ui-sref="docs.query_language">Query Language</a> documentation.\n' +
    '    This will only be a brief, syntactical overview.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field name="and" type-json=\'["and", <filter>, ...]\'>\n' +
    '    A boolean \'and\' operation, evaluates to <code language="json">true</code> if <em>all</em> of the nested <code language="json">&lt;filter&gt;</code>\'s are <code language="json">true</code>, otherwise <code language="json">true</code>.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="or" type-json=\'["or", <filter>, ...]\'>\n' +
    '    A boolean \'or\' operation, evaluates to <code language="json">true</code> if <em>any</em> of the nested <code language="json">&lt;filter&gt;</code>\'s are <code language="json">true</code>, otherwise <code language="json">false</code>.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="not" type-json=\'["not", <filter>]\'>\n' +
    '    A boolean \'not\' operation, evaluates to <code language="json">true</code> if the nested <code language="json">&lt;filter&gt;</code> is <code language="json">false</code>.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-match-options.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-match-options.ngt',
    '<api-type name="MatchOptions">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.suggest.MatchOptions</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <div class="callout callout-warning">\n' +
    '    These are a set of experimental match options that are typically set during\n' +
    '    suggest queries.\n' +
    '\n' +
    '    None of them should typically be enabled unless an experiment is being\n' +
    '    performed since they might severely impact the performance of the service.\n' +
    '  </div>\n' +
    '\n' +
    '  <api-field name="fuzzy" type-json="<boolean>">\n' +
    '    If set, perform elasticsearch fuzzy matching (very slow).\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="tokenize" type-json="<boolean>">\n' +
    '    If set, perform additional server-side tokenization.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-metric-collection.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-metric-collection.ngt',
    '<api-type name="MetricCollection">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.MetricCollection</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"points" | "events"\'>\n' +
    '    The type of data to write.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="data" type-json=\'[Point | Event, ..]\'>\n' +
    '    The data to write. The type depends on the value of the <code language="json">type</code> field.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-point.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-point.ngt',
    '<api-type name="Point" kind="structural">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.Point</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    See the <a ui-sref="^.data_model({\'#\': \'data-points\'})">Points section in Data Model</a>\n' +
    '    for more details about <em>what</em> a point is.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="datapoint" type-json=\'[<timestamp>, <value>]\'>\n' +
    '    <p>\n' +
    '      A single datapoint.\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      The <code language="json">&lt;timestamp&gt;</code> is the number of milliseconds since unix epoch.\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      The <code language="json">&lt;value&gt;</code> is the sample value.\n' +
    '    </p>\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-query-date-range.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-query-date-range.ngt',
    '<!-- anchor to use when linking to QueryDateRange -->\n' +
    '<span api-type-id="QueryDateRange"></span>\n' +
    '\n' +
    '<api-type name="QueryDateRange.Absolute">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.QueryDateRange</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    An absolute time span.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"absolute"\'>\n' +
    '    Type of the absolute date range.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="start" type-json=\'<number>\'>\n' +
    '    Starting timestamp in milliseconds from the unix epoch.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="end" type-json=\'<number>\'>\n' +
    '    Ending timestamp in milliseconds from the unix epoch.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '<api-type name="QueryDateRange.Relative">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.QueryDateRange</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    Relative date ranges range from <em>the current</em> time, to a specific\n' +
    '    point in the past.\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    This object tells the distance to the point in the past.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"relative"\'>\n' +
    '    Type of the relative date range.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="unit" type-json=\'<string>\'>\n' +
    '    <p>\n' +
    '      Unit to use for <code>value</code>\n' +
    '    </p>\n' +
    '\n' +
    '    <p>\n' +
    '      <code>unit</code> must be one of.\n' +
    '    </p>\n' +
    '\n' +
    '    <ul class="list-inline">\n' +
    '      <li><code language="json">"MILLISECONDS"</code></li>\n' +
    '      <li><code language="json">"SECONDS"</code></li>\n' +
    '      <li><code language="json">"MINUTES"</code></li>\n' +
    '      <li><code language="json">"HOURS"</code></li>\n' +
    '      <li><code language="json">"DAYS"</code></li>\n' +
    '      <li><code language="json">"WEEKS"</code></li>\n' +
    '      <li><code language="json">"MONTHS"</code></li>\n' +
    '    </ul>\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field name="value" type-json=\'<number>\'>\n' +
    '    How many <code>unit</code> timespans back in time this date starts.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-request-error.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-request-error.ngt',
    '<api-type name="RequestError">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.RequestError</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"node"|"shard"|"query"\'>\n' +
    '    Type of the error.\n' +
    '\n' +
    '    The different error types have different sets of information associated with them.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '<api-type name="NodeError">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.NodeError</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"node"\'></api-field>\n' +
    '\n' +
    '  <api-field required="true" name="error" type-json=\'<string>\'>\n' +
    '    An error message.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="nodeId" type-json=\'<string>\'>\n' +
    '    UUID of the failing node.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="node" type-json=\'<string>\'>\n' +
    '    String-representation of the failing node.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="tags" type-json=\'{<string>: <string>, ..}\'>\n' +
    '    <a ui-sref="^.config.cluster({\'#\': \'tags\'})">Cluster tags</a> of the failing node.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '<api-type name="ShardError">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.ShardError</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"shard"\'></api-field>\n' +
    '\n' +
    '  <api-field required="true" name="error" type-json="<string>">\n' +
    '    A human readable message indicating what went wrong.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="nodes" type-json="[<string>, ...]">\n' +
    '    A list of nodes belonging to the shard where the error originated.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="shard" type-json="{<string>: <string>, ...}">\n' +
    '    The shard tags associated with the error, all nodes in <code>nodes</code>\n' +
    '    belong to this shard.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '<api-type name="QueryError">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.QueryError</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"series"\'></api-field>\n' +
    '\n' +
    '  <api-field required="true" name="error" type-json=\'<string>\'>\n' +
    '    A human readable message indicating what went wrong.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-sampling-query.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-sampling-query.ngt',
    '<api-type name="SamplingQuery">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.aggregation.SamplingQuery</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="false" name="unit" type-json=\'<string>\'>\n' +
    '    The default unit to use in size and extent (unless specified).\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="size" type-json=\'<number> | {"unit": <string>, "value": <number>}\'>\n' +
    '    The size to use in a given aggregation.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="extent" type-json=\'<number> | {"unit": <string>, "value": <number>}\'>\n' +
    '    The extent to use in a given aggregation.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/api/type-series.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-series.ngt',
    '<api-type name="Series">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.common.Series</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <p>\n' +
    '    See the <a ui-sref="^.data_model({\'#\': \'series\'})">Series section in Data Model</a>\n' +
    '    for details about <em>what</em> a series is.\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="key" type-json=\'<string>\'>\n' +
    '    Key of the series.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="tags" type-json=\'{<string>: <string>, ...}\'>\n' +
    '    Tags of the series.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '');
}]);

angular.module('_pages/docs/api/type-sharded-result-group.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-sharded-result-group.ngt',
    '<api-type name="ShardedResultGroup">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.metric.ShardedResultGroup</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field required="true" name="type" type-json=\'"points"|"events"|"spreads"|"groups"\'>\n' +
    '    Type of the result group.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="hash" type-json=\'<string>\'>\n' +
    '    A string uniquely identifying this result group.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="shard" type-json=\'{<string>: <string>}\'>\n' +
    '    The shard that the result group came from.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="cadence" type-json=\'<number>\'>\n' +
    '    The cadence at which a client can expect samples in this group to adhere to.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="values" type-json=\'[[<timestamp>, <sample>], ..]\'>\n' +
    '    A list of values of the given <code>type</code>.\n' +
    '\n' +
    '    <ul>\n' +
    '      <li>\n' +
    '        <code language="json">"series"</code> indicates that the\n' +
    '        <code language="json">&lt;sample&gt;</code> is a <code language="json">&lt;number&gt;</code>.\n' +
    '      </li>\n' +
    '      <li>\n' +
    '        <code language="json">"events"</code> indicates that the\n' +
    '        <code language="json">&lt;sample&gt;</code> is an <code language="json">{&lt;string&gt;: &lt;any&gt;}</code> object.\n' +
    '      </li>\n' +
    '    </ul>\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="tags" type-json=\'{<string>: <string>}\'>\n' +
    '    The set of tags that have a single value.\n' +
    '  </api-field>\n' +
    '\n' +
    '  <api-field required="true" name="tagCounts" type-json=\'{<string>: <number>}\'>\n' +
    '    The set of tags that have more than a single value, the number is the number of distinct tags.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '\n' +
    '');
}]);

angular.module('_pages/docs/api/type-statistics.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/api/type-statistics.ngt',
    '<api-type name="Statistics">\n' +
    '  <p>\n' +
    '    Java Class: <a git-href-java="heroic-component">com.spotify.heroic.common.Statistics</a>\n' +
    '  </p>\n' +
    '\n' +
    '  <api-field name="counters" type-json=\'{<string>: <number>}\'>\n' +
    '    Counters containing statistics.\n' +
    '  </api-field>\n' +
    '</api-type>\n' +
    '');
}]);

angular.module('_pages/docs/config/cluster.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/cluster.ngt',
    '<h2>Cluster Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Cluster configuration is located in the <code>cluster</code> section of the configuration.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A typical configuration looks something like this.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'cluster:\n' +
    '  tags:\n' +
    '    site: london\n' +
    '  protocols:\n' +
    '    - type: grpc\n' +
    '  capabilities:\n' +
    '    - QUERY\n' +
    '  discovery:\n' +
    '    type: static\n' +
    '    nodes:\n' +
    '      - "grpc://heroic-stockholm-a"\n' +
    '      - "grpc://heroic-stockholm-b"\n' +
    '      - "grpc://heroic-london-a"\n' +
    '      - "grpc://heroic-london-b"\n' +
    '</codeblock>\n' +
    '\n' +
    '<h3 id="tags"><code>tags</code></h3>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'site: london\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Defines a set of tags that identifies which part of the cluster this node belongs to.\n' +
    '  It\'s defined as an object like <code language="json">{"site": "london"}</code>.\n' +
    '  Nodes that have an identical set of tags are said to be part of the same <em>shard</em>.\n' +
    '  See the <a ui-sref="^.^.federation">federation</a> section for more details.\n' +
    '</p>\n' +
    '\n' +
    '<h3><code>protocols</code></h3>\n' +
    '\n' +
    '<p>\n' +
    '  Contains a list of protocols that this node can speak.\n' +
    '</p>\n' +
    '\n' +
    '<h4>grpc</h4>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'host: &lt;host&gt;\n' +
    'port: &lt;port&gt;\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>host</code></td>\n' +
    '    <td>the address of the interface that this node will bind to.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>port</code></td>\n' +
    '    <td>the port number that this node will bind to.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '    <a href="https://grpc.io">gRPC</a> is an open source RPC protocol.\n' +
    '</p>\n' +
    '\n' +
    '<h4>nativerpc</h4>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'host: &lt;host&gt;\n' +
    'port: &lt;port&gt;\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>host</code></td>\n' +
    '    <td>the address of the interface that this node will bind to.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>port</code></td>\n' +
    '    <td>the port number that this node will bind to.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '  nativerpc is a protocol specifically designed to combat timeout and keep-alive issues that exists in HTTP.\n' +
    '  A TCP connection might decide to stall for arbitrary periods of time, and most HTTP clients have a hard time detecting if this happens <em>after</em> a request has been received by the server.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A typical HTTP client supports two times of timeouts, <em>connection</em> and <em>read</em>.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>The <em>connection</em> timeout is typically the time it takes to establish an open TCP connection.</li>\n' +
    '  <li>The <em>read</em> timeout is typically the time it takes to send and receive a response from the remote server.</li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  A common error scenario arises when a client cannot predict how long a request will be running for.\n' +
    '  Given that we are dealing with cross-site traffic this is very bad, because a broken flow will leave the client hanging until it triggers its <em>read</em> timeout.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  <b>nativerpc</b> solves this by using <em>heartbeats</em>.\n' +
    '  The client communicates at which interval it expects to receive heartbeats, and as long as the server is actively processing the request it is expected to keep sending them.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  This simple mechanism allows the client to detect broken flows, regardless of cause.\n' +
    '  And allows the request to fail faster than what is permitted by HTTP.\n' +
    '</p>\n' +
    '\n' +
    '<h3><code>capabilities</code></h3>\n' +
    '\n' +
    '<p>\n' +
    '  This is a list of capabilities that is supported by this node.\n' +
    '  Capabilities restrict what other nodes perceive this node to be capable of doing.\n' +
    '  The current set of capabilities available right now are.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><code>QUERY</code> - the node is capable of receiving queries, this needs to be set if you intend for this node to receive queries over the cluster.</li>\n' +
    '  <li><code>WRITE</code> - the node is capable of receiving writes, this needs to be set if you intend for this node to receive writes over the cluster.</li>\n' +
    '</ul>\n' +
    '\n' +
    '<h3><code>discovery</code></h3>\n' +
    '\n' +
    '<p>\n' +
    '  The mechanism used to discover nodes in the cluster.\n' +
    '</p>\n' +
    '\n' +
    '<h4>static</h4>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'nodes:\n' +
    '  - &lt;url&gt;\n' +
    '  - ..\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <th>nodes</th>\n' +
    '    <td>\n' +
    '      Is the list of nodes that this node will attempt to add to its cluster registry.\n' +
    '      A valid url has the form <code>protocol://host[:port]</code>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '  Static is the simplest possible form of discovery.\n' +
    '\n' +
    '  It takes a list of nodes that may, or may not be reachable at the moment.\n' +
    '\n' +
    '  This list will be queried at a given interval, and any that responds to a metadata request will be added to the local list of known members.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/config/consumer.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/consumer.ngt',
    '<h2>Consumer Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  A consumer is a component responsible for "consuming" metrics, and introducing them into a Heroic cluster.\n' +
    '  Heroic currently has support for the following types of consumers.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following is an example configuration showcasing some options.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'consumers:\n' +
    '  - type: kafka\n' +
    '    schema: com.spotify.heroic.consumer.schemas.Spotify100\n' +
    '    topics:\n' +
    '      - "metrics-sto1"\n' +
    '      - "metrics-sto2"\n' +
    '    config:\n' +
    '      group.id: heroic-sto\n' +
    '      zookeeper.connect: zookeeper1.example.com,zookeeper2.example.com,zookeeper3.example.com/heroic\n' +
    '      auto.offset.reset: largest\n' +
    '      auto.commit.enable: true\n' +
    '  - type: collectd\n' +
    '    hostPattern:\n' +
    '      pattern: "%{role}.%{site}.%{domain}"\n' +
    '</codeblock>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref="{\'#\': \'kafka\'}">Kafka</a></li>\n' +
    '  <li><a ui-sref="{\'#\': \'collectd\'}">Collectd</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<h3 id="kafka">Kafka</h3>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: kafka\n' +
    'schema: com.spotify.heroic.consumer.schemas.Spotify100\n' +
    'topics:\n' +
    '  - "metrics-sto1"\n' +
    '  - "metrics-sto2"\n' +
    'config:\n' +
    '  group.id: heroic-sto\n' +
    '  zookeeper.connect: zookeeper1.example.com,zookeeper2.example.com,zookeeper3.example.com/heroic\n' +
    '  auto.offset.reset: largest\n' +
    '  auto.commit.enable: true\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  A kafka consumer that reads and parses data out of a Kafka queue.\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td colspan="2">\n' +
    '      <code>type: kafka</code>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>schema</code></td>\n' +
    '    <td>The schema to use when decoding messages.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>topics</code></td>\n' +
    '    <td>A list of topics to read from.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>config</code></td>\n' +
    '    <td>\n' +
    '      An object that will be provided to the Kafka consumer as configuration.\n' +
    '      See the <a href="https://kafka.apache.org/08/configuration.html#consumerconfigs">official documentation</a> for what is expected.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="collectd">Collectd</h3>\n' +
    '\n' +
    '<div class="callout callout-warning">\n' +
    '  <h4>support for collectd is currently in early stage development</h4>\n' +
    '  <p>If you are interested in this, please help out by <a git-href-package="consumer/collectd:com.spotify.heroic.consumer.collectd">contributing to the module</a></p>\n' +
    '</div>\n' +
    '\n' +
    '<p>\n' +
    '  The collectd consumer causes the service to behave as a collectd instance, and listen for packets using the <a href="https://collectd.org/wiki/index.php/Binary_protocol">collectd binary protocol</a>.\n' +
    '  Support is very early stage, and tags are typically generated which are sub-optimal.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'host: "0.0.0.0"\n' +
    'port: 25826\n' +
    'hostPattern:\n' +
    '  pattern: "%{role}.%{site}.%{domain}"\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Metrics collected this way will have the following tags unless <a ui-sref="{\'#\': \'explicit-mapping\'}">explicitly mapped</a>:\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td colspan="2">\n' +
    '      <code>type: collectd</code>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>plugin</code></td>\n' +
    '    <td>The plugin that the metric was emitted from.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>plugin_instance</code></td>\n' +
    '    <td>The plugin instance that the metric was emitted from.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td>The type that the metric was emitted from.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>type_instance</code></td>\n' +
    '    <td>The type instance that the metric was emitted from.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4 id="explicit-mapping">Explicit Mapping</h4>\n' +
    '\n' +
    '<p>\n' +
    '  If you\'d like to improve support for your favorite collectd plugin, please contribute to the <a git-href-java="consumer/collectd">com.spotify.heroic.consumer.collectd.CollectdTypes</a> class.\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/config/elasticsearch_connection.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/elasticsearch_connection.ngt',
    '<h2>Elasticsearch Connection</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Elasticsearch backends are configured with a connection object.\n' +
    '  This determines how an elasticsearch-based backend is connected.\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>clusterName</code></td>\n' +
    '    <td>Cluster name to connect to.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>seeds</code></td>\n' +
    '    <td>List of seeds to connect to.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>nodeClient</code></td>\n' +
    '    <td>If set to <code>true</code>, use a node client by default.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>concurrentBulkRequests</code></td>\n' +
    '    <td>The number of concurrent bulk requests this connection supports.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>flushInterval</code></td>\n' +
    '    <td>The interval in milliseconds at which the bulk will flush.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>bulkActions</code></td>\n' +
    '    <td>The number of bulk actions that will be pooled before a flush.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>index</code></td>\n' +
    '    <td>\n' +
    '      The index mapping to use.<br />\n' +
    '      The index mapping determines which index names will be used.<br />\n' +
    '      The following types of index mappings are available.\n' +
    '\n' +
    '      <ul>\n' +
    '        <li><a ui-sref="{\'#\': \'elasticsearch-rotating\'}">rotating</a> - a rotating index mapping, where indexes rotate over time.</li>\n' +
    '        <li><a ui-sref="{\'#\': \'elasticsearch-single\'}">single</a> - a single index mapping, where the same index is used indefinitely.</li>\n' +
    '      </ul>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>templateName</code></td>\n' +
    '    <td>The template name to use for templates created by this backend.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>clientSetup</code></td>\n' +
    '    <td>\n' +
    '      The client configuration to use.<br />\n' +
    '      The following types are available.\n' +
    '\n' +
    '      <ul>\n' +
    '        <li><a ui-sref="{\'#\': \'elasticsearch-transport\'}">transport</a> - a transport client, which connects to the cluster using the transport protocol.</li>\n' +
    '        <li><a ui-sref="{\'#\': \'elasticsearch-node\'}">node</a> - the node client, which joins the cluster as a non-data, non-leader node.</li>\n' +
    '        <li><a ui-sref="{\'#\': \'elasticsearch-standalone\'}">standalone</a> - standalone mode, which create a complete local cluster.</li>\n' +
    '      </ul>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="elasticsearch-rotating">Rotating Index Mapping</h3>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: rotating\n' +
    'pattern: metadata-%s\n' +
    'interval: 1w\n' +
    'maxReadIndices: 2\n' +
    'maxWriteIndices: 1\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Mapping that maps to the same single index.\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>single</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>pattern</code></td>\n' +
    '    <td>Pattern of the index to use, must include <code>%s</code> which will be replaced with the unix timestamp of the index.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>interval</code></td>\n' +
    '    <td>The duration at which the index will be rotated.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>maxReadIndices</code></td>\n' +
    '    <td>The of indexes to read from.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>maxWriteIndices</code></td>\n' +
    '    <td>The number of indexes to write to.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="elasticsearch-single">Single Index Mapping</h3>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: single\n' +
    'index: metadata\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Mapping that maps to the same single index.\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>single</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>index</code></td>\n' +
    '    <td>Name of the index to use.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="elasticsearch-transport">Transport Client</h3>\n' +
    '\n' +
    '<p>\n' +
    '  The transport client connects to the cluster from the outside using the transport protocol.\n' +
    '  This is the most lightweight method of interacting with the Elasticsearch cluster.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: transport\n' +
    'clusterName: heroic\n' +
    'seeds:\n' +
    '  - localhost\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>node</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>clusterName</code></td>\n' +
    '    <td>The name of the cluster to setup.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>seeds</code></td>\n' +
    '    <td>\n' +
    '      A list of seeds to connect to.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="elasticsearch-node">Node Client</h3>\n' +
    '\n' +
    '<p>\n' +
    '  A node client joins the cluster as a node.\n' +
    '  This will typically yield better performance since index lookups and aggregations can be performed without having to \'hop\' to another node.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  It is typically recommended against, due the complexity involved in the client.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: node\n' +
    'clusterName: heroic\n' +
    'seeds:\n' +
    '  - localhost\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>node</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>clusterName</code></td>\n' +
    '    <td>The name of the cluster to setup.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>seeds</code></td>\n' +
    '    <td>\n' +
    '      A list of seeds to connect to.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="elasticsearch-standalone">Standalone Client</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Standalone client setup configures a complete local Elasticsearch node.\n' +
    '  This is typically used when running a fully in-memory configuration of heroic.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: standalone\n' +
    'clusterName: heroic-standalone\n' +
    'root: /tmp/heroic-standalone\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>standalone</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>clusterName</code></td>\n' +
    '    <td>The name of the cluster to setup.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>root</code></td>\n' +
    '    <td>\n' +
    '      Root directory where indexes will be stored.\n' +
    '      If omitted, will create a temporary root directory.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '');
}]);

angular.module('_pages/docs/config/features.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/features.ngt',
    '<h2>Features Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Features are a way to modify the behaviour of your service.\n' +
    '  They are implemented as flags namespaced to heroic, an example would be:\n' +
    '  <code>com.spotify.heroic.shift_range</code>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Features can be configured either on a per-query basis, or in the\n' +
    '  configuration section <code>features</code> to apply it to all queries by\n' +
    '  default.\n' +
    '  They can either be enabled or disabled. To enable a flag, you specify its\n' +
    '  name. To disable it, you specify it\'s name prefixed with a minux sign\n' +
    '  <code>-&lt;name&gt;</code>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Precedence is defined as the following:\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>Query</li>\n' +
    '  <li>Configuration</li>\n' +
    '  <li>Default</li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  This is applied for any given feature flag.\n' +
    '  If omitted in one stage, it will be looked up in another.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A typical configuration looks something like this.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'features:\n' +
    '  - com.spotify.heroic.shift_range\n' +
    '  - -com.spotify.heroic.distributed_aggregations\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  When added to a query, they are added to the <code>.features</code> section\n' +
    '  like this:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="json">\n' +
    '{\n' +
    '  "features": [\n' +
    '    "com.spotify.heroic.shift_range",\n' +
    '    "-com.spotify.heroic.distributed_aggregations"\n' +
    '  ]\n' +
    '}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h3 id="available-features">Available Features</h3>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>This section can quickly become out of date</h4>\n' +
    '  <p>\n' +
    '    For the source of truth, look at the Feature class in heroic:\n' +
    '    <a git-href-java="heroic-component">com.spotify.heroic.common.Feature</a>\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <th>Name</th>\n' +
    '    <th>Description</th>\n' +
    '    <th>Default</th>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <td><code>com.spotify.heroic.distributed_aggregations</code></td>\n' +
    '    <td>\n' +
    '      Enable feature to perform distributed aggregations.\n' +
    '      Aggregations are commonly performed per-shard, and the result\n' +
    '      concatenated. This enables experimental support for distributed\n' +
    '      aggregations which behave transparently across shards.\n' +
    '      Typically this will cause more data to be transported across shards for\n' +
    '      each request.\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      off\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '\n' +
    '  <tr>\n' +
    '    <td><code>com.spotify.heroic.shift_range</code></td>\n' +
    '    <td>\n' +
    '      Enable feature to cause range to be rounded on the current cadence.\n' +
    '      This will assert that there are data outside of the range queried for and\n' +
    '      that the range is aligned to the queried cadence.\n' +
    '      Which is a useful feature when using a dashboarding system.\n' +
    '    </td>\n' +
    '    <td>\n' +
    '      on\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '');
}]);

angular.module('_pages/docs/config/metadata.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/metadata.ngt',
    '<h2>Configuring Metadata</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Metadata acts as the index to time series data, it is the driving force behind our <a ui-sref="^.^.query_language">Query Language</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Metadata resolution is important since it allows operators to specify a subset of known metadata, and resolve it into a set of matching time series.\n' +
    '  Without metadata, the burden of keeping track of time series would lie solely in the client.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A typical metadata configuration could look like the following.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'metadata:\n' +
    '  backends:\n' +
    '    - type: elasticsearch\n' +
    '      connection:\n' +
    '        index:\n' +
    '          type: rotating\n' +
    '          pattern: heroic-a-%s\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  <b>WARNINGS</b> There are ElasticSearch settings and mappings that must be configured before indexing operations are processed. These are required to make the reads efficient. At Spotify\n' +
    '  these settings are added when setting up the ElasticSearch cluster with Puppet.\n' +
    '\n' +
    '<a href="https://github.com/spotify/heroic/blob/7ff07a654048ce760e867835e11f230cd7c5a4ee/metadata/elasticsearch/src/main/resources/com.spotify.heroic.metadata.elasticsearch/kv/metadata.json">settings/mappings are here</a>\n' +
    '\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>backends</code></td>\n' +
    '    <td>A list of metadata backend configurations</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>defaultBackends</code></td>\n' +
    '    <td>\n' +
    '      A list of backend group names that are part of the default group.\n' +
    '      The default group is the group of backends that are used for operations unless a specified group is used.\n' +
    '      The default behaviour is to include <em>all configured backends</em> in the default group.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4>Elasticsearch</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code>"elasticsearch"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>id</code></td>\n' +
    '    <td>\n' +
    '      Specify which id this backend has that is used to uniquely identify this backend.\n' +
    '      If not specified, the id will be generated.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>groups</code></td>\n' +
    '    <td>\n' +
    '      Specify which group, or list groups, this backend should be part of.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>connection</code></td>\n' +
    '    <td>\n' +
    '      An elasticsearch connection specification.\n' +
    '      For details, see <a ui-sref="^.elasticsearch_connection">Elasticsearch Connection</a>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>writesPerSecond</code></td>\n' +
    '    <td>\n' +
    '      The number of writes this backend allows per second before rate-limiting kicks in.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>writeCacheDurationMinutes</code></td>\n' +
    '    <td>\n' +
    '      The number of minutes a write will be cached for.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>templateName</code></td>\n' +
    '    <td>\n' +
    '      Default name of the template that will be configured in elasticsearch for this backend.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>backendType</code></td>\n' +
    '    <td>\n' +
    '      Which backend configuration to use, has an effect on the schema and how tags are accessed.\n' +
    '      Currently, the following types are available.\n' +
    '\n' +
    '      <ul>\n' +
    '        <li>\n' +
    '          <b>v1</b> - which is the first generation backend schema.\n' +
    '        </li>\n' +
    '        <li>\n' +
    '          <b>kv</b> - a backend type based of a flattening the key-value context into a single array.\n' +
    '          We\'ve shown that it is more efficient, but have to support both since some users rely on the first generation schema.\n' +
    '        </li>\n' +
    '      </ul>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '  Example of what the stored metadata look like.\n' +
    '</p>\n' +
    '<codeblock language="json">\n' +
    '\n' +
    '{\'_index\': \'heroic-1535587200000\', \'_type\': \'metadata\', \'_id\': \'447939eaf69475f685518dc2c179ddaf\', \'_version\': 1, \'found\': True, \'_source\': {\'key\': \'apollo\', \'tags\': [\'component\\x00memcache-client\', \'operation\\x00get\', \'what\\x00memcache-results\'], \'tag_keys\': [\'component\', \'operation\', \'what\']}}\n' +
    '\n' +
    '\n' +
    '</codeblock>\n' +
    '');
}]);

angular.module('_pages/docs/config/metrics.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/metrics.ngt',
    '<h2>Metric Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  The metric backend is the one responsible for storing and fetching metrics to, and from various backends.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following is an example configuration:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'metrics:\n' +
    '  backends:\n' +
    '    - type: datastax\n' +
    '      groups: a\n' +
    '      seeds:\n' +
    '        - localhost\n' +
    '    - type: bigtable\n' +
    '      groups: b\n' +
    '      project: my-cloud-project-1\n' +
    '      instance: heroic-metrics\n' +
    '      credentials:\n' +
    '        type: default\n' +
    '  defaultBackends:\n' +
    '    - a\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>backends</code></td>\n' +
    '    <td>A list of metric backend configurations</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>defaultBackends</code></td>\n' +
    '    <td>\n' +
    '      A list of backend group names that are part of the default group.\n' +
    '      The default group is the group of backends that are used for operations unless a specified group is used.\n' +
    '      The default behaviour is to include <em>all configured backends</em> in the default group.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>groupLimit</code></td>\n' +
    '    <td>\n' +
    '      A number indicating the maximum number of distinct groups a single result group may contains.\n' +
    '      The default limit is 500.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>seriesLimit</code></td>\n' +
    '    <td>\n' +
    '      A number indicating the maximum amount of time series a single request is allowed to fetch.\n' +
    '      The default limit is 10000.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>aggregationLimit</code></td>\n' +
    '    <td>\n' +
    '      A number indicating the maximum number of data points a single aggregation is allowed to output.\n' +
    '      The default limit is 10000.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>dataLimit</code></td>\n' +
    '    <td>\n' +
    '      A number indicating the maximum number of samples a single request may fetch from the backends.\n' +
    '      The default limit is 20000000 (20 million).\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>fetchParallelism</code></td>\n' +
    '    <td>\n' +
    '      A number indicating how many fetches are allowed to be performed <em>in parallel</em> for each request.\n' +
    '      The default value is 100.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h3 id="datastax-cassandra">Datastax Cassandra</h3>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code>"datastax"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>id</code></td>\n' +
    '    <td>\n' +
    '      Specify which id this backend has that is used to uniquely identify this backend.\n' +
    '      If not specified, the id will be generated.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>groups</code></td>\n' +
    '    <td>\n' +
    '      Specify which group, or list groups, this backend should be part of.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>seeds</code></td>\n' +
    '    <td>\n' +
    '      A list of seed hosts to use when connecting to a C* cluster.\n' +
    '      Defaults to <code language="json">["localhost"]</code>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>schema</code></td>\n' +
    '    <td>\n' +
    '      Configures which data schema to use.\n' +
    '      Defaults to <a ui-sref="{\'#\': \'schema-ng\'}">Next Generation</a>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4 id="schema-ng">Next Generation Schema</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code language="json">"ng"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>keyspace</code></td>\n' +
    '    <td>Name of the keyspace to use.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '  The next version of the schema, built with no strings attached to previous implementations.\n' +
    '</p>\n' +
    '\n' +
    '<h4 id="schema-legacy">Legacy Schema</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code language="json">"legacy"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>keyspace</code></td>\n' +
    '    <td>Name of the keyspace to use.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    '  The initial schema that was used since the dawn of time.\n' +
    '  It was inherited, in part, from some of the key composition utilities made available by <em>datastax</em>, making it a bit awkward to work with for other utilities.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="google-bigtable">Google Bigtable</h3>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: bigtable\n' +
    'project: my-cloud-project-1\n' +
    'instance: heroic-metrics\n' +
    'credentials:\n' +
    '  type: default\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code>"bigtable"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>id</code></td>\n' +
    '    <td>\n' +
    '      Specify which id this backend has that is used to uniquely identify this backend.\n' +
    '      If not specified, the id will be generated.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>groups</code></td>\n' +
    '    <td>\n' +
    '      Specify which group, or list groups, this backend should be part of.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>project</code></td>\n' +
    '    <td>\n' +
    '      Which <a href="https://cloud.google.com/storage/docs/projects">Project</a> the backend should connect to.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>instance</code></td>\n' +
    '    <td>\n' +
    '      Which <a href="https://cloud.google.com/bigtable/docs/creating-instance">Instance</a> the backend should connect to.\n' +
    '      Defaults to <code>"heroic"</code>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>table</code></td>\n' +
    '    <td>\n' +
    '      Which table the backend should use. Defaults to <code>"metrics"</code>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>credentials</code></td>\n' +
    '    <td>\n' +
    '      Which credentials to use to connect.\n' +
    '      Can be one of:\n' +
    '\n' +
    '      <ul>\n' +
    '        <li><a ui-sref="{\'#\':\'json-credentials\'}">JSON Credentials</a></li>\n' +
    '        <li><a ui-sref="{\'#\':\'compute-engine-credentials\'}">Compute Engine Credentials</a></li>\n' +
    '      </ul>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>disableBulkMutations</code></td>\n' +
    '    <td>\n' +
    '      Disable bulk mutations.\n' +
    '      This will cause each individual write to be performed as a single request.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>flushIntervalSeconds</code></td>\n' +
    '    <td>\n' +
    '      When bulk mutations are enabled, this is the maximum amount of time a single batch will collect data for.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>batchSize</code></td>\n' +
    '    <td>\n' +
    '      When bulk mutations are enabled, this is the maximum size of a single batch.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4 id="compute-engine-credentials">Compute Engine Credentials</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>compute-engine</b></td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4>Default Credentials</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>default</b></td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4 id="json-credentials">JSON Credentials</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><b>json</b></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>path</code></td>\n' +
    '    <td>\n' +
    '      Path to credentials file to use.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<p>\n' +
    'A note on sending metrics with the same timestamp and/or duplicate metrics.\n' +
    '\n' +
    'These metric values will not be duplicated within the row, since Heroic is mutating rows and not appending to the column family. In bigtable each \n' +
    'timestamp + value is a column within the row.\n' +
    '\n' +
    '<img style="width: 100%;" src="images/row-mutation.png"></img>\n' +
    '\n' +
    '</p>\n' +
    '');
}]);

angular.module('_pages/docs/config/query_logging.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/query_logging.ngt',
    '<h2>Query Logging Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  This section documents how to configure query logging. I.e. detailed logging of internal Heroic state at different stages of query processing.\n' +
    '</p>\n' +
    '\n' +
    '<h3>Configuration file</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Query Logging configuration is located in the <code>queryLogging</code> section of the configuration.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  A typical configuration looks something like this.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'queryLogging:\n' +
    '  type: slf4j\n' +
    '  name: "com.spotify.heroic.query_logging"\n' +
    '</codeblock>\n' +
    '\n' +
    '<h4>Field definitions</h4>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'type: slf4j\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Defines which type of logger that should be used for query logging. Currently only supports \'slf4j\'. Without this line, query logging is disabled.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'name: "com.spotify.heroic.query_logging"\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Defines the Slf4j logger to use when logging. A matching logger needs to be defined in the Slf4j configuration file to actually get some output.\n' +
    '</p>\n' +
    '\n' +
    '\n' +
    '<h3>Contextual information</h3>\n' +
    '\n' +
    '<p>\n' +
    '  It\'s possible to supply contextual information in the query. This information will then be included in the query log, to ease mapping of performed query to the query log output.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Add the following clientContext snippet to the query:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '{\n' +
    '  "clientContext": {\n' +
    '    "dashboardId": "my-system-metrics",\n' +
    '    "user": "my-user"\n' +
    '  }\n' +
    '  "filter": ...\n' +
    '}\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  You\'ll get the following output in the query log:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '{\n' +
    '  "component": ...,\n' +
    '  "queryId": "ed6fe51c-afba-4320-a859-a88795c15175",\n' +
    '  "clientContext": {\n' +
    '    "dashboardId": "my-system-metrics",\n' +
    '    "user": "my-user"\n' +
    '  },\n' +
    '  "type": ...,\n' +
    '  "data": ...\n' +
    '}\n' +
    '</codeblock>\n' +
    '\n' +
    '\n' +
    '<h3>Query log output</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Each successful query will result in several output entries in the query log. Entries from different stages of the query. Example output:\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '{\n' +
    '  "component": ...,\n' +
    '  "queryId": "ed6fe51c-afba-4320-a859-a88795c15175",\n' +
    '  "clientContext": {\n' +
    '    "dashboardId": "my-system-metrics",\n' +
    '    "user": "my-user"\n' +
    '  },\n' +
    '  "type": ...,\n' +
    '  "data": ...\n' +
    '}\n' +
    '</codeblock>\n' +
    '\n' +
    '<h4>Field definitions</h4>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '  "component": ...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Specifies the internal component in Heroic that outputs this query log output.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '  "queryId": ...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Generated id that is unique for this particular query. Can be used to group query log entries together. The queryId is also returned in the final query response.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '  "clientContext": ...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  The contextual information supplied by user. See the Contextual Information section above.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '  "type": ...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Specifies the query stage at which this particular query log entry was generated.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '  "data": ...\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  Contains data relevant to this query stage. This might for example be the original query, a partial response or the final response.\n' +
    '</p>\n' +
    '\n' +
    '');
}]);

angular.module('_pages/docs/config/shell_server.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/shell_server.ngt',
    '<h2>Shell Server Configuration</h2>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic supports that <a ui-sref="^.^.shell">remote shells</a> establish connections to perform administration.\n' +
    '  See the <a ui-sref="^.^.shell">Shell</a> section for more information.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'shellServer:\n' +
    '  host: "localhost"\n' +
    '  port: 9190\n' +
    '</codeblock>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>host</code></td>\n' +
    '    <td>Host name of the interface to bind to.</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>port</code></td>\n' +
    '    <td>Port number to bind to.</td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '');
}]);

angular.module('_pages/docs/config/suggest.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/config/suggest.ngt',
    '<h2>Configuring Suggestions</h2>\n' +
    '\n' +
    '<p>\n' +
    '  The ability to perform suggestions is an important usability feature.\n' +
    '  It makes the difference for your system to be a complete black box, to giving your developers the ability to find and make use of time series on their own.\n' +
    '  Suggests are fairly expensive in terms of data storage and indexing operations. As each tag that is part of a metric is indexed. \n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Suggestions is an optional feature of heroic.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  This section will document how suggestions are configured in heroic.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    'suggest:\n' +
    '  backends:\n' +
    '    - type: elasticsearch\n' +
    '      connection:\n' +
    '        index:\n' +
    '          type: rotating\n' +
    '          pattern: heroic-a-%s\n' +
    '</codeblock>\n' +
    '\n' +
    '\n' +
    '<p>\n' +
    '  <b>WARNINGS</b> There are ElasticSearch settings and mappings that must be configured before indexing operations are processed. These are required to make the reads efficient. At Spotify\n' +
    '  these settings are added when setting up the ElasticSearch cluster with Puppet.\n' +
    '\n' +
    '<a href="https://github.com/spotify/heroic/tree/7ff07a654048ce760e867835e11f230cd7c5a4ee/suggest/elasticsearch/src/main/resources/com.spotify.heroic.suggest.elasticsearch/kv">\n' +
    '  settings/mappings are here</a>\n' +
    '</p>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>backends</code></td>\n' +
    '    <td>A list of suggest backend configurations</td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>defaultBackends</code></td>\n' +
    '    <td>\n' +
    '      A list of backend group names that are part of the default group.\n' +
    '      The default group is the group of backends that are used for operations unless a specified group is used.\n' +
    '      The default behavior is to include <em>all configured backends</em> in the default group.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '<h4>Elasticsearch</h4>\n' +
    '\n' +
    '<table class="table">\n' +
    '  <tr>\n' +
    '    <td><code>type</code></td>\n' +
    '    <td><code>"elasticsearch"</code></td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>id</code></td>\n' +
    '    <td>\n' +
    '      Specify which id this backend has that is used to uniquely identify this backend.\n' +
    '      If not specified, the id will be generated.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>groups</code></td>\n' +
    '    <td>\n' +
    '      Specify which group, or list groups, this backend should be part of.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>connection</code></td>\n' +
    '    <td>\n' +
    '      An elasticsearch connection specification.\n' +
    '      For details, see <a ui-sref="^.elasticsearch_connection">Elasticsearch Connection</a>.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>writesPerSecond</code></td>\n' +
    '    <td>\n' +
    '      The number of writes this backend allows per second before rate-limiting kicks in.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>writeCacheDurationMinutes</code></td>\n' +
    '    <td>\n' +
    '      The number of minutes a write will be cached for.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>templateName</code></td>\n' +
    '    <td>\n' +
    '      Default name of the template that will be configured in elasticsearch for this backend.\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '  <tr>\n' +
    '    <td><code>backendType</code></td>\n' +
    '    <td>\n' +
    '      Which backend configuration to use, has an effect on the schema and how tags are accessed.\n' +
    '      Currently, the following types are available.\n' +
    '\n' +
    '      <ul>\n' +
    '        <li>\n' +
    '          <b>v1</b> - which is the first generation backend schema.\n' +
    '        </li>\n' +
    '        <li>\n' +
    '          <b>kv</b> - a backend type based of a flattening the key-value context into a single array.\n' +
    '          We\'ve shown that it is more efficient, but have to support both since some users rely on the first generation schema.\n' +
    '        </li>\n' +
    '      </ul>\n' +
    '    </td>\n' +
    '  </tr>\n' +
    '</table>\n' +
    '\n' +
    '\n' +
    '<p>\n' +
    '  Example of what the stored suggestions look like.\n' +
    '</p>\n' +
    '<codeblock language="json">\n' +
    '\n' +
    '{\'_index\': \'heroic-1536192000000\', \'_type\': \'series\', \'_id\': \'447939eaf69475f685518dc2c179ddaf\', \'_version\': 1, \'found\': True, \'_source\': {\'key\': \'apollo\', \'tags\': [\'component\\x00memcache-client\', \'operation\\x00get\', \'what\\x00memcache-results\'], \'tag_keys\': [component\', \'operation\', \'what\'], \'series_id\': \'447939eaf69475f685518dc2c179ddaf\'}}\n' +
    '\n' +
    '{\'_index\': \'heroic-1536192000000\', \'_type\': \'tag\', \'_id\': \'447939eaf69475f685518dc2c179ddaf:687d7854\', \'_version\': 1, \'found\': True, \'_source\': {\'key\': \'apollo\', \'tags\': [\'component\\x00memcache-client\', \'what\\x00memcache-results\', \'operation\\x00get\'], \'tag_keys\': [\'component\', \'what\', \'operation\'], \'series_id\': \'447939eaf69475f685518dc2c179ddaf\', \'skey\': \'component\', \'sval\': \'memcache-client\', \'kv\': \'component\\x00memcache-client\'}}\n' +
    '\n' +
    '</codeblock>\n' +
    '');
}]);

angular.module('_pages/docs/getting_started/compile.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/getting_started/compile.ngt',
    '<h2>Compile Heroic</h2>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>\n' +
    '    Unstable Project\n' +
    '  </h4>\n' +
    '  <p>\n' +
    '    In order to run Heroic you currently have to compile your own jar.\n' +
    '    This is because the project should still be considered unstable, and might change in the future.\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<p>\n' +
    '  In order to compile Heroic, you\'ll need a working <a href="http://www.oracle.com/technetwork/java/javase/downloads/index.html">Java 8 JDK</a>, and <a href="https://maven.apache.org/">Maven 3</a>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Get the Heroic source from GitHub:\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    '$ git clone https://github.com/spotify/heroic.git\n' +
    '</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Go to the cloned directory and run Maven:\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    '$ cd heroic\n' +
    '$ tools/install-repackaged\n' +
    '$ mvn clean package\n' +
    '</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  The previous command will generate a new .jar file in <b>heroic-dist/target</b>, which you can\n' +
    '  use to run Heroic through <a git-href-java="heroic-dist">com.spotify.heroic.HeroicService</a>.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    '$ java -cp heroic-dist/target/heroic-dist-0.0.1-SNAPSHOT-shaded.jar com.spotify.heroic.HeroicService &lt;config&gt;\n' +
    '</code></pre>\n' +
    '');
}]);

angular.module('_pages/docs/getting_started/configuration.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/getting_started/configuration.ngt',
    '<h2>Configure Cluster</h2>\n' +
    '\n' +
    '<p>\n' +
    '  In this section we\'ll be configuring all the components that make up a Heroic\n' +
    '  cluster.\n' +
    '  These should have been installed in the <a ui-sref="^.installation">previous step</a>.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref=".({\'#\': \'cassandra\'})">Configuring Cassandra</a></li>\n' +
    '  <li><a ui-sref=".({\'#\': \'bigtable\'})">Configuring Bigtable</a></li>\n' +
    '  <li><a ui-sref=".({\'#\': \'elasticsearch\'})">Configuring Elasticsearch</a></li>\n' +
    '  <li><a ui-sref=".({\'#\': \'heroic\'})">Configuring Heroic</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<h3 id="cassandra">Configuring Cassandra</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic (by default) uses the <code>heroic</code> keyspace, which can be\n' +
    '  configured using the <a ui-sref="^.^.shell">Heroic shell</a>.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    'tools/heroic-shell -P cassandra -X cassandra.seeds=&lt;seeds&gt; -X datastax.configure\n' +
    '...\n' +
    'heroic> configure\n' +
    '</code></pre>\n' +
    '\n' +
    '<h3 id="bigtable">Configuring Bigtable</h3>\n' +
    '\n' +
    '<p>\n' +
    '  If you want to use Google Cloud Bigtable to store metrics, you can configure it with the following command.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    'tools/heroic-shell -P bigtable \\\n' +
    '  -X bigtable.project=&lt;project&gt; \\\n' +
    '  -X bigtable.instance=&lt;instance&gt; \\\n' +
    '  -X bigtable.credentials=default \\\n' +
    '  -X bigtable.configure\n' +
    '...\n' +
    'heroic> configure\n' +
    '</code></pre>\n' +
    '\n' +
    '<h3 id="elasticsearch">Configuring ElasticSearch</h3>\n' +
    '\n' +
    '<p>\n' +
    '  Elasticsearch is also configured using the <a ui-sref="^.^.shell">Heroic shell</a>.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    'tools/heroic-shell -P elasticsearch-suggest -P elasticsearch-metadata -X elasticsearch.seeds=&lt;seeds&gt;\n' +
    '...\n' +
    'heroic> configure\n' +
    '</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Heroic suggest also requires dynamic scripting to be enabled, you do this by\n' +
    '  adding the following to your elasticsearch.yml\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="yaml">\n' +
    'script:\n' +
    '  disable_dynamic: false\n' +
    '</pre></code>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>Enabling Dynamic Scripting</h4>\n' +
    '  <p>\n' +
    '    Enabling dynamic scripting for a public Elasticsearch instance is a\n' +
    '    security risk. Make sure that your server is on a private network.\n' +
    '    See <a href="https://github.com/elastic/elasticsearch/issues/10091">issue #10091</a>\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<h3 id="heroic">Configuring Heroic</h3>\n' +
    '\n' +
    '<p>\n' +
    '  The following configuration assumes that you\'ve managed to either download or\n' +
    '  build the heroic project.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  We will look into setting up three different <em>kinds</em> of heroic nodes,\n' +
    '  each with a different purpose.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li>An <a href="#heroic-api-node">API Node</a>, designated to receive user traffic</li>\n' +
    '  <li>A <a href="#heroic-data-node">Data Node</a>, designated to process requests received by the API nodes</li>\n' +
    '  <li>A <a href="#heroic-consumer-node">Consumer Node</a>, designated to consume data from Kafka topics</li>\n' +
    '</ul>\n' +
    '\n' +
    '<p>\n' +
    '  All the different node types run the same code, the only difference is how\n' +
    '  they are configured.\n' +
    '</p>\n' +
    '\n' +
    '<div class="callout callout-info">\n' +
    '  <h4>One, or more roles?</h4>\n' +
    '  <p>\n' +
    '    All the nodes use the same type of configuration, but they are configured\n' +
    '    to do different things.\n' +
    '    A single node can have one or more role, the exact setup depends on how you\n' +
    '    wish to scale the system.\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<h4 id="heroic-api-node">API Node</h4>\n' +
    '\n' +
    '<p>\n' +
    '  API nodes are nodes designated to receive user traffic over HTTP.\n' +
    '  The following is the minimal required configuration in order to set one up.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="yaml">\n' +
    '# heroic.yaml\n' +
    'port: 8080\n' +
    '\n' +
    'cluster:\n' +
    '  protocols:\n' +
    '    - type: grpc\n' +
    '  discovery:\n' +
    '    type: static\n' +
    '    nodes:\n' +
    '      - "grpc://localhost"\n' +
    '</pre></code>\n' +
    '\n' +
    '<h4 id="heroic-data-node">Data Node</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Data nodes are designated to process requests for the API nodes.\n' +
    '  They are responsible for fetching and aggregating metrics from a backend.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Data nodes must have the <code>QUERY</code> node capability, as seen in\n' +
    '  <code>cluster.capabilities</code>.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The following configuration shows how to setup a data nodes that fetches\n' +
    '  metrics from Cassandra, and uses Elasticsearch for metadata and suggestions.\n' +
    '</p>\n' +
    '\n' +
    '<codeblock language="yaml">\n' +
    '# heroic.yaml\n' +
    'port: 8080\n' +
    '\n' +
    'cluster:\n' +
    '  protocols:\n' +
    '    - type: grpc\n' +
    '  discovery:\n' +
    '    type: static\n' +
    '    nodes:\n' +
    '      - "grpc://localhost"\n' +
    '\n' +
    '# This showcases two different metric backends, choose which one you want.\n' +
    'metrics:\n' +
    '  backends:\n' +
    '    - type: datastax\n' +
    '      seeds:\n' +
    '        - localhost\n' +
    '    - type: bigtable\n' +
    '      instance: heroic\n' +
    '      credentials:\n' +
    '        type: default\n' +
    '\n' +
    'metadata:\n' +
    '  backends:\n' +
    '    - type: elasticsearch\n' +
    '      connection:\n' +
    '        seeds:\n' +
    '          - localhost\n' +
    '\n' +
    'suggest:\n' +
    '  backends:\n' +
    '    - type: elasticsearch\n' +
    '      connection:\n' +
    '        seeds:\n' +
    '          - localhost\n' +
    '</codeblock>\n' +
    '\n' +
    '<p>\n' +
    '  For more details in how the service can be configured, see the <a ui-sref="^.^.config">Configuration Section</a>.\n' +
    '</p>\n' +
    '\n' +
    '<h4 id="heroic-data-node">Consumer Node</h4>\n' +
    '\n' +
    '<p>\n' +
    '  A consumer node is responsible for reading data from kafka, and writing it\n' +
    '  into the configured backends.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  The below configuration is just an extension to the data node configuration\n' +
    '  above, by adding consumers you are instructing the service to act as a\n' +
    '  consumer.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="yaml">\n' +
    '# heroic.yaml\n' +
    'port: 8080\n' +
    '\n' +
    '# Data Node configuration...\n' +
    '\n' +
    'consumers:\n' +
    '  - type: kafka\n' +
    '    schema: com.spotify.heroic.consumer.schemas.Spotify100\n' +
    '    topics:\n' +
    '      - "metrics"\n' +
    '    config:\n' +
    '      group.id: heroic-consumer\n' +
    '      zookeeper.connect: localhost\n' +
    '      auto.offset.reset: smallest\n' +
    '      auto.commit.enable: true\n' +
    '</pre></code>\n' +
    '\n' +
    '<h4>Run a Heroic Node</h4>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    '$ java -cp heroic.jar com.spotify.heroic.HeroicService heroic.yaml\n' +
    '</code></pre>\n' +
    '\n' +
    '<h4>Testing that a node Works</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Test that the node is running properly.\n' +
    '  Healthy instances should produce a output like the following.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">\n' +
    '$ curl http://localhost:8080/status\n' +
    '</code><code language="json">\n' +
    '{\n' +
    '  "ok": true,\n' +
    '  "consumers": {\n' +
    '    "ok": true,\n' +
    '    "available": 0,\n' +
    '    "ready": 0,\n' +
    '    "errors": 0\n' +
    '  },\n' +
    '  "backends": {\n' +
    '    "ok": true,\n' +
    '    "available": 0,\n' +
    '    "ready": 0\n' +
    '  },\n' +
    '  "metadataBackends": {\n' +
    '    "ok": true,\n' +
    '    "available": 0,\n' +
    '    "ready": 0\n' +
    '  },\n' +
    '  "cluster": {\n' +
    '    "ok": true,\n' +
    '    "onlineNodes": 1,\n' +
    '    "offlineNodes": 0\n' +
    '  }\n' +
    '}\n' +
    '</code></pre>\n' +
    '');
}]);

angular.module('_pages/docs/getting_started/installation.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_pages/docs/getting_started/installation.ngt',
    '<h2>Install Dependencies</h2>\n' +
    '\n' +
    '<p>\n' +
    '  For getting start, you\'ll need the following systems in place.\n' +
    '</p>\n' +
    '\n' +
    '<ul>\n' +
    '  <li><a ui-sref=".({\'#\': \'cassandra\'})">A Cassandra 2.x cluster</a></li>\n' +
    '  <li><a ui-sref=".({\'#\': \'elasticsearch\'})">An Elasticsearch cluster</a></li>\n' +
    '  <li><a ui-sref=".({\'#\': \'kafka\'})">A Kafka Cluster</a></li>\n' +
    '</ul>\n' +
    '\n' +
    '<div class="callout callout-danger">\n' +
    '  <h4>This page does not describe industry best practices</h4>\n' +
    '  <p>\n' +
    '    You should consider the result of this an <em>experiment</em>, it is not recommended to operate the system like this in a production infrastructure.\n' +
    '  </p>\n' +
    '</div>\n' +
    '\n' +
    '<h3>\n' +
    '</h3>\n' +
    '\n' +
    '<h3 id="cassandra">\n' +
    '  Cassandra 2.x\n' +
    '<h3>\n' +
    '\n' +
    '<h4>\n' +
    '  <img src="images/debian.25.png"></img>\n' +
    '  Install on Debian\n' +
    '</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Follow <a href="https://wiki.apache.org/cassandra/DebianPackaging">instructions on the Cassandra Wiki</a>\n' +
    '</p>\n' +
    '\n' +
    '<h4>\n' +
    '  <img src="images/tux.25.png"></img>\n' +
    '  Other Linux\n' +
    '</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Download .tar.gz from <a href="https://cassandra.apache.org/download/">https://cassandra.apache.org/download/</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Unpack and run (replace <code>&lt;version&gt;</code> with the actual 2.x version)\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">tar -xvf apache-cassandra-&lt;version&gt;-bin.tar.gz\n' +
    'cd apache-cassandra-&lt;version&gt;</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Run cassandra (<code language="bash">-f</code> keeps it in the foreground)\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">bin/cassandra -f</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Next you should read how to <a ui-sref="^.configuration({\'#\': \'cassandra\'})">configure cassandra</a>.\n' +
    '</p>\n' +
    '\n' +
    '<h3 id="elasticsearch">\n' +
    '  Elasticsearch\n' +
    '<h3>\n' +
    '\n' +
    '<h4>\n' +
    '  <img src="images/debian.25.png"></img>\n' +
    '  Debian\n' +
    '</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Follow instructions on <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-repositories.html">https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-repositories.html</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  After you have the package installed, edit the configuration file <em>/etc/default/elasticsearch</em> and uncomment the lines defining variables LOG_DIR, DATA_DIR, WORK_DIR, CONF_DIR and CONF_FILE.\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Now you may start the service:\n' +
    '<pre><code language="bash">sudo systemctl start elasticsearch</code></pre>\n' +
    '</p>\n' +
    '\n' +
    '<h4>\n' +
    '  <img src="images/tux.25.png"></img>\n' +
    '  Generic Linux\n' +
    '</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Download .tar.gz from <a href="https://www.elastic.co/downloads/elasticsearch">https://www.elastic.co/downloads/elasticsearch</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Unpack and run (replace <code>&lt;version&gt;</code> with the actual version)\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">tar -xvf elasticsearch-&lt;version&gt;-bin.tar.gz\n' +
    'cd elasticsearch-&lt;version&gt;</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Run Elasticsearch\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">bin/elasticsearch</code></pre>\n' +
    '\n' +
    '<h3 id="kafka">Kafka</h3>\n' +
    '\n' +
    '<h4>\n' +
    '  <img src="images/tux.25.png"></img>\n' +
    '  Generic Linux\n' +
    '</h4>\n' +
    '\n' +
    '<p>\n' +
    '  Download .tgz from <a href="http://kafka.apache.org/downloads.html">http://kafka.apache.org/downloads.html</a>\n' +
    '</p>\n' +
    '\n' +
    '<p>\n' +
    '  Unpack and run (replace <code>&lt;scala-version&gt;</code>, and <code>&lt;version&gt;</code> with the actual versions)\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">tar -xvf kafka_&lt;scala-version&gt;-&lt;version&gt;.tgz\n' +
    'cd kafka_&lt;scala-version&gt;-&lt;version&gt;</code></pre>\n' +
    '\n' +
    '<p>\n' +
    '  Run ZooKeeper and Kafka.\n' +
    '</p>\n' +
    '\n' +
    '<pre><code language="bash">bin/zookeeper-server-start.sh config/zookeeper.properties\n' +
    'bin/kafka-server-start.sh config/server.properties</code></pre>\n' +
    '');
}]);

angular.module('_js/api-accept.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_js/api-accept.ngt',
    '<div class="api-accept">\n' +
    '  <span class="icon glyphicon glyphicon-upload" title="Accept"></span>\n' +
    '  <span ng-if="isEmpty">\n' +
    '    <em>Does not accept a body</em>\n' +
    '    <span\n' +
    '        class="glyphicon glyphicon-question-sign"\n' +
    '        popover="Sending a body will cause a 400 error to be returned"\n' +
    '        popover-title="Request body is not supported"\n' +
    '        popover-trigger="mouseenter"></span>\n' +
    '  </span>\n' +
    '\n' +
    '  <span ng-if="!isEmpty" class="content-type">{{contentType}}</span>\n' +
    '\n' +
    '  <div ng-if="!isEmpty" class="accept-doc"><ng-transclude /></div>\n' +
    '\n' +
    '  <div ng-if="curl !== null">\n' +
    '    <h4>Example Curl</h4>\n' +
    '    <codeblock language="bash" content="curl"></codeblock>\n' +
    '  </div>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_js/api-endpoint.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_js/api-endpoint.ngt',
    '<div class="api-endpoint">\n' +
    '  <h3 class="heading" ng-class="{\'post\': method === \'POST\', \'get\': method === \'GET\', \'put\': method === \'PUT\', \'delete\': method === \'DELETE\'}">\n' +
    '    <span class="method">{{method}}</span>: <span class="path">{{path}}</span>\n' +
    '  </h3>\n' +
    '\n' +
    '  <ng-transclude></ng-transclude>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_js/api-response.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_js/api-response.ngt',
    '<div class="api-response">\n' +
    '  <span class="icon glyphicon glyphicon-download" title="Response {{status}}"></span>\n' +
    '  <span class="status">Status: {{status}},</span>\n' +
    '  <span class="content-type">Content-Type: {{contentType}}</span>\n' +
    '\n' +
    '  <div class="response-doc"><ng-transclude /></div>\n' +
    '</div>\n' +
    '');
}]);

angular.module('_js/api-type.ngt', []).run(['$templateCache', function($templateCache) {
  $templateCache.put('_js/api-type.ngt',
    '<div class="api-type">\n' +
    '  <h3 ng-if="!!name">{{name}}</h3>\n' +
    '\n' +
    '  <div class="doc">\n' +
    '    <ng-transclude></ng-transclude>\n' +
    '  </div>\n' +
    '\n' +
    '  <div class="structure-doc">\n' +
    '    <h4 ng-if="!structural">Structure</h4>\n' +
    '\n' +
    '    <div class="table-responsive">\n' +
    '    <table class="table table-bordered">\n' +
    '      <tr>\n' +
    '        <th ng-if="!structural">Field</th>\n' +
    '        <th ng-if="!structural">Type</th>\n' +
    '        <th ng-if="structural">Structure</th>\n' +
    '        <th>Purpose</th>\n' +
    '      </tr>\n' +
    '\n' +
    '      <tr ng-repeat="f in fields">\n' +
    '        <td ng-if="!structural">\n' +
    '          <code>{{f.name}}</code>&nbsp;<small ng-if="f.required">\n' +
    '            <em class="sr-only">required</em>\n' +
    '            <span class="glyphicon glyphicon-check" popover="required field" popover-trigger="mouseenter"></span>\n' +
    '          </small>\n' +
    '        </td>\n' +
    '        <td api-field-bind="f.type"></td>\n' +
    '        <td api-field-bind="f.purpose"></td>\n' +
    '      </tr>\n' +
    '    </table>\n' +
    '    </div>\n' +
    '  </div>\n' +
    '</div>\n' +
    '');
}]);

