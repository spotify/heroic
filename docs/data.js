function endpoint(method, id, url, params) {
  var help = params.help || '';

  return {
    method: method,
    id: id,
    url: url,
    sref: method.toLowerCase() + '-' + id,
    help: help
  };
}

function type(id, name) {
  return {id: id, name: name};
}

var endpoints = [
  endpoint('GET', 'status', '/status', {
    help: 'Get the state of the service'
  }),
  endpoint('POST', 'write', '/write', {
    help: "Write metrics"
  }),
  endpoint('POST', 'query-metrics', '/query/metrics', {
    help: "Query for metrics"
  }),
  endpoint('POST', 'metadata-keys', '/metadata/keys', {
    help: "Search for time series keys"
  }),
  endpoint('POST', 'metadata-series-count', '/metadata/series-count', {
    help: "Count time series metadata"
  }),
  endpoint('POST', 'metadata-series', '/metadata/series', {
    help: "Search for time series metadata"
  }),
  endpoint('PUT', 'metadata-series', '/metadata/series', {
    help: "Insert time series metadata"
  }),
  endpoint('DELETE', 'metadata-series', '/metadata/series', {
    help: "Delete time series metadata"
  }),
  endpoint('POST', 'metadata-tags', '/metadata/tags', {
    help: "Search for tags metadata"
  }),
  endpoint('POST', 'metadata-key-suggest', '/metadata/key-suggest', {
    help: "Search for key suggestions"
  }),
  endpoint('POST', 'metadata-tag-suggest', '/metadata/tag-suggest', {
    help: "Search for tag suggestions"
  }),
  endpoint('POST', 'metadata-tag-value-suggest', '/metadata/tag-value-suggest', {
    help: "Search for tag value suggestions"
  }),
  endpoint('POST', 'query-batch', '/query/batch', {
    help: "Perform a batch query"
  })
];

var types = [
  type('aggregation', 'Aggregation'),
  type('event', 'Event'),
  type('filter', 'Filter'),
  type('metric-collection', 'MetricCollection'),
  type('point', 'Point'),
  type('query-date-range', 'QueryDateRange'),
  type('request-error', 'RequestError'),
  type('sampling-query', 'SamplingQuery'),
  type('series', 'Series'),
  type('sharded-result-group', 'ShardedResultGroup'),
  type('statistics', 'Statistics'),
  type('match-options', 'MatchOptions')
];

