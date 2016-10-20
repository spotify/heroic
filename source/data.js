function endpoint(method, id, url) {
  return {method: method, id: id, url: url, sref: method.toLowerCase() + '-' + id};
}

function type(id, name) {
  return {id: id, name: name};
}

var endpoints = [
  endpoint('GET', 'status', '/status'),
  endpoint('POST', 'metadata-keys', '/metadata/keys'),
  endpoint('POST', 'metadata-series-count', '/metadata/series-count'),
  endpoint('POST', 'metadata-series', '/metadata/series'),
  endpoint('PUT', 'metadata-series', '/metadata/series'),
  endpoint('DELETE', 'metadata-series', '/metadata/series'),
  endpoint('POST', 'metadata-tags', '/metadata/tags'),
  endpoint('POST', 'write', '/write'),
  endpoint('POST', 'query-metrics', '/query/metrics'),
  endpoint('POST', 'query-batch', '/query/batch')
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
  type('statistics', 'Statistics')
];

