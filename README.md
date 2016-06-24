## ElasticSearch Plugin for Riemann

This plugin sends ElasticSearch node metrics to [Riemann](http://riemann.io/) in near real time (interval can be configured)


## Installation

bin/plugin install https://github.com/searchly/elasticsearch-monitoring-riemann-plugin/releases/download/elasticsearch-riemann-plugin-2.2.2/elasticsearch-riemann-plugin-2.2.2.zip

## Configuration

```
metrics:
    riemann:
        every: 3s
        host: "localhost"
        tags: "production"
```
