## ElasticSearch Plugin for Riemann

This plugin sends ElasticSearch node metrics to [Riemann](http://riemann.io/) in near real time (interval can be configured)


## Installation

bin/plugin install https://github.com/searchly/elasticsearch-monitoring-riemann-plugin/releases/download/elasticsearch-riemann-plugin-2.3.3/elasticsearch-riemann-plugin-2.3.3.zip

## Configuration

```
metrics:
    riemann:
        every: 3s
        host: "localhost"
        tags: "production"
```
