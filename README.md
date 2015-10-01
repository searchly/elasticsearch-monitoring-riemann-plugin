## ElasticSearch Plugin for Riemann

This plugin sends ElasticSearch node metrics to [Riemann](http://riemann.io/) in near real time (interval can be configured)


## Installation

bin/plugin -url https://github.com/searchly/elasticsearch-monitoring-riemann-plugin/releases/download/elasticsearch-riemann-plugin-1.7.2/elasticsearch-riemann-plugin-1.7.2.zip  -install riemann

## Configuration

```
metrics:
    riemann:
        every: 3000
        host: "localhost"
        tags: "production"
```
