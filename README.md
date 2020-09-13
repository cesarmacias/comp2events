# comp2events
Elasticsearch Aggregation composite transform to event format (simple JSON line) for insert again to Elasticsearch

# Considerations
* The aggregations names are converted to field name in the object event
* Only one aggregation level for composite is suppoterd
* All aggs for response have to be part of one sub-aggs
* The program is for roll-up time series data to aggregation per day
* The file cm-json is a example of DSL query

# Run
node aggs.js --config=config.json
