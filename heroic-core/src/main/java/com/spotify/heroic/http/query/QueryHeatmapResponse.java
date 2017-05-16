package com.spotify.heroic.http.query;

/**
 * Created by lucile on 09/05/17.
 */

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ResultLimits;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import lombok.Data;
import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

@Data
@JsonSerialize(using = QueryHeatmapResponse.Serializer.class)
public class QueryHeatmapResponse {
    @NonNull
    private final DateRange range;

    @NonNull
    private final List<ShardedResultGroup> result;

    @NonNull
    private final Statistics statistics = Statistics.empty();

    @NonNull
    private final List<RequestError> errors;

    @NonNull
    private final QueryTrace trace;

    @NonNull
    private final ResultLimits limits;

    public static class Serializer extends JsonSerializer<QueryHeatmapResponse> {
        @Override
        public void serialize(
            QueryHeatmapResponse response, JsonGenerator g, SerializerProvider provider
        ) throws IOException {
            final List<ShardedResultGroup> result = response.getResult();
            final Map<String, SortedSet<String>> common = calculateCommon(g, result);

            //g.writeStartObject();

            //g.writeObjectField("range", response.getRange());
            //g.writeObjectField("trace", response.getTrace());
            //g.writeObjectField("limits", response.getLimits());

            //g.writeFieldName("commonTags");
            //serializeCommonTags(g, common);

            //g.writeFieldName("result");
            serializeResult(g, common, result);

            //g.writeFieldName("errors");
            //serializeErrors(g, response.getErrors());

            //g.writeEndObject();
        }

        private void serializeCommonTags(
            final JsonGenerator g, final Map<String, SortedSet<String>> common
        ) throws IOException {
            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> e : common.entrySet()) {
                g.writeFieldName(e.getKey());

                g.writeStartArray();

                for (final String value : e.getValue()) {
                    g.writeString(value);
                }

                g.writeEndArray();
            }

            g.writeEndObject();
        }

        private void serializeErrors(final JsonGenerator g, final List<RequestError> errors)
            throws IOException {
            g.writeStartArray();

            for (final RequestError error : errors) {
                g.writeObject(error);
            }

            g.writeEndArray();
        }

        private Map<String, SortedSet<String>> calculateCommon(
            final JsonGenerator g, final List<ShardedResultGroup> result
        ) {
            final Map<String, SortedSet<String>> common = new HashMap<>();
            final Set<String> blacklist = new HashSet<>();

            for (final ShardedResultGroup r : result) {
                final Set<Map.Entry<String, SortedSet<String>>> entries =
                    SeriesValues.fromSeries(r.getSeries().iterator()).getTags().entrySet();

                for (final Map.Entry<String, SortedSet<String>> e : entries) {
                    if (blacklist.contains(e.getKey())) {
                        continue;
                    }

                    final SortedSet<String> previous = common.put(e.getKey(), e.getValue());

                    if (previous == null) {
                        continue;
                    }

                    if (previous.equals(e.getValue())) {
                        continue;
                    }

                    blacklist.add(e.getKey());
                    common.remove(e.getKey());
                }
            }

            return common;
        }

        private void serializeResult(
            final JsonGenerator g, final Map<String, SortedSet<String>> common,
            final List<ShardedResultGroup> result
        ) throws IOException {

            //g.writeStartArray();
            g.writeStartObject();
            for (final ShardedResultGroup group : result) {


                final MetricCollection collection = group.getMetrics();
                final SeriesValues series = SeriesValues.fromSeries(group.getSeries().iterator());

                //g.writeStringField("type", collection.getType().identifier());
                //g.writeStringField("hash", Integer.toHexString(group.hashCode()));
                //g.writeObjectField("shard", group.getShard());
                //g.writeNumberField("cadence", group.getCadence());
                //g.writeObjectField("values", collection.getData());
                //g.writeStartObject();

                String f="";
                //g.writeObjectFieldStart(String fieldName)
                String k="";
                if (series.getKeys().size() == 1) {
                    k = series.getKeys().iterator().next();
                }
                for (final Map.Entry<String, SortedSet<String>> pair : series.getTags().entrySet()) {

                    System.out.println(pair);
                    final SortedSet<String> values = pair.getValue();

                    if (values.size() != 1) {

                        continue;
                    }

                    //g.writeStringField(pair.getKey(), values.iterator().next());
                    String K = pair.getKey();
                    if (k.equals("orfees") && K.equals("f")){
                        //System.out.println("condition ok ");
                        //g.writeString( values.iterator().next());
                        f = values.iterator().next();
                    }


                    if (k.equals("nrh") && K.equals("coor")){
                        //System.out.println("condition ok ");
                        //g.writeString( values.iterator().next());
                        f = values.iterator().next();
                    }

                }
                g.writeFieldName(f);
                g.writeStartObject();
                if (collection.getType() == MetricType.POINT) {
                    final List<Point> points= collection.getDataAs(Point.class);
                        //g.writeStartArray();
                        for (final Point p :points){

                            String strLong = Long.toString(p.getTimestamp());
                            String strDouble = Double.toString(p.getValue());

                            //g.writeStartObject();
                            g.writeNumberField(strLong,p.getValue());
                            //g.writeEndObject();


                            //writeString(value);
                            //g.writeStartArray();
                            //String strLong = Long.toString(p.getTimestamp());
                            //String strDouble = Double.toString(p.getValue());
                            //g.writeStringField("timestamp", strLong);

                            //g.writeStringField(f,g.writeStringField(strLong,strDouble));

                            //g.writeString(strLong);

                            //g.writeStartArray();
                            //g.writeStringField("value",strDouble );
                            //g.writeString(strDouble);
                            //writeKey(g, series.getKeys());
                            //writeTags(g, common, series.getTags());
                            //g.writeEndArray();


                            //g.writeEndArray();
                        }
                        //g.writeEndArray();
                }
                //writeKey(g, series.getKeys());
                //writeTags(g, common, series.getTags());
                //writeTagCounts(g, series.getTags());
                //g.writeEndObject();
                g.writeEndObject();
                //g.writeStringField("key", series.getKeys().iterator().next());

            }
            g.writeEndObject();
            //g.writeEndArray();
        }

        void writeKey(JsonGenerator g, final SortedSet<String> keys) throws IOException {
            g.writeFieldName("key");

            if (keys.size() == 1) {
                g.writeString(keys.iterator().next());
            } else {
                g.writeNull();
            }
        }

        void writeTags(
            JsonGenerator g, final Map<String, SortedSet<String>> common,
            final Map<String, SortedSet<String>> tags
        ) throws IOException {
            //g.writeFieldName("tags");

            //g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                // TODO: enable this when commonTags is used.
                /*if (common.containsKey(pair.getKey())) {
                    continue;
                }*/

                final SortedSet<String> values = pair.getValue();

                if (values.size() != 1) {

                    continue;
                }

                //g.writeStringField(pair.getKey(), values.iterator().next());
                if (pair.getKey()=="f"){
                    g.writeString( values.iterator().next());
                }
            }

            //g.writeEndObject();
        }

        void writeTagCounts(JsonGenerator g, final Map<String, SortedSet<String>> tags)
            throws IOException {
            g.writeFieldName("tagCounts");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() <= 1) {
                    continue;
                }

                g.writeNumberField(pair.getKey(), values.size());
            }

            g.writeEndObject();
        }
    }
}
