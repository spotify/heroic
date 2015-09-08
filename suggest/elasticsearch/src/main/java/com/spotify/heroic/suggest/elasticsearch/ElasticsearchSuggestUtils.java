package com.spotify.heroic.suggest.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.google.common.collect.ImmutableMap;

public class ElasticsearchSuggestUtils {
    public static Map<String, Object> loadJsonResource(String path) throws IOException {
        final String fullPath = ElasticsearchSuggestModule.class.getPackage().getName() + "/" + path;

        try (final InputStream input = ElasticsearchSuggestModule.class.getClassLoader().getResourceAsStream(fullPath)) {
            if (input == null)
                return ImmutableMap.of();

            return JsonXContent.jsonXContent.createParser(input).map();
        }
    }
}