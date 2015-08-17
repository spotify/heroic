package com.spotify.heroic.shell.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskElasticsearchParameters;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagSuggest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Slf4j
@TaskUsage("Execute a set of suggest performance tests")
@TaskName("suggest-performance")
public class SuggestPerformance implements ShellTask {
    @Inject
    private SuggestManager suggest;

    @Inject
    private QueryParser parser;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final SuggestBackend s = suggest.useGroup(params.group);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final List<Callable<TestResult>> tests = new ArrayList<>();

        final DateRange range = DateRange.now();

        try (final InputStream input = open(params.file)) {
            final TestSuite suite = mapper.readValue(input, TestSuite.class);

            for (final TestCase c : suite.getTests()) {
                final Filter context = parser.parseFilter(c.getContext());
                final RangeFilter filter = new RangeFilter(context, range, params.limit);

                for (final int concurrency : suite.getConcurrency()) {
                    tests.add(setupTest(out, c.getContext(), concurrency, filter, c, s));
                }
            }
        }

        final ObjectMapper m = new ObjectMapper();

        for (final Callable<TestResult> test : tests) {
            final TestResult result = test.call();

            final TestOutput output = new TestOutput(result.getContext(), result.getConcurrency(), result.getErrors(),
                    result.getMismatches(), result.getMatches(), result.getCount(),
                    result.getTimes());

            out.println(m.writeValueAsString(output));
            out.flush();
        }

        return async.resolved();
    }

    private Callable<TestResult> setupTest(final PrintWriter out, final String context, final int concurrency,
            final RangeFilter filter, final TestCase c, final SuggestBackend s) {
        return new Callable<TestResult>() {
            @Override
            public TestResult call() throws Exception {
                log.info("Running context {} with concurrency {}", context, concurrency);
                final ExecutorService service = Executors.newFixedThreadPool(concurrency);
                final List<Future<TestPartialResult>> futures = new ArrayList<>();

                final AtomicInteger index = new AtomicInteger();

                final int count = c.getCount();
                final List<TestSuggestion> suggestions = c.getSuggestions();

                for (int i = 0; i < concurrency; i++) {
                    futures.add(service.submit(setupTestThread(out, index, count, suggestions, filter, s)));
                }

                final List<Long> times = new ArrayList<>();
                int errors = 0;
                int mismatches = 0;
                int matches = 0;

                for (final Future<TestPartialResult> future : futures) {
                    final TestPartialResult partial = future.get();
                    times.addAll(partial.getTimes());
                    errors += partial.getErrors();
                    mismatches += partial.getMismatches();
                    matches += partial.getMatches();
                }

                Collections.sort(times);

                service.shutdown();
                service.awaitTermination(10, TimeUnit.SECONDS);
                return new TestResult(context, concurrency, times, errors, mismatches, matches, count);
            }
        };
    }

    private Callable<TestPartialResult> setupTestThread(final PrintWriter out, final AtomicInteger index,
            final int count, final List<TestSuggestion> suggestions, final RangeFilter filter, final SuggestBackend s) {
        return new Callable<TestPartialResult>() {
            @Override
            public TestPartialResult call() throws Exception {
                int i = 0;

                final List<Long> times = new ArrayList<>();
                int errors = 0;
                int mismatches = 0;
                int matches = 0;

                while ((i = index.getAndIncrement()) < count) {
                    final TestSuggestion test = suggestions.get(i % suggestions.size());
                    final long start = System.nanoTime();

                    final Suggestion input = test.getInput();

                    final AsyncFuture<TagSuggest> future = s.tagSuggest(filter, MatchOptions.builder().build(),
                            input.getKey(), input.getValue());

                    final TagSuggest result;

                    try {
                        result = future.get();
                    } catch (ExecutionException e) {
                        errors++;
                        continue;
                    }

                    final Set<Suggestion> expect = new HashSet<>(test.getExpect());

                    if (result.getSuggestions().isEmpty()) {
                        log.error("no matches");
                        mismatches++;
                        continue;
                    }

                    for (TagSuggest.Suggestion s : result.getSuggestions()) {
                        expect.remove(new Suggestion(s.getKey(), s.getValue()));

                        if (expect.isEmpty())
                            break;
                    }

                    if (!expect.isEmpty()) {
                        log.error("{} <> {}", expect, result.getSuggestions());
                    }

                    matches++;
                    final long diff = System.nanoTime() - start;
                    times.add(diff);
                }

                // put the service under load.
                return new TestPartialResult(times, errors, mismatches, matches);
            }
        };
    }

    private InputStream open(Path file) throws IOException {
        final InputStream input = Files.newInputStream(file);

        // unpack gzip.
        if (!file.getFileName().toString().endsWith(".gz"))
            return input;

        return new GZIPInputStream(input);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams implements TaskElasticsearchParameters {
        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group;

        @Option(name = "-f", usage = "File to load tests from", metaVar = "<yaml>")
        @Getter
        private Path file = Paths.get("tests.yaml");

        @Option(name = "-l", usage = "Limit the number of results", metaVar = "<int>")
        @Getter
        private int limit = 10;

        @Option(name = "--seeds", usage = "Elasticsearch Seeds (standalone only)")
        @Getter
        private String seeds = "localhost";

        @Option(name = "--cluster-name", usage = "Elasticsearch ClusterName (standalone only)")
        @Getter
        private String clusterName = "elasticsearch";

        @Option(name = "--backend-type", usage = "Elasticsearch Backend Type (standalone only)")
        @Getter
        private String backendType = "default";
    }

    @Data
    public static class TestOutput {
        private final String context;
        private final int concurrency;
        private final int errors;
        private final int mismatches;
        private final int matches;
        private final int count;
        private final List<Long> times;
    }

    @Data
    public static class TestPartialResult {
        final List<Long> times;
        final int errors;
        final int mismatches;
        final int matches;
    }

    @Data
    public static class TestResult {
        final String context;
        final int concurrency;
        final List<Long> times;
        final int errors;
        final int mismatches;
        final int matches;
        final int count;
    }

    @Data
    public static class TestSuite {
        private final List<Integer> concurrency;
        private final List<TestCase> tests;

        @JsonCreator
        public TestSuite(@JsonProperty("concurrenty") List<Integer> concurrency,
                @JsonProperty("tests") List<TestCase> tests) {
            this.concurrency = concurrency;
            this.tests = tests;
        }
    }

    @Data
    public static class TestCase {
        private String context;
        private final int count;
        private List<TestSuggestion> suggestions;

        @JsonCreator
        public TestCase(@JsonProperty("context") String context, @JsonProperty("count") int count,
                @JsonProperty("suggestions") List<TestSuggestion> suggestions) {
            this.context = context;
            this.count = count;
            this.suggestions = suggestions;
        }
    }

    @Data
    public static class TestSuggestion {
        private final Suggestion input;
        private final Set<Suggestion> expect;

        public TestSuggestion(@JsonProperty("input") Suggestion input, @JsonProperty("expect") Set<Suggestion> expect) {
            this.input = input;
            this.expect = expect;
        }
    }

    @Data
    @RequiredArgsConstructor
    public static class Suggestion {
        private final String key;
        private final String value;

        @JsonCreator
        public Suggestion(JsonNode node) {
            final String text = node.asText();
            final String[] split = text.split(":", 2);
            this.key = split[0];
            this.value = split.length > 1 ? split[1] : "";
        }
    }
}