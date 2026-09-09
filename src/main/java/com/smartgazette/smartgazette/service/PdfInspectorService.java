package com.smartgazette.smartgazette.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Extracts text from a gazette PDF by shelling out to pdf-inspector
 * (Firecrawl, MIT) through a small Node script.
 *
 * WHY THIS EXISTS
 * ---------------
 * The Kenya Gazette is a two-column newspaper layout. PDFBox's
 * PDFTextStripper reads straight across the page, so it interleaves the left
 * and right columns line by line and produces text like:
 *
 *     GAZETTE NOTICE No. 6558  4. Papers  THE CONSTITUTION OF KENYA  (a) Report...
 *
 * An LLM can untangle that; a regex cannot. Since the whole template-extraction
 * plan depends on clean input, fixing the reading order is a prerequisite for
 * everything downstream.
 *
 * pdf-inspector's extractText() produces correct multi-column reading order.
 * It has no Java bindings, but it ships an npm package, so we call it through
 * a Node script and read the result from standard output.
 *
 * SAFETY
 * ------
 * This class never throws on failure. It returns null, and the caller keeps
 * its existing PDFBox path. That is deliberate: a new extraction engine should
 * be able to fail without taking the pipeline down with it.
 */
@Service
public class PdfInspectorService {

    private static final Logger log = LoggerFactory.getLogger(PdfInspectorService.class);

    /**
     * Which engine processAndSavePdf should use. Defaults to "legacy" so that
     * merely deploying this class changes nothing; the new path is opt-in via
     * application.properties.
     */
    @Value("${extraction.engine:legacy}")
    private String extractionEngine;

    /** Absolute path to inspect.js. */
    @Value("${extraction.inspector.script:tools/inspect.js}")
    private String inspectorScript;

    /** The node executable. Overridable when node is not on the PATH. */
    @Value("${extraction.inspector.node:node}")
    private String nodeCommand;

    /** A 100-page gazette extracts in well under a second; 120s is a generous
     *  ceiling that still guarantees we never hang the async worker forever. */
    @Value("${extraction.inspector.timeout-seconds:120}")
    private long timeoutSeconds;

    public boolean isInspectorEnabled() {
        return "inspector".equalsIgnoreCase(extractionEngine);
    }

    /**
     * Runs pdf-inspector over the given PDF and returns its raw text.
     *
     * @return the extracted text, or null if anything at all went wrong.
     *         Never throws — the caller falls back to PDFBox on null.
     */
    public String extractText(File pdf) {
        if (pdf == null || !pdf.isFile()) {
            log.warn("pdf-inspector: file is missing or not a regular file: {}", pdf);
            return null;
        }

        File script = new File(inspectorScript);
        if (!script.isFile()) {
            log.error("pdf-inspector: script not found at '{}'. Falling back to PDFBox.",
                    script.getAbsolutePath());
            return null;
        }

        long start = System.currentTimeMillis();
        Process process = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    nodeCommand,
                    script.getAbsolutePath(),
                    pdf.getAbsolutePath());

            // Run with the script's own folder as the working directory, so
            // Node resolves node_modules relative to the script rather than
            // relative to wherever the JVM happens to have been started.
            if (script.getParentFile() != null) {
                pb.directory(script.getParentFile());
            }

            // Keep stdout and stderr separate. If we merged them (via
            // redirectErrorStream(true)) a stray Node warning would end up
            // inside the extracted gazette text.
            pb.redirectErrorStream(false);

            // Force UTF-8 in the child process. Without this, Node on Windows
            // inherits the console code page and mangles em-dashes and
            // apostrophes ("deceased's" -> "deceasedÔÇÖs").
            Map<String, String> env = pb.environment();
            env.put("LANG", "en_US.UTF-8");
            env.put("LC_ALL", "en_US.UTF-8");

            process = pb.start();

            // Read stdout and stderr on separate threads.
            //
            // This matters more than it looks. The pipe between the two
            // processes has a small OS-level buffer. If Node fills stderr
            // while we are only reading stdout, Node blocks on the write, we
            // block waiting for output that will never come, and the whole
            // thing deadlocks. Draining both concurrently avoids that.
            StreamCollector out = new StreamCollector(process.getInputStream());
            StreamCollector err = new StreamCollector(process.getErrorStream());
            Thread outThread = new Thread(out, "pdf-inspector-stdout");
            Thread errThread = new Thread(err, "pdf-inspector-stderr");
            outThread.start();
            errThread.start();

            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                log.error("pdf-inspector: timed out after {}s on {}. Falling back to PDFBox.",
                        timeoutSeconds, pdf.getName());
                process.destroyForcibly();
                return null;
            }

            outThread.join(5000);
            errThread.join(5000);

            int exit = process.exitValue();
            if (exit != 0) {
                log.error("pdf-inspector: exited with code {} on {}. stderr: {}",
                        exit, pdf.getName(), abbreviate(err.text(), 400));
                return null;
            }

            String text = out.text();
            if (text == null || text.isBlank()) {
                log.error("pdf-inspector: produced no output for {}. Falling back to PDFBox.",
                        pdf.getName());
                return null;
            }
            if (isDegenerate(text)) {
                log.error("pdf-inspector: output looks degenerate for {} (mostly one repeated "
                        + "character). Falling back to PDFBox.", pdf.getName());
                return null;
            }

            log.info("pdf-inspector: extracted {} chars from {} in {}ms.",
                    text.length(), pdf.getName(), System.currentTimeMillis() - start);
            return text;

        } catch (IOException e) {
            log.error("pdf-inspector: could not start '{}'. Is Node installed and on the PATH? "
                    + "Falling back to PDFBox.", nodeCommand, e);
            return null;
        } catch (InterruptedException e) {
            // Restore the flag. Swallowing an interrupt hides a shutdown
            // request from every layer above this one.
            Thread.currentThread().interrupt();
            log.warn("pdf-inspector: interrupted while extracting {}.", pdf.getName());
            return null;
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    /**
     * A drawn horizontal rule is sometimes read as a long run of dash
     * characters, producing output that looks like text but carries no
     * content. If one character dominates, treat the extraction as failed
     * rather than feeding rubbish downstream.
     */
    private boolean isDegenerate(String text) {
        String stripped = text.replaceAll("\\s", "");
        if (stripped.length() < 200) {
            return false;                 // too short to judge
        }
        Map<Character, Integer> freq = new HashMap<>();
        for (char c : stripped.toCharArray()) {
            freq.merge(c, 1, Integer::sum);
        }
        int max = freq.values().stream().max(Integer::compareTo).orElse(0);
        return (double) max / stripped.length() > 0.60;
    }

    private static String abbreviate(String s, int max) {
        if (s == null) return "";
        s = s.trim();
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    /**
     * Drains one stream to a string on its own thread, decoding as UTF-8
     * explicitly rather than relying on the platform default charset (which on
     * Windows is usually windows-1252 and is what produces mojibake).
     */
    private static final class StreamCollector implements Runnable {
        private final java.io.InputStream in;
        private final StringBuilder sb = new StringBuilder();

        StreamCollector(java.io.InputStream in) {
            this.in = in;
        }

        @Override
        public void run() {
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            } catch (IOException e) {
                log.debug("pdf-inspector: stream reader closed early: {}", e.getMessage());
            }
        }

        String text() {
            return sb.toString();
        }
    }
}
