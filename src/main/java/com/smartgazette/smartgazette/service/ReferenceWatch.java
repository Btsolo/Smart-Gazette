package com.smartgazette.smartgazette.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.smartgazette.smartgazette.model.ReferenceFlag;
import com.smartgazette.smartgazette.repository.ReferenceFlagRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reference watch (docs/specs/reference-watch.md) - the Java side of
 * tools/reference_watch.py. From what the system reads, it notices when the
 * reference data may be out of date and says exactly what to do; it never
 * edits reference data. Flags go to the log ("REFERENCE-WATCH", one line per
 * new flag, a daily summary) and the reference_flag table; a flag the library
 * itself now answers (after a rebuild) closes by itself.
 */
@Service
public class ReferenceWatch {

    private static final Logger log = LoggerFactory.getLogger(ReferenceWatch.class);

    /** A sign that reference data may be out of date. */
    public record Flag(String set, String key, String watcher, String detail, String message, String action, String evidence) {}

    private static final int U = Pattern.UNICODE_CHARACTER_CLASS;
    private static final Pattern AMENDMENT = Pattern.compile(
            "((?:[A-Z][A-Za-z'-]*\\s+(?:and\\s+|of\\s+|on\\s+)?){1,8})\\(\\s*Amendment\\s*\\)\\s*Act,?\\s*(\\d{4})", U);
    private static final Pattern MISC = Pattern.compile(
            "Statute\\s+Law\\s*\\(\\s*Miscellaneous\\s+Amendments?\\s*\\)\\s*Act,?\\s*(\\d{4})", Pattern.CASE_INSENSITIVE | U);
    private static final Pattern SUPPLEMENT_AMEND = Pattern.compile(
            "AN\\s+ACT\\s+of\\s+Parliament\\s+to\\s+amend\\s+the\\s+((?:[A-Z][A-Za-z'-]*\\s+(?:and\\s+|of\\s+|on\\s+)?){1,9}?Act)",
            Pattern.CASE_INSENSITIVE | U);

    private final LawReferenceService laws;
    private final ReferenceFlagRepository repo;

    @Value("${reference-watch.old-copy-years:5}")
    private int oldCopyYears = 5;

    public ReferenceWatch(LawReferenceService laws, ReferenceFlagRepository repo) {
        this.laws = laws;
        this.repo = repo;
    }

    // ------------------------------------------------------------- watchers (= reference_watch.flags)

    private Integer yearOf(String key) {
        JsonNode d = laws.law(key);
        String v = d != null && d.hasNonNull("version_date") ? d.path("version_date").asText() : null;
        return v != null && v.length() >= 4 ? Integer.valueOf(v.substring(0, 4)) : null;
    }

    private String field(String key, String name) {
        JsonNode d = laws.law(key);
        return d != null && d.hasNonNull(name) ? d.path(name).asText() : null;
    }

    static String sentenceAt(String text, int start, int end) {
        int a = Math.max(text.lastIndexOf('.', start - 1), text.lastIndexOf('\n', start - 1)) + 1;
        int b1 = text.indexOf('.', end), b2 = text.indexOf('\n', end);
        int b = b1 < 0 ? (b2 < 0 ? text.length() : b2) : (b2 < 0 ? b1 : Math.min(b1, b2));
        String s = text.substring(a, Math.min(text.length(), b + 1)).replaceAll("\\s+", " ").strip();
        return s.length() > 300 ? s.substring(0, 300) : s;
    }

    /** The flags one notice (or one Gazette Supplement) raises. */
    public List<Flag> flags(String text) {
        List<Flag> out = new ArrayList<>();
        if (text == null || text.isBlank()) return out;
        // 1. cited sections our copy does not have
        for (LawReferenceService.LawRef r : laws.refs(text)) {
            if (r.law().startsWith("?") || r.found() || r.provision().endsWith("SCHEDULE")) continue;
            Matcher m = Pattern.compile("\\b" + Pattern.quote(r.provision()) + "\\b", U).matcher(text);
            String url = field(r.law(), "source_url");
            out.add(new Flag("law", r.law(), "law.missing_section", "s." + r.provision(),
                    "A notice cites section " + r.provision() + " of the " + nameOf(r.law()) + "; our copy (text as at "
                            + field(r.law(), "version_date") + ") has no section " + r.provision() + ".",
                    "Check the current version on Kenya Law (" + (url != null ? url : "kenyalaw.org") + "); if it has section "
                            + r.provision() + ", save its PDF to raw/law/ and run: python tools/build_law_reference.py pdfs raw/law",
                    m.find() ? sentenceAt(text, m.start(), m.end()) : ""));
        }
        // 2. amendment Acts newer than our copy
        Matcher a = AMENDMENT.matcher(text);
        while (a.find()) {
            String key = LawReferenceService.whichLaw(a.group(1).strip() + " Act");
            int year = Integer.parseInt(a.group(2));
            if (key != null && !key.startsWith("?") && yearOf(key) != null && year > yearOf(key)) {
                String title = nameOf(key);
                out.add(amended(key, title.substring(0, title.length() - 4) + " (Amendment) Act, " + year, text, a));
            }
        }
        Matcher misc = MISC.matcher(text);
        while (misc.find()) {
            int year = Integer.parseInt(misc.group(1));
            List<String> keys = new ArrayList<>(LawReferenceService.headingLaws(text));
            for (LawReferenceService.LawRef r : laws.refs(text)) keys.add(r.law());
            for (String key : keys) {
                if (!key.startsWith("?") && yearOf(key) != null && year > yearOf(key))
                    out.add(amended(key, "Statute Law (Miscellaneous Amendments) Act, " + year, text, misc));
            }
        }
        // 3. a supplement that amends a law we hold
        Matcher s = SUPPLEMENT_AMEND.matcher(text.length() > 3000 ? text.substring(0, 3000) : text);
        while (s.find()) {
            String key = LawReferenceService.whichLaw(s.group(1));
            if (key != null && !key.startsWith("?")) {
                out.add(new Flag("law", key, "law.amending_supplement", s.group(1).strip(),
                        "A Gazette Supplement amends the " + nameOf(key) + "; our copy is text as at " + field(key, "version_date") + ".",
                        "When Kenya Law publishes the amended version, save its PDF to raw/law/ and rebuild "
                                + "(python tools/build_law_reference.py pdfs raw/law).",
                        sentenceAt(text, s.start(), s.end())));
            }
        }
        Set<String> seen = new HashSet<>();
        List<Flag> unique = new ArrayList<>();
        for (Flag f : out) if (seen.add(f.key() + "|" + f.watcher() + "|" + f.detail())) unique.add(f);
        return unique;
    }

    private Flag amended(String key, String amending, String text, Matcher m) {
        return new Flag("law", key, "law.amended_after_copy", amending,
                "A notice cites the " + amending + "; our copy of the " + nameOf(key) + " is text as at "
                        + field(key, "version_date") + ", before it.",
                "Save the current version of the " + nameOf(key) + " from Kenya Law to raw/law/ and rebuild "
                        + "(python tools/build_law_reference.py pdfs raw/law).",
                sentenceAt(text, m.start(), m.end()));
    }

    private String nameOf(String key) {
        String t = field(key, "title");
        return t != null ? t : key;
    }

    /** law.old_copy: copies older than reference-watch.old-copy-years. */
    public List<Flag> oldCopies(LocalDate today) {
        List<Flag> out = new ArrayList<>();
        for (String key : laws.lawKeys()) {
            String v = field(key, "version_date");
            if (v != null && ChronoUnit.DAYS.between(LocalDate.parse(v), today) > oldCopyYears * 365L) {
                String url = field(key, "source_url");
                out.add(new Flag("law", key, "law.old_copy", v,
                        "Our copy of the " + nameOf(key) + " is text as at " + v + " (over " + oldCopyYears + " years old).",
                        "Check Kenya Law for a newer version (" + (url != null ? url : "kenyalaw.org") + ").", ""));
            }
        }
        return out;
    }

    /** A flag the library itself now answers, after a rebuild. */
    boolean isClosed(String key, String watcher, String detail) {
        JsonNode d = laws.law(key);
        if ("law.missing_section".equals(watcher)) return d != null && d.path("provisions").has(detail.substring(2));
        if ("law.amended_after_copy".equals(watcher) || "law.amending_supplement".equals(watcher)) {
            Matcher y = Pattern.compile("(\\d{4})\\s*$").matcher(detail);
            return y.find() && yearOf(key) != null && yearOf(key) >= Integer.parseInt(y.group(1));
        }
        if ("law.old_copy".equals(watcher)) return !detail.equals(field(key, "version_date"));
        return false;
    }

    // ------------------------------------------------------------- recording and telling the owner

    /** Watches every notice of a processed gazette (or a supplement). Never throws. */
    public void watch(List<String> texts, String source) {
        try {
            int added = 0;
            for (int i = 0; i < texts.size(); i++) {
                for (Flag f : flags(texts.get(i))) {
                    if (record(f, source + " #" + (i + 1))) added++;
                }
            }
            if (added > 0) log.warn("REFERENCE-WATCH: {} new flag(s) from {} - see the reference_flag table.", added, source);
        } catch (Exception e) {
            log.error("Reference watch failed for {} - processing is not affected.", source, e);
        }
    }

    /** Stores a flag; true when it is new (logged once). A dismissed flag stays dismissed. */
    boolean record(Flag f, String source) {
        Optional<ReferenceFlag> old = repo.findByDataSetAndRefKeyAndWatcherAndDetail(f.set(), f.key(), f.watcher(), f.detail());
        LocalDateTime now = LocalDateTime.now();
        if (old.isPresent()) {
            ReferenceFlag r = old.get();
            r.setSeenCount(r.getSeenCount() + 1);
            r.setLastSeen(now);
            if (r.getStatus() == ReferenceFlag.Status.DONE && !isClosed(f.key(), f.watcher(), f.detail())) {
                r.setStatus(ReferenceFlag.Status.OPEN);             // it came back
                r.setClosedAt(null);
            }
            repo.save(r);
            return false;
        }
        ReferenceFlag r = new ReferenceFlag();
        r.setDataSet(f.set());
        r.setRefKey(f.key());
        r.setWatcher(f.watcher());
        r.setDetail(f.detail());
        r.setMessage(f.message());
        r.setAction(f.action());
        r.setEvidence(f.evidence());
        r.setSource(source);
        r.setSeenCount(1);
        r.setFirstSeen(now);
        r.setLastSeen(now);
        repo.save(r);
        log.warn("REFERENCE-WATCH [{}] {} {}: {} -> {}", f.watcher(), f.key(), f.detail(), f.message(), f.action());
        return true;
    }

    /**
     * Daily: closes the flags the library now answers, raises old-copy reminders,
     * and logs one summary line of what is still open.
     */
    @Scheduled(cron = "${reference-watch.cron:0 0 7 * * *}", zone = "${reference-watch.zone:Africa/Nairobi}")
    public void review() {
        try {
            for (Flag f : oldCopies(LocalDate.now())) record(f, "daily review");
            List<ReferenceFlag> open = repo.findByStatusOrderByFirstSeenAsc(ReferenceFlag.Status.OPEN);
            int closed = 0;
            for (ReferenceFlag r : open) {
                if (isClosed(r.getRefKey(), r.getWatcher(), r.getDetail())) {
                    r.setStatus(ReferenceFlag.Status.DONE);
                    r.setClosedAt(LocalDateTime.now());
                    repo.save(r);
                    closed++;
                    log.info("REFERENCE-WATCH closed: {} {} {} (the library now answers it)", r.getWatcher(), r.getRefKey(), r.getDetail());
                }
            }
            List<ReferenceFlag> still = repo.findByStatusOrderByFirstSeenAsc(ReferenceFlag.Status.OPEN);
            if (!still.isEmpty() || closed > 0) {
                StringBuilder sb = new StringBuilder();
                for (ReferenceFlag r : still) sb.append(sb.length() > 0 ? "; " : "").append(r.getRefKey()).append(' ').append(r.getDetail());
                log.warn("REFERENCE-WATCH summary: {} open flag(s){}{}", still.size(), closed > 0 ? ", " + closed + " closed today" : "",
                        still.isEmpty() ? "" : ": " + sb);
            }
        } catch (Exception e) {
            log.error("Reference watch review failed.", e);
        }
    }
}
