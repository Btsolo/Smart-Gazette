package com.smartgazette.smartgazette.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Canonical Kenyan geography, loaded once at startup from
 * reference/geography.json (extracted from the IEBC schedules published in
 * Gazette Notice No. 13502).
 *
 * WHY THIS EXISTS
 * The county field is how a land or county notice gets tied to a place, so it
 * has to be one value per county rather than whatever the notice happened to
 * print. The source text is inconsistent in three ways: spelling variants
 * ("TransNzoia", "Trans Nzoia", "Trans-Nzoia"), a sub-county or town given
 * where the county belongs ("Thika", "Naivasha"), and fragments that are not
 * places at all ("the", "sub -") left behind by text extraction.
 */
@Service
public class GeographyService {

    private static final Logger log = LoggerFactory.getLogger(GeographyService.class);

    /** squashed lowercase name -> canonical name, e.g. "transnzoia" -> "Trans Nzoia" */
    private final Map<String, String> canonicalByKey = new HashMap<>();
    private final Set<String> constituencies = new HashSet<>();
    private final Set<String> wards = new HashSet<>();

    // Sub-counties and towns that appear in a notice where the county is
    // expected. Extend as new ones show up in the unresolved-county log line.
    private static final Map<String, String> SUB_COUNTY = Map.ofEntries(
            Map.entry("thika", "Kiambu"),       Map.entry("ruiru", "Kiambu"),
            Map.entry("gatundu", "Kiambu"),     Map.entry("limuru", "Kiambu"),
            Map.entry("juja", "Kiambu"),        Map.entry("githunguri", "Kiambu"),
            Map.entry("kikuyu", "Kiambu"),      Map.entry("naivasha", "Nakuru"),
            Map.entry("molo", "Nakuru"),        Map.entry("gilgil", "Nakuru"),
            Map.entry("njoro", "Nakuru"),       Map.entry("nyando", "Kisumu"),
            Map.entry("muhoroni", "Kisumu"),    Map.entry("rachuonyo", "Homa Bay"),
            Map.entry("ndhiwa", "Homa Bay"),    Map.entry("koibatek", "Baringo"),
            Map.entry("sabatia", "Vihiga"),     Map.entry("matungu", "Kakamega"),
            Map.entry("mumias", "Kakamega"),    Map.entry("butere", "Kakamega"));

    private static final Set<String> NOT_A_PLACE =
            Set.of("the", "sub", "district", "area", "county", "republic", "kenya", "said");

    private static String squash(String s) {
        return s.toLowerCase().replaceAll("[^a-z]", "");
    }

    @PostConstruct
    void load() {
        try (InputStream in = new ClassPathResource("reference/geography.json").getInputStream()) {
            JsonNode root = new ObjectMapper().readTree(in);
            root.path("counties").forEach(n -> {
                String name = n.path("name").asText();
                if (!name.isBlank()) {
                    canonicalByKey.put(squash(name), name);
                }
            });
            root.path("constituencies").forEach(n -> constituencies.add(n.path("name").asText()));
            root.path("wards").forEach(n -> wards.add(n.path("name").asText()));
            log.info("Geography loaded: {} counties, {} constituencies, {} wards.",
                    canonicalByKey.size(), constituencies.size(), wards.size());
        } catch (IOException e) {
            // Not fatal: normaliseCounty then falls back to returning the
            // cleaned string, which is what it did before this service existed.
            log.error("Could not load reference/geography.json. County normalisation "
                    + "will pass values through uncorrected.", e);
        }
    }

    /**
     * Maps a messy location string to a canonical county name.
     *
     * @return the canonical county, the cleaned original when it cannot be
     *         resolved, or null when the string is not a place at all.
     *         Null is deliberate — an empty field beats stored noise.
     */
    public String normaliseCounty(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String cleaned = raw.replaceAll("\\s+", " ").replaceAll("\\s*-\\s*$", "").trim();
        String key = cleaned.toLowerCase().replaceAll("[^a-z ]", "").trim();
        if (key.length() < 3 || NOT_A_PLACE.contains(key)) {
            return null;
        }

        String flat = key.replace(" ", "");
        String exact = canonicalByKey.get(flat);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, String> e : SUB_COUNTY.entrySet()) {
            if (key.matches(".*\\b" + Pattern.quote(e.getKey()) + "\\b.*")) {
                return e.getValue();
            }
        }
        // "Nakuru Municipality" — a county name carrying a qualifier.
        for (Map.Entry<String, String> e : canonicalByKey.entrySet()) {
            if (e.getKey().length() >= 5 && flat.contains(e.getKey())) {
                return e.getValue();
            }
        }
        log.debug("Unresolved county '{}' — add it to SUB_COUNTY if it recurs.", cleaned);
        return cleaned;
    }

    public boolean isKnownConstituency(String name) {
        return name != null && constituencies.contains(name.trim());
    }

    public boolean isKnownWard(String name) {
        return name != null && wards.contains(name.trim());
    }
}