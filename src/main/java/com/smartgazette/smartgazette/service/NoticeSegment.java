package com.smartgazette.smartgazette.service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record NoticeSegment(
        int sourceOrder,          // position in gazette (1, 2, 3...)
        String rawText,           // original text from PDF
        int charCount,            // rawText.length()
        int estimatedTokens,      // charCount / 4
        boolean isOversized,      // estimatedTokens > 3500
        String preFilterCategory  // from keywordPreFilter — null if AI needed
) {
    // Matches "CAUSE NO. E153 OF 2024", "CAUSE NO 766 OF 2014", etc.
    // Case-insensitive, optional period after NO, optional leading
    // letter on the case number (E153 vs 153).
    private static final Pattern CAUSE_NO_PATTERN =
            Pattern.compile("CAUSE NO\\.?\\s+[A-Z]?\\d+\\s+OF\\s+\\d{4}", Pattern.CASE_INSENSITIVE);

    // Static factory method — builds a NoticeSegment from raw text
    public static NoticeSegment of(int sourceOrder, String rawText) {
        int charCount = rawText.length();
        int estimatedTokens = charCount / 4;
        return new NoticeSegment(
                sourceOrder,
                rawText,
                charCount,
                estimatedTokens,
                estimatedTokens > 3500,
                null  // pre-filter runs separately after construction
        );
    }

    // Returns a copy with the pre-filter category attached
    public NoticeSegment withCategory(String category) {
        return new NoticeSegment(
                sourceOrder, rawText, charCount,
                estimatedTokens, isOversized, category
        );
    }

    // Text to actually send to AI — respects token budget
    public String textForProcessing() {
        if (!isOversized) return rawText;

        // Find clean sentence boundary near 14000 chars (~3500 tokens)
        int targetChar = 14000;
        if (rawText.length() <= targetChar) return rawText;

        String candidate = rawText.substring(0, targetChar);
        int lastPeriod = Math.max(
                candidate.lastIndexOf(".\n"),
                candidate.lastIndexOf(". ")
        );

        if (lastPeriod > targetChar * 0.7) {
            return rawText.substring(0, lastPeriod + 1);
        }
        return rawText.substring(0, targetChar);
    }

    /**
     * Counts how many distinct "CAUSE NO. ... OF ...." entries appear
     * inside this segment.
     *
     * WHY THIS MATTERS: a single "GAZETTE NOTICE NO." block (e.g. probate
     * notices under "PROBATE AND ADMINISTRATION") routinely bundles
     * several unrelated court cases under one shared header. The pipeline
     * currently has no way to know that — it treats the whole block as
     * one notice, which either produces one muddled article covering
     * several cases, or silently drops everything past the first case,
     * or just blows the token budget if the block is large enough.
     *
     * Counting cause numbers is the cheap, deterministic signal that
     * tells us "this segment isn't one case, it's N cases stapled
     * together" — before we spend any AI call on it.
     */
    public int caseCount() {
        Matcher matcher = CAUSE_NO_PATTERN.matcher(rawText);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    // True when this segment bundles more than one court case under
    // a single GAZETTE NOTICE NO. — the signal that triggers splitting
    // instead of normal single-case extraction.
    public boolean isMultiCase() {
        return caseCount() > 1;
    }

    /**
     * Splits a multi-case segment into one chunk per cause number,
     * with the shared header (court name, act citation, etc. — the
     * text that appears before the FIRST "CAUSE NO.") prepended to
     * every chunk so each sub-case keeps its court context when sent
     * to extraction independently.
     *
     * Returns an empty list if this segment is not actually multi-case
     * (callers should check isMultiCase() first; this is a defensive
     * fallback, not the primary guard).
     */
    public List<String> splitByCauseNumber() {
        List<String> chunks = new ArrayList<>();

        Matcher matcher = CAUSE_NO_PATTERN.matcher(rawText);
        List<Integer> startPositions = new ArrayList<>();
        while (matcher.find()) {
            startPositions.add(matcher.start());
        }

        if (startPositions.size() < 2) {
            // Not actually multi-case — nothing to split
            return chunks;
        }

        // Everything before the first "CAUSE NO." is the shared header:
        // court name, act citation, "PROBATE AND ADMINISTRATION" etc.
        String sharedHeader = rawText.substring(0, startPositions.get(0)).trim();

        for (int i = 0; i < startPositions.size(); i++) {
            int chunkStart = startPositions.get(i);
            int chunkEnd = (i + 1 < startPositions.size())
                    ? startPositions.get(i + 1)
                    : rawText.length();

            String caseBody = rawText.substring(chunkStart, chunkEnd).trim();

            String fullChunk = sharedHeader.isEmpty()
                    ? caseBody
                    : sharedHeader + "\n\n" + caseBody;

            chunks.add(fullChunk);
        }

        return chunks;
    }
}