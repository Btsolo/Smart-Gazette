package com.smartgazette.smartgazette.service.templates;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The few Python string behaviours the templates rely on, reproduced exactly
 * so the Java templates give the same records as tools/*_template.py.
 *
 * Regex flags: Python's str patterns are Unicode-aware (\s, \w, \b, \d) and
 * treat only "\n" as a line end for "." and "$"; Java needs
 * UNICODE_CHARACTER_CLASS and UNIX_LINES for the same behaviour.
 */
final class Py {

    static final int U = Pattern.UNICODE_CHARACTER_CLASS | Pattern.UNIX_LINES;
    static final int I = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    static final int S = Pattern.DOTALL;
    static final int M = Pattern.MULTILINE;
    static final Pattern WS = Pattern.compile("\\s+", U);

    private Py() {
    }

    static Pattern re(String regex, int flags) {
        return Pattern.compile(regex, U | flags);
    }

    /** str.strip(chars) */
    static String strip(String s, String chars) {
        int a = 0, b = s.length();
        while (a < b && chars.indexOf(s.charAt(a)) >= 0) a++;
        while (b > a && chars.indexOf(s.charAt(b - 1)) >= 0) b--;
        return s.substring(a, b);
    }

    /** str.strip() - Unicode whitespace, as Python. */
    static String strip(String s) {
        int a = 0, b = s.length();
        while (a < b && isPySpace(s.charAt(a))) a++;
        while (b > a && isPySpace(s.charAt(b - 1))) b--;
        return s.substring(a, b);
    }

    static boolean isPySpace(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u001c' || c == '\u001d'
                || c == '\u001e' || c == '\u001f' || c == '\u0085';
    }

    /** re.sub(r'\s+', ' ', s) */
    static String squash(String s) {
        return WS.matcher(s).replaceAll(" ");
    }

    static String sub(Pattern p, String repl, String s) {
        return p.matcher(s).replaceAll(repl);
    }

    static boolean search(Pattern p, String s) {
        return p.matcher(s).find();
    }

    static Matcher find(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m : null;
    }

    static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /** str.title(): upper-case a cased character that follows an uncased one,
     *  lower-case the rest ("letters of administration" -> "Letters Of Administration"). */
    static String title(String s) {
        StringBuilder b = new StringBuilder(s.length());
        boolean prevCased = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean cased = Character.isUpperCase(c) || Character.isLowerCase(c) || Character.isTitleCase(c);
            if (cased) {
                b.append(prevCased ? Character.toLowerCase(c) : Character.toTitleCase(c));
            } else {
                b.append(c);
            }
            prevCased = cased;
        }
        return b.toString();
    }

    /** str.split() on whitespace: number of words. */
    static int words(String s) {
        String t = strip(s);
        return t.isEmpty() ? 0 : t.split("\\s+").length;
    }
}
