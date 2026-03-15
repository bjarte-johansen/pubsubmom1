package no.hvl.dat110.common;

public class LoggerSyntaxHighlight {




    public static class IntelliJDarkAnsi {

        private IntelliJDarkAnsi() {}

        public static final String RESET = "\033[0m";

        // background
        public static final String BG = "\033[48;2;43;43;43m"; // #2B2B2B

        // foreground colors (Darcula-like)
        public final String DEFAULT             = "\033[38;2;169;183;198m";     // #A9B7C6
        public final String IDENTIFIER          = DEFAULT;
        public final String KEYWORD             = "\033[38;2;204;120;50m";      // #CC7832
        public final String STRING              = "\033[38;2;106;135;89m";      // #6A8759
        public final String NUMBER              = "\033[38;2;104;151;187m";     // #6897BB
        public final String SYMBOL              = "\033[38;2;124;151;147m";     // #6897BB
        public final String COMMENT             = "\033[38;2;128;128;128m";     // #808080
        public final String CLASS               = DEFAULT;                      // same as default in many setups
    }

    public static class ColorScheme{
        private static final String RESET   = "\033[0m";

        private static final String COLOR_GREEN         = "\033[32m";  // strings
        private static final String COLOR_YELLOW        = "\033[33m";  // symbols
        private static final String COLOR_CYAN          = "\033[36m";  // identifiers
        private static final String COLOR_MAGENTA       = "\033[35m";  // numbers

        public final String DEFAULT             = RESET;
        public final String STRING              = COLOR_GREEN;
        public final String SYMBOL              = COLOR_YELLOW;
        public final String NUMBER              = COLOR_MAGENTA;
        public final String IDENTIFIER          = COLOR_CYAN;
        public final String KEYWORD             = COLOR_CYAN;
    }

    public static class HighContrastScheme {
        private static final String RESET   = "\033[0m";

        private static final String BRIGHT_GREEN    = "\033[92m";
        private static final String BRIGHT_YELLOW   = "\033[93m";
        private static final String BRIGHT_CYAN     = "\033[96m";
        private static final String BRIGHT_MAGENTA  = "\033[95m";
        private static final String BRIGHT_WHITE    = "\033[97m";

        public final String DEFAULT             = RESET;
        public final String STRING              = BRIGHT_GREEN;     // readable, calm
        public final String SYMBOL              = BRIGHT_YELLOW;    // strong contrast
        public final String NUMBER              = BRIGHT_MAGENTA;   // pops but not harsh
        public final String IDENTIFIER          = BRIGHT_WHITE;      // crisp
        public final String KEYWORD             = BRIGHT_CYAN;       // distinct and readable
    }


    public static String highlight(String s) {
        var colorScheme = new IntelliJDarkAnsi();

        StringBuilder out = new StringBuilder(s.length() * 4);

        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);

            // ----- STRING -----
            if (c == '"') {
                int start = i++;
                while (i < s.length()) {
                    if (s.charAt(i) == '"' && s.charAt(i - 1) != '\\') {
                        i++;
                        break;
                    }
                    i++;
                }
                out.append(colorScheme.STRING)
                    .append(s, start, i)
                    .append(colorScheme.RESET);
                continue;
            }

            // ----- SYMBOL -----
            if (isSymbol(c)) {
                out.append(colorScheme.SYMBOL)
                    .append(c)
                    .append(colorScheme.RESET);
                i++;
                continue;
            }

            // ----- NUMBER -----
            if (Character.isDigit(c)) {
                int start = i++;
                while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
                out.append(colorScheme.NUMBER)
                    .append(s, start, i)
                    .append(colorScheme.RESET);
                continue;
            }

            // ----- IDENTIFIER -----
            if (Character.isLetter(c) || c == '_') {
                int start = i++;
                while (i < s.length()) {
                    char cc = s.charAt(i);
                    if (!Character.isLetterOrDigit(cc) && cc != '_') break;
                    i++;
                }
                out.append(colorScheme.IDENTIFIER)
                    .append(s, start, i)
                    .append(colorScheme.RESET);
                continue;
            }

            // other
            out.append(c);
            i++;
        }

        return out.toString();
    }

    private static boolean isSymbol(char c) {
        int type = Character.getType(c);
        if(type == Character.MATH_SYMBOL
            || type == Character.CURRENCY_SYMBOL
            || type == Character.MODIFIER_SYMBOL
            || type == Character.OTHER_SYMBOL){
            return true;
        }

        return switch (c) {
            case '=', '+', '-', '*', '/', '[', ']', '{', '}',
                 '(', ')', '<', '>', ',', ':', ';' -> true;
            default -> false;
        };
    }
}
