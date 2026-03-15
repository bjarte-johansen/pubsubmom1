package no.hvl.dat110.common;


import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

class LoggerFormatType{
    public static final int YAML = 1;
    public static final int BLOCK = 2;
    public static final int TITLED_BLOCK = 3;
    public static final int BEGIN_END = 4;
    public static final int TREE = 5;
}

public class Logger {
    public static boolean DUMB_PRINT = true;
	public static boolean debug = true;
    public static boolean USE_MAGIC_LOGGING = true;
    public static boolean LAYOUT_ADJUST_MAGIC_RIGHT = false;
    public static int FORMAT_TYPE = LoggerFormatType.YAML;
    public static boolean SHOW_TREE = false;
    public static ThreadLocal<Integer> depth = ThreadLocal.withInitial(() -> 0);
    public static ConcurrentHashMap<Integer, String> indentCache = new ConcurrentHashMap<>();

    public static final String GREEN        = "\u001B[32m";
    public static final String DARK_GRAY    = "\u001B[90m";
    public static final String LIGHT_GRAY   = "\u001B[37m";
    public static final String RESET        = "\u001B[0m";
    public static final String UNDERLINE    = "\u001B[4m";
    public static final String BOLD         = "\u001B[1m";
    public static final String REVERSE      = "\u001B[7m";


    public static <T> String formatMagicParameters(String fmt, LMStackFrame callerFrame) {
        // case insensitive region match helper
        BiFunction<Integer, String, Boolean> matchString = (fromIndex, other) -> {
            if (fmt == null || other == null) return false;
            return fmt.regionMatches(true, fromIndex, other, 0, other.length());
        };

        StringBuilder sb = new StringBuilder(fmt.length() + 64);

        // TODO: optimize by scanning for '@' first and only doing region matches when found, to avoid unnecessary
        //  regionMatches calls when fmt is long and has few parameters (AKA theres no need to append characters one
        //  by one when there are no parameters, the common case for short formats like "@classMethod" or "@link")

        for (int i = 0; i < fmt.length(); ) {
            char ch = fmt.charAt(i);

            if (ch == '@') {
                if (matchString.apply(i, "@classMethod")) {
                    sb.append(callerFrame.getClassAndMethod());
                    i += 12;
                    continue;
                } else if (matchString.apply(i, "@class")) {
                    sb.append(callerFrame.getSimpleName());
                    i += 6;
                    continue;
                } else if (matchString.apply(i, "@method")) {
                    sb.append(callerFrame.getMethodName());
                    i += 7;
                    continue;
                } else if (matchString.apply(i, "@link")) {
                    sb.append(callerFrame.getSourceLink("@ "));
                    i += 5;
                    continue;
                } else if (matchString.apply(i, "@line")) {
                    sb.append(callerFrame.getLineNumber());
                    i += 5;
                    continue;
                } else if (matchString.apply(i, "@file")) {
                    sb.append(callerFrame.getFileName());
                    i += 5;
                    continue;
                }
            }

            sb.append(ch);
            i++;
        }

        return sb.toString();
    }


    /*
        Indent-handling in setup of try-with entering scope
    */

    public static LoggerScope scope(String title){
        // magic logging
        if (USE_MAGIC_LOGGING) {
            var callerFrame = LMStackFrame.createFromCurrentStack(0);

            switch(FORMAT_TYPE) {
                case LoggerFormatType.BEGIN_END:
                    System.out.println(getIndentStr() + GREEN + "### " + LIGHT_GRAY + "BEGIN " + title + " - " + formatMagicParameters("@classMethod @link", callerFrame) + ":" + RESET);
                    break;

                case LoggerFormatType.YAML:
                    System.out.println(getIndentStr() + LIGHT_GRAY + title + " - " + formatMagicParameters("@classMethod @link", callerFrame) + ":" + RESET);
                    break;

                case LoggerFormatType.BLOCK:
                    System.out.println(getIndentStr() + LIGHT_GRAY + formatMagicParameters("@classMethod @link", callerFrame) + " {" + RESET);
                    break;

                case LoggerFormatType.TITLED_BLOCK:
                    System.out.println(getIndentStr() + LIGHT_GRAY + title + " - " + formatMagicParameters("@classMethod @link", callerFrame) + " {" + RESET);
                    break;
            }
        }

        // enter block
        enter();

        // return scope object that will leave block on close
        return () -> {
            // leave block
            leave();

            // magic logging
            switch(FORMAT_TYPE) {
                case LoggerFormatType.BEGIN_END:
                    System.out.println(getIndentStr() + GREEN + "### " + LIGHT_GRAY + "LEAVE" + RESET);
                    break;
               case LoggerFormatType.YAML:
                   break;

               case LoggerFormatType.BLOCK:
               case LoggerFormatType.TITLED_BLOCK:
                   System.out.println("}");
                   break;

                //System.out.println(getIndentStr() + LIGHT_GRAY + "}" + RESET);
            }
        };
    }

    public static void enter() {
        //System.out.println("│   ".repeat(depth.get()) + "├── Node");

        depth.set(depth.get() + 1);
    }
    public static void leave() {
        depth.set(depth.get() - 1);
    }



    /*
        Internal logging method that handles indentation and optional magic logging features.
        This method is synchronized to ensure thread safety when multiple threads are logging
        simultaneously.
    */

    protected static String getIndentStr(){
        return indentCache.computeIfAbsent(depth.get(), d -> "    ".repeat(Math.max(0, d)));
    }


    /*
    ansi functions
     */

    public static class AnsiStringUtils {
        private static final Pattern ANSI = Pattern.compile("\\u001B\\[[0-9;?]*[ -/]*[@-~]");

        public static String stripAnsi(String s) {
            return ANSI.matcher(s).replaceAll("");
        }

        public static int visibleLength(String s) {
            return stripAnsi(s).length();
        }
    }

    //

    static String padRight(String s, int totalWidth) {
        int paddingWidth = totalWidth - AnsiStringUtils.visibleLength(s);
        if (paddingWidth > 0) {
            return s + " ".repeat(paddingWidth);
        } else {
            return s;
        }
    }

    protected static void doLog(String s){
        String indentStr = getIndentStr();

        synchronized (Logger.class) {
            String magicOut = "";
            String userOut = LoggerSyntaxHighlight.highlight(s);

            if(DUMB_PRINT){
                //System.out.print(indentStr);
                System.out.println(userOut);
                return;
            }

            if(USE_MAGIC_LOGGING) {
                var callerFrame = LMStackFrame.createFromCurrentStack(1);
                magicOut = DARK_GRAY + formatMagicParameters("@classMethod @link", callerFrame) + RESET;
            }

            if(LAYOUT_ADJUST_MAGIC_RIGHT && !magicOut.isEmpty()) {
                int totalWidth = 104;
                int paddingWidth = totalWidth - indentStr.length() - s.length();
                String padding = " ".repeat(Math.max(0, paddingWidth));

                System.out.print(indentStr);
                System.out.println(userOut + padding + magicOut);
            } else {
                System.out.print(indentStr);
                System.out.println(magicOut);
                System.out.println(indentStr + "\t" + s);
            }
        }
    }

    public static void log(String s) {
        if (debug) {
            doLog(s);
        }
    }

    public static void log(String... args) {
        if (debug) {
            StringJoiner joiner = new StringJoiner(" ");
            for (String arg : args) {
                if (arg == null) {
                    arg = "null";
                }
                joiner.add(arg);
            }
            doLog(joiner.toString());
        }
    }

    public static void lg(String s) {
        if (debug) {
            doLog(s);
		}
	}
}
