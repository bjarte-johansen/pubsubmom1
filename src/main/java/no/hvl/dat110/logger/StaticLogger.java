package no.hvl.dat110.logger;

import java.util.*;

enum StaticLoggerBlockFormatType {
    BEGIN_END,
    BRACES,
    PREFIX_WITH_BRACES,
    NAME_BRACES_END,
    NAME_INDENT
};


class StaticLoggerContext {
    public StaticLogger logger = new StaticLogger();
    public Deque<String> stack = new ArrayDeque<>();
    public Integer indentLevel = 0;
    public String indentUnit = "    ";
    public StaticLoggerBlockFormatType format = StaticLoggerBlockFormatType.NAME_INDENT;
    public Map<Integer, String> indentCache = new HashMap<>();
    public boolean autoEnter = false;

    public StaticLogger getLogger() {
        return logger;
    }

    public StaticLoggerContext enter(String name) {
        stack.push(name);

        switch(format) {
            case BEGIN_END -> print("BEGIN (" + name + ")");
            case PREFIX_WITH_BRACES -> print("BEGIN (" + name + ") {");
            case NAME_BRACES_END -> print(name + " {");
            case BRACES -> print("{");
            case NAME_INDENT -> print(name + ":");
        }

        indentLevel = indentLevel + 1;

        return this;
    }

    public StaticLoggerContext leave() {
        indentLevel = indentLevel - 1;

        if(!stack.isEmpty()){
            String popped = stack.pop();

            switch(format) {
                case BEGIN_END -> print("END " + popped);
                case PREFIX_WITH_BRACES, NAME_BRACES_END, BRACES  -> print("}");
                case NAME_INDENT -> {} // no closing format for this type
            }
        }

        return this;
    }

    public void print(String msg){
        String spaces = indentCache.computeIfAbsent(
            indentLevel,
            (l) -> indentUnit.repeat(l)
        );
        System.out.println( spaces + msg );
    }
}


class StaticLogger implements AutoCloseable{
    private static final ThreadLocal<StaticLoggerContext> CTX = ThreadLocal.withInitial(StaticLoggerContext::new);

    /**
     * This method provides access to the current StaticLoggerContext for the thread.
     *
     * @return The current StaticLoggerContext instance for the thread.
     */
    protected static StaticLoggerContext ctx(){
        return CTX.get();
    }


    // AutoCloseable implementation

    @Override
    public void close(){
        //System.out.println("Logger: close() called");
        leave();
    }


    // block management methods

    public static StaticLogger scoped(String msg) {
        return ctx().enter(msg).getLogger();
    }

    public static StaticLogger enter(String msg) {
        return ctx().enter(msg).getLogger();
    }

    public static StaticLogger leave() {
        return ctx().leave().getLogger();
    }



    // logging methods

    protected static void logKeyValue(String key, Object value) {
        print(key + ": " + value);
    }
    public static void log(Object msg) {
        print(String.valueOf(msg));
    }
    public static void log(String key, Object value) {
        logKeyValue(key, value);
    }
    public static void log(Object[] tmp) {
        print(Arrays.toString(tmp));
    }

    public static void log(String key, Object[] tmp) {logKeyValue(key, Arrays.toString(tmp));}

    public static void log(String key, Object arg1, Object arg2, Object... rest) {
        if (rest.length != 0) {
            Object[] tmp = new Object[rest.length + 2];
            tmp[0] = arg1;
            tmp[1] = arg2;
            System.arraycopy(rest, 0, tmp, 2, rest.length);
            logKeyValue(key, Arrays.toString(tmp));
        } else {
            logKeyValue(key, arg1);
        }
    }


    // actual print method

    protected static void print(String msg) {
        ctx().print(msg);
    }
}