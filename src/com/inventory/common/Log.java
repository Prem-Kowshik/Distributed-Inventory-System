package com.inventory.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Tiny timestamped, colour-coded console logger shared by all processes. */
public final class Log {
    public static final String RESET = "\u001b[0m", BOLD = "\u001b[1m", RED = "\u001b[31m",
            GREEN = "\u001b[32m", YELLOW = "\u001b[33m", BLUE = "\u001b[34m",
            MAGENTA = "\u001b[35m", CYAN = "\u001b[36m";

    private static final boolean COLOR = System.getenv("NO_COLOR") == null;
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static String name = "?";
    private static String nameColor = "";

    private Log() {}

    public static void init(String processName, String color) {
        name = processName;
        nameColor = color;
    }

    private static synchronized void out(String style, String tag, String msg) {
        String ts = LocalTime.now().format(TS);
        if (COLOR) {
            System.out.println(ts + " " + nameColor + "[" + name + "]" + RESET + " " + style + tag + msg + RESET);
        } else {
            System.out.println(ts + " [" + name + "] " + tag + msg);
        }
        System.out.flush();
    }

    private static String fmt(String f, Object... a) { return a.length == 0 ? f : String.format(f, a); }

    public static void info(String f, Object... a)  { out("", "", fmt(f, a)); }
    public static void ok(String f, Object... a)    { out(GREEN, "", fmt(f, a)); }
    public static void event(String f, Object... a) { out(BOLD + CYAN, ">> ", fmt(f, a)); }
    public static void warn(String f, Object... a)  { out(YELLOW, "WARN ", fmt(f, a)); }
    public static void error(String f, Object... a) { out(BOLD + RED, "!! ", fmt(f, a)); }

    public static String c(String color, String s) { return COLOR ? color + s + RESET : s; }
}
