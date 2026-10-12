package com.example.url_shortner.exceptions;

public final class LogCodes {
    private LogCodes() {}

    // Warning codes (warn_<number>)
    public static final String WARN_LINK_EXPIRED       = "warn_001";
    public static final String WARN_LINK_NOT_FOUND     = "warn_002";
    public static final String WARN_INVALID_SHORT_CODE = "warn_003";
    public static final String WARN_DUPLICATE_CODE     = "warn_004";

    // Error codes (error_<number>)
    public static final String ERR_LINK_PERSISTENCE    = "error_001";
    public static final String ERR_KAFKA_DISPATCH      = "error_002";
    public static final String ERR_INTERNAL_SERVER     = "error_003";
}
