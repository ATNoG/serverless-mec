#include <stdarg.h>
#include <stdio.h>
#include "logger.h"

void logger_info(const char* format, ...) {
    va_list args;
    va_start(args, format);
    printf("%s[info]%s ", COLOR_GREEN, COLOR_RESET);
    vprintf(format, args);
    printf("\n");
    va_end(args);
}

void logger_debug(const char* format, ...) {
    va_list args;
    va_start(args, format);
    printf("%s[debug]%s ", COLOR_GRAY, COLOR_RESET);
    vprintf(format, args);
    printf("\n");
    va_end(args);
}

void logger_warn(const char* format, ...) {
    va_list args;
    va_start(args, format);
    printf("%s[warn]%s ", COLOR_ORANGE, COLOR_RESET);
    vprintf(format, args);
    printf("\n");
    va_end(args);
}

void logger_error(const char* format, ...) {
    va_list args;
    va_start(args, format);
    printf("%s[error]%s ", COLOR_RED, COLOR_RESET);
    vprintf(format, args);
    printf("\n");
    va_end(args);
}
