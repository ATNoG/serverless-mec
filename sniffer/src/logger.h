/* Colors */
#define COLOR_GRAY "\033[0;37m"
#define COLOR_GREEN "\033[0;32m"
#define COLOR_RED "\033[0;31m"
#define COLOR_ORANGE "\033[0;33m"
#define COLOR_RESET "\033[0m"

/* Levels */
#define LEVEL_DEBUG 4
#define LEVEL_INFO 3
#define LEVEL_WARNING 2
#define LEVEL_ERROR 1

/* Methods */
#define log_error(msg, ...) logger_error(msg, ##__VA_ARGS__)
#define log_warn(msg, ...)  logger_warn(msg, ##__VA_ARGS__)
#define log_debug(msg, ...) logger_debug(msg, ##__VA_ARGS__)
#define log_info(msg, ...) logger_info(msg, ##__VA_ARGS__)
void logger_error(const char* format, ...);
void logger_warn(const char* format, ...);
void logger_debug(const char* format, ...);
void logger_info(const char* format, ...);
