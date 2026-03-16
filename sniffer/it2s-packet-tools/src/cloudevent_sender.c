#define _GNU_SOURCE
#include "cloudevent_sender.h"

#include <arpa/inet.h>
#include <curl/curl.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <stdarg.h>

typedef struct {
    char *body;
    size_t body_len;

    char ce_id[37];
    char ce_type[128];
    uint64_t seq_no;
    int64_t t_event_unix_ns;
    int64_t t_ce_built_unix_ns;
    int64_t t_enqueue_unix_ns;
} ce_job_t;

typedef struct {
    ce_job_t **items;
    int cap, head, tail, count;
    pthread_mutex_t mu;
    pthread_cond_t cv_not_empty;
} ce_jobq_t;

static ce_sender_config_t g_cfg;
static bool g_cfg_valid = false;
static bool g_sender_enabled = false;
static volatile sig_atomic_t g_stop = 0;

static pthread_t g_sender_thread;
static bool g_sender_started = false;

static pthread_t g_health_thread;
static bool g_health_started = false;

static ce_jobq_t g_q;
static bool g_q_initialized = false;

static char g_ce_source_buf[256] = {0};

static void ce_log_info(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[ce] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

static void ce_log_warn(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[ce][WARN] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

static void ce_log_error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[ce][ERROR] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

static int64_t now_unix_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t) ts.tv_sec * 1000000000LL + (int64_t) ts.tv_nsec;
}

static int64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000000LL + (int64_t) ts.tv_nsec;
}

static void iso8601_now(char *out, size_t out_sz) {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    struct tm tm;
    time_t sec = tv.tv_sec;
    gmtime_r(&sec, &tm);

    int n = snprintf(out, out_sz,
                     "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec, (long) tv.tv_usec);
    if (n < 0 || (size_t) n >= out_sz) {
        snprintf(out, out_sz, "1970-01-01T00:00:00.000000Z");
    }
}

static char *json_escape(const char *s) {
    if (!s) s = "";

    size_t n = 0;
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '\"':
            case '\\':
            case '\b':
            case '\f':
            case '\n':
            case '\r':
            case '\t':
                n += 2;
                break;
            default:
                if ((unsigned char) *p < 0x20) n += 6;
                else n += 1;
        }
    }

    char *out = (char *) malloc(n + 1);
    if (!out) return NULL;

    char *o = out;
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '\"': *o++ = '\\'; *o++ = '\"'; break;
            case '\\': *o++ = '\\'; *o++ = '\\'; break;
            case '\b': *o++ = '\\'; *o++ = 'b'; break;
            case '\f': *o++ = '\\'; *o++ = 'f'; break;
            case '\n': *o++ = '\\'; *o++ = 'n'; break;
            case '\r': *o++ = '\\'; *o++ = 'r'; break;
            case '\t': *o++ = '\\'; *o++ = 't'; break;
            default:
                if ((unsigned char) *p < 0x20) {
                    sprintf(o, "\\u%04x", (unsigned char) *p);
                    o += 6;
                } else {
                    *o++ = *p;
                }
        }
    }
    *o = '\0';
    return out;
}

static bool uuid4(char out[37]) {
    uint8_t b[16];
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) return false;

    ssize_t r = read(fd, b, sizeof(b));
    close(fd);
    if (r != (ssize_t) sizeof(b)) return false;

    b[6] = (b[6] & 0x0F) | 0x40;
    b[8] = (b[8] & 0x3F) | 0x80;

    snprintf(out, 37,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
             b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
    return true;
}

static void jobq_init(ce_jobq_t *q, int cap) {
    q->items = (ce_job_t **) calloc((size_t) cap, sizeof(ce_job_t *));
    q->cap = cap;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->cv_not_empty, NULL);
}

static void jobq_destroy(ce_jobq_t *q) {
    pthread_mutex_lock(&q->mu);
    for (int i = 0; i < q->cap; i++) {
        if (q->items[i]) {
            free(q->items[i]->body);
            free(q->items[i]);
        }
    }
    free(q->items);
    pthread_mutex_unlock(&q->mu);

    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->cv_not_empty);
}

static bool jobq_try_push(ce_jobq_t *q, ce_job_t *job) {
    bool ok = false;

    pthread_mutex_lock(&q->mu);
    if (q->count < q->cap) {
        q->items[q->tail] = job;
        q->tail = (q->tail + 1) % q->cap;
        q->count++;
        ok = true;
        pthread_cond_signal(&q->cv_not_empty);
    }
    pthread_mutex_unlock(&q->mu);

    return ok;
}

static ce_job_t *jobq_pop_block(ce_jobq_t *q) {
    pthread_mutex_lock(&q->mu);
    while (q->count == 0 && !g_stop) {
        pthread_cond_wait(&q->cv_not_empty, &q->mu);
    }

    ce_job_t *job = NULL;
    if (q->count > 0) {
        job = q->items[q->head];
        q->items[q->head] = NULL;
        q->head = (q->head + 1) % q->cap;
        q->count--;
    }
    pthread_mutex_unlock(&q->mu);

    return job;
}

static char *build_cloudevent_structured(const char *event_type_or_null,
                                         const char *subject_or_null,
                                         const char *data_json_obj,
                                         int stationtype_or_neg,
                                         const char *ce_id,
                                         size_t *out_len) {
    const char *event_type =
        (event_type_or_null && event_type_or_null[0])
            ? event_type_or_null
            : ((g_cfg.ce_type && g_cfg.ce_type[0]) ? g_cfg.ce_type : "its.packet");

    char time_buf[64];
    iso8601_now(time_buf, sizeof(time_buf));

    char *type_esc = json_escape(event_type);
    char *source_esc = json_escape(g_cfg.ce_source ? g_cfg.ce_source : "sniffer://unknown");
    char *id_esc = json_escape(ce_id ? ce_id : "00000000-0000-4000-8000-000000000000");
    char *time_esc = json_escape(time_buf);

    char *subject_part = NULL;
    if (subject_or_null && subject_or_null[0]) {
        char *subject_esc = json_escape(subject_or_null);
        if (!subject_esc) {
            free(type_esc);
            free(source_esc);
            free(id_esc);
            free(time_esc);
            return NULL;
        }
        size_t part_sz = strlen(subject_esc) + 32;
        subject_part = (char *) malloc(part_sz);
        if (!subject_part) {
            free(subject_esc);
            free(type_esc);
            free(source_esc);
            free(id_esc);
            free(time_esc);
            return NULL;
        }
        snprintf(subject_part, part_sz, ",\"subject\":\"%s\"", subject_esc);
        free(subject_esc);
    } else {
        subject_part = strdup("");
    }

    char stationtype_part[64];
    stationtype_part[0] = '\0';
    if (stationtype_or_neg >= 0) {
        snprintf(stationtype_part, sizeof(stationtype_part),
                 ",\"stationtype\":%d", stationtype_or_neg);
    }

    if (!type_esc || !source_esc || !id_esc || !time_esc || !subject_part) {
        free(type_esc);
        free(source_esc);
        free(id_esc);
        free(time_esc);
        free(subject_part);
        return NULL;
    }

    size_t body_sz =
        strlen(type_esc) + strlen(source_esc) + strlen(id_esc) +
        strlen(time_esc) + strlen(subject_part) +
        strlen(stationtype_part) + strlen(data_json_obj) + 256;

    char *body = (char *) malloc(body_sz);
    if (!body) {
        free(type_esc);
        free(source_esc);
        free(id_esc);
        free(time_esc);
        free(subject_part);
        return NULL;
    }

    snprintf(body, body_sz,
             "{"
               "\"specversion\":\"1.0\","
               "\"type\":\"%s\","
               "\"source\":\"%s\","
               "\"id\":\"%s\","
               "\"time\":\"%s\","
               "\"datacontenttype\":\"application/json\""
               "%s"
               "%s,"
               "\"data\":%s"
             "}",
             type_esc,
             source_esc,
             id_esc,
             time_esc,
             subject_part,
             stationtype_part,
             data_json_obj);

    if (out_len) *out_len = strlen(body);

    free(type_esc);
    free(source_esc);
    free(id_esc);
    free(time_esc);
    free(subject_part);

    return body;
}

static void *health_thread_fn(void *arg) {
    (void) arg;

    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        ce_log_error("health socket() failed: %s", strerror(errno));
        return NULL;
    }

    int one = 1;
    (void) setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t) g_cfg.health_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(s, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        ce_log_error("health bind(0.0.0.0:%d) failed: %s",
                     g_cfg.health_port, strerror(errno));
        close(s);
        return NULL;
    }

    if (listen(s, 16) < 0) {
        ce_log_error("health listen() failed: %s", strerror(errno));
        close(s);
        return NULL;
    }

    ce_log_info("health listening on 0.0.0.0:%d", g_cfg.health_port);

    const char resp[] =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 2\r\n"
        "Connection: close\r\n"
        "\r\n"
        "OK";

    while (!g_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int r = select(s + 1, &rfds, NULL, NULL, &tv);
        if (r < 0) {
            if (errno == EINTR) continue;
            ce_log_warn("health select() error: %s", strerror(errno));
            continue;
        }
        if (r == 0) continue;

        if (FD_ISSET(s, &rfds)) {
            int c = accept(s, NULL, NULL);
            if (c < 0) {
                if (errno == EINTR) continue;
                ce_log_warn("health accept() error: %s", strerror(errno));
                continue;
            }

            char buf[512];
            (void) read(c, buf, sizeof(buf));
            (void) write(c, resp, sizeof(resp) - 1);
            close(c);
        }
    }

    close(s);
    return NULL;
}

static void *sender_thread_fn(void *arg) {
    (void) arg;

    CURL *curl = curl_easy_init();
    if (!curl) {
        ce_log_error("curl_easy_init failed");
        return NULL;
    }

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/cloudevents+json");

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, g_cfg.sink_url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long) g_cfg.curl_timeout_sec);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_PROXY, "");
    curl_easy_setopt(curl, CURLOPT_NOPROXY, "*");
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

    while (!g_stop) {
        ce_job_t *job = jobq_pop_block(&g_q);
        if (!job) continue;

        int64_t t_send_start_unix_ns = now_unix_ns();
        int64_t t0m = mono_ns();

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, job->body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long) job->body_len);

        CURLcode rc = curl_easy_perform(curl);

        int64_t t1m = mono_ns();
        int64_t t_send_end_unix_ns = now_unix_ns();

        long status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

        long long elapsed_ns = (long long) (t1m - t0m);

        if (rc != CURLE_OK) {
            ce_log_warn("POST seq=%" PRIu64 " type=%s elapsed_ns=%lld status=%ld curl_err=%s",
                        job->seq_no, job->ce_type, elapsed_ns, status, curl_easy_strerror(rc));
        } else {
            ce_log_info("POST seq=%" PRIu64 " type=%s elapsed_ns=%lld status=%ld",
                        job->seq_no, job->ce_type, elapsed_ns, status);
        }

        fprintf(stdout,
                "{\"kind\":\"bench\",\"component\":\"%s\",\"node\":\"%s\",\"ce_id\":\"%s\","
                "\"ce_type\":\"%s\","
                "\"frame_no\":%" PRIu64 ","
                "\"t_capture_unix_ns\":%" PRId64 ","
                "\"t_ce_built_unix_ns\":%" PRId64 ","
                "\"t_enqueue_unix_ns\":%" PRId64 ","
                "\"t_send_start_unix_ns\":%" PRId64 ","
                "\"t_send_end_unix_ns\":%" PRId64 ","
                "\"send_elapsed_ns\":%lld,"
                "\"http_status\":%ld,"
                "\"curl_rc\":%d}\n",
                g_cfg.component ? g_cfg.component : "sniffer",
                g_cfg.node_name ? g_cfg.node_name : "unknown",
                job->ce_id,
                job->ce_type,
                job->seq_no,
                job->t_event_unix_ns,
                job->t_ce_built_unix_ns,
                job->t_enqueue_unix_ns,
                t_send_start_unix_ns,
                t_send_end_unix_ns,
                elapsed_ns,
                status,
                (int) rc);
        fflush(stdout);

        free(job->body);
        free(job);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return NULL;
}

void ce_sender_config_from_env(ce_sender_config_t *cfg,
                               const char *default_component,
                               const char *default_iface) {
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));

    const char *sink = getenv("K_SINK");
    const char *ce_type = getenv("CE_TYPE");
    const char *ce_source = getenv("CE_SOURCE");
    const char *queue_max = getenv("SEND_QUEUE_MAX");
    const char *port = getenv("PORT");

    const char *node_name =
        getenv("K8S_NODE_NAME") ? getenv("K8S_NODE_NAME") :
        getenv("NODE_NAME")     ? getenv("NODE_NAME")     :
        getenv("HOSTNAME")      ? getenv("HOSTNAME")      : "host";

    cfg->sink_url = (sink && sink[0]) ? sink : "";
    cfg->ce_type = (ce_type && ce_type[0]) ? ce_type : "its.packet";
    cfg->ce_source = (ce_source && ce_source[0]) ? ce_source : NULL;
    cfg->component = default_component ? default_component : "sniffer";
    cfg->iface = default_iface ? default_iface : "lo";
    cfg->node_name = node_name;
    cfg->send_queue_max = 1000;
    cfg->curl_timeout_sec = 5;
    cfg->enable_health_server = false;
    cfg->health_port = 8080;

    if (queue_max && queue_max[0]) {
        cfg->send_queue_max = atoi(queue_max);
        if (cfg->send_queue_max <= 0) cfg->send_queue_max = 1000;
    }

    if (port && port[0]) {
        cfg->enable_health_server = true;
        cfg->health_port = atoi(port);
        if (cfg->health_port <= 0 || cfg->health_port > 65535) {
            cfg->health_port = 8080;
        }
    }

    if (!cfg->ce_source) {
        snprintf(g_ce_source_buf, sizeof(g_ce_source_buf),
                 "%s://%s/%s",
                 cfg->component ? cfg->component : "sniffer",
                 cfg->node_name ? cfg->node_name : "host",
                 cfg->iface ? cfg->iface : "lo");
        cfg->ce_source = g_ce_source_buf;
    }
}

int ce_sender_init(const ce_sender_config_t *cfg) {
    if (!cfg) return -1;

    memset(&g_cfg, 0, sizeof(g_cfg));
    g_cfg = *cfg;
    g_cfg_valid = true;
    g_stop = 0;

    if (g_cfg.enable_health_server) {
        if (pthread_create(&g_health_thread, NULL, health_thread_fn, NULL) != 0) {
            ce_log_error("failed to start health thread");
            g_cfg_valid = false;
            return -1;
        }
        g_health_started = true;
    }

    if (!g_cfg.sink_url || !g_cfg.sink_url[0]) {
        g_sender_enabled = false;
        ce_log_info("CloudEvents disabled (K_SINK not set)");
        return 0;
    }

    jobq_init(&g_q, g_cfg.send_queue_max);
    g_q_initialized = true;

    curl_global_init(CURL_GLOBAL_ALL);
    if (pthread_create(&g_sender_thread, NULL, sender_thread_fn, NULL) != 0) {
        ce_log_error("failed to start sender thread");
        curl_global_cleanup();
        jobq_destroy(&g_q);
        g_q_initialized = false;
        return -1;
    }

    g_sender_started = true;
    g_sender_enabled = true;

    ce_log_info("CloudEvents enabled -> %s", g_cfg.sink_url);
    ce_log_info("type fallback=%s source=%s queue=%d",
                g_cfg.ce_type, g_cfg.ce_source, g_cfg.send_queue_max);

    return 0;
}

void ce_sender_shutdown(void) {
    if (!g_cfg_valid) return;

    g_stop = 1;

    if (g_sender_started && g_q_initialized) {
        pthread_mutex_lock(&g_q.mu);
        pthread_cond_broadcast(&g_q.cv_not_empty);
        pthread_mutex_unlock(&g_q.mu);

        pthread_join(g_sender_thread, NULL);
        curl_global_cleanup();
        jobq_destroy(&g_q);

        g_q_initialized = false;
        g_sender_started = false;
        g_sender_enabled = false;
    }

    if (g_health_started) {
        pthread_join(g_health_thread, NULL);
        g_health_started = false;
    }

    g_cfg_valid = false;
}

bool ce_sender_enabled(void) {
    return g_sender_enabled;
}

int ce_sender_send_json(const char *event_type_or_null,
                        const char *subject_or_null,
                        const char *data_json_obj,
                        int stationtype_or_neg,
                        uint64_t seq_no,
                        int64_t t_event_unix_ns,
                        ce_sender_meta_t *out_meta) {
    if (!g_sender_enabled) return 0;
    if (!data_json_obj || !data_json_obj[0]) return -1;

    const char *resolved_type =
        (event_type_or_null && event_type_or_null[0])
            ? event_type_or_null
            : ((g_cfg.ce_type && g_cfg.ce_type[0]) ? g_cfg.ce_type : "its.packet");

    char ce_id[37];
    if (!uuid4(ce_id)) {
        snprintf(ce_id, sizeof(ce_id),
                 "00000000-0000-4000-8000-000000000000");
    }

    int64_t t_ce_built_unix_ns = now_unix_ns();

    size_t body_len = 0;
    char *body = build_cloudevent_structured(
        resolved_type,
        subject_or_null,
        data_json_obj,
        stationtype_or_neg,
        ce_id,
        &body_len
    );
    if (!body) return -1;

    ce_job_t *job = (ce_job_t *) calloc(1, sizeof(ce_job_t));
    if (!job) {
        free(body);
        return -1;
    }

    job->body = body;
    job->body_len = body_len;
    snprintf(job->ce_id, sizeof(job->ce_id), "%s", ce_id);
    snprintf(job->ce_type, sizeof(job->ce_type), "%s", resolved_type);
    job->seq_no = seq_no;
    job->t_event_unix_ns = t_event_unix_ns;
    job->t_ce_built_unix_ns = t_ce_built_unix_ns;
    job->t_enqueue_unix_ns = now_unix_ns();

    if (!jobq_try_push(&g_q, job)) {
        ce_log_warn("SEND_QUEUE full, dropping seq=%" PRIu64, seq_no);
        free(job->body);
        free(job);
        return -1;
    }

    if (out_meta) {
        snprintf(out_meta->ce_id, sizeof(out_meta->ce_id), "%s", ce_id);
        out_meta->seq_no = seq_no;
        out_meta->t_event_unix_ns = t_event_unix_ns;
        out_meta->t_ce_built_unix_ns = t_ce_built_unix_ns;
        out_meta->t_enqueue_unix_ns = job->t_enqueue_unix_ns;
    }

    return 0;
}
