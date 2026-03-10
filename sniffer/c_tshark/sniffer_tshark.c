// sniffer_tshark.c — tshark capture+decode (ITS payload, pyshark-like) + CloudEvents structured sender + bench NDJSON
// Build (Alpine): gcc -O2 -Wall -Wextra -pthread sniffer_tshark.c -lcurl -o sniffer_tshark
//
// What this program does:
//  1) Spawns `tshark` to capture packets live from IFACE using a BPF filter (and optional display filter).
//  2) Uses tshark's `-T ek` JSON output to extract the decoded ITS layer fields into a "cam_fields" object.
//  3) Prints an NDJSON record for each decoded ITS packet (stdout), and optionally includes the full frame hex.
//  4) If K_SINK is configured, wraps the NDJSON record as a *structured* CloudEvent JSON and POSTs it.
//  5) Sender runs on a separate thread with a bounded queue.
//  6) Emits additional NDJSON "bench" records (exact format) to measure capture->send timings.
//  7) If PORT is set, opens a tiny HTTP server on that port (Knative readiness).
//
// Design notes:
//  - Log formats are kept stable (especially "sniffer POST ..." and bench NDJSON) for downstream tooling.
//  - We avoid adding extra CloudEvent extensions (besides optional stationtype) to prevent Broker 400s.
//  - We can include "frame_raw_hex" in CloudEvent data even if stdout doesn't include it.
//
// If you want only CAM, set e.g.
//   DISPLAY_FILTER='btpb.dstport == 2001 && its.messageId == 2'
//
#define _GNU_SOURCE

#include <curl/curl.h>

#include <arpa/inet.h>   /* htons/htonl/INADDR_ANY */
#include <netinet/in.h>  /* sockaddr_in */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* -------------------------
 * Config via environment
 *
 * Mirrors the Python config knobs:
 *  IFACE              capture interface
 *  BPF                pcap BPF filter (tshark -f)
 *  DISPLAY_FILTER     wireshark display filter (tshark -Y), optional
 *  LOG_EVERY          periodic progress log cadence
 *  CE_TYPE            CloudEvent type
 *  INCLUDE_RAW_HEX    include frame_raw_hex in stdout NDJSON
 *  CE_INCLUDE_RAW_HEX include frame_raw_hex in CloudEvent data (default ON)
 *  K_SINK             HTTP URL to send CloudEvents to (Knative Broker ingress)
 *  STDOUT_NDJSON      print per-packet NDJSON record
 *  PROMISCUOUS        capture in promisc mode (if false, pass -p to tshark)
 *  SEND_QUEUE_MAX     bounded queue depth between capture loop and sender thread
 *  TSHARK_BIN         override tshark binary (default "tshark")
 *  PORT               if set, run health server on that port (Knative readiness)
 * ------------------------- */
static const char *IFACE;
static const char *BPF;
static const char *DISPLAY_FILTER;
static int   LOG_EVERY = 10;
static const char *CE_TYPE;
static bool  INCLUDE_RAW_HEX = false;   /* stdout record */
static bool  CE_INCLUDE_RAW_HEX = true; /* CloudEvents data (default ON) */
static const char *SINK_URL;            /* K_SINK */
static bool  STDOUT_NDJSON = true;
static bool  PROMISCUOUS = false;
static int   SEND_QUEUE_MAX = 1000;
static const char *TSHARK_BIN = "tshark";

/* Bench: node name (used in bench NDJSON) */
static char ENV_NODE[128] = {0};
/* CloudEvent source (sniffer://<node>/<iface>) */
static char CE_SOURCE[256] = {0};

/* Knative readiness */
static int  PORT = 8080;
static bool HAS_PORT = false;

/* -------------------------
 * Stop flags
 *
 * g_stop:         global shutdown signal for capture loop + health server
 * g_sender_stop:  lets sender thread exit cleanly even if capture already stopped
 * ------------------------- */
static volatile sig_atomic_t g_stop = 0;
static volatile sig_atomic_t g_sender_stop = 0;
static void on_sig(int sig) { (void)sig; g_stop = 1; }

/* -------------------------
 * Logging
 *
 * Logs to stdout with "just the message" format, like the Python logging format="%(message)s"
 * ------------------------- */
static void log_line(const char *fmt, ...) {
  va_list ap; va_start(ap, fmt);
  vprintf(fmt, ap);
  printf("\n");
  va_end(ap);
  fflush(stdout);
}

/* -------------------------
 * Bench helpers
 *
 * now_unix_ns(): wall-clock ns (CLOCK_REALTIME) for pipeline timestamping
 * mono_ns():     monotonic ns (CLOCK_MONOTONIC) for measuring durations
 * ------------------------- */
static int64_t now_unix_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}
static int64_t mono_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

/* -------------------------
 * Misc helpers
 * ------------------------- */

/* Parse env-style truthy booleans */
static const char *truthy(const char *s) {
  if (!s) return NULL;
  if (strcmp(s, "1") == 0) return s;
  if (strcasecmp(s, "true") == 0) return s;
  if (strcasecmp(s, "yes") == 0) return s;
  return NULL;
}

/* Produce UTC ISO timestamp */
static void now_iso(char out[64]) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  time_t sec = tv.tv_sec;
  struct tm tm;
  gmtime_r(&sec, &tm);

  if (tv.tv_usec == 0) {
    snprintf(out, 64,
             "%04d-%02d-%02dT%02d:%02d:%02dZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
  } else {
    snprintf(out, 64,
             "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, (long)tv.tv_usec);
  }
}

/* Minimal JSON string escaper (for embedding strings safely into JSON we build with snprintf) */
static char *json_escape(const char *s) {
  if (!s) s = "";
  size_t n = 0;
  for (const char *p = s; *p; p++) {
    switch (*p) {
      case '"': case '\\': case '\b': case '\f': case '\n': case '\r': case '\t':
        n += 2; break;
      default:
        if ((unsigned char)*p < 0x20) n += 6; else n += 1;
    }
  }
  char *out = (char *)malloc(n + 1);
  if (!out) return NULL;

  char *o = out;
  for (const char *p = s; *p; p++) {
    switch (*p) {
      case '"':  *o++='\\'; *o++='"'; break;
      case '\\': *o++='\\'; *o++='\\'; break;
      case '\b': *o++='\\'; *o++='b'; break;
      case '\f': *o++='\\'; *o++='f'; break;
      case '\n': *o++='\\'; *o++='n'; break;
      case '\r': *o++='\\'; *o++='r'; break;
      case '\t': *o++='\\'; *o++='t'; break;
      default:
        if ((unsigned char)*p < 0x20) {
          sprintf(o, "\\u%04x", (unsigned char)*p);
          o += 6;
        } else {
          *o++ = *p;
        }
    }
  }
  *o = '\0';
  return out;
}

/* Helpers for turning tshark hex output into a compact lower-case hex string */
static bool looks_hex(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}
static char hex_lower(char c) {
  if (c >= 'A' && c <= 'F') return (char)(c - 'A' + 'a');
  return c;
}
static char *normalize_hex(const char *in) {
  if (!in || !in[0]) return NULL;
  size_t n = 0;
  for (const char *p = in; *p; p++) if (looks_hex(*p)) n++;
  if (n < 2) return NULL;
  if ((n % 2) != 0) n -= 1;

  char *out = (char *)malloc(n + 1);
  if (!out) return NULL;

  size_t o = 0;
  for (const char *p = in; *p && o < n; p++) {
    if (looks_hex(*p)) out[o++] = hex_lower(*p);
  }
  out[n] = '\0';
  return out;
}

/* UUIDv4 generator for CE id */
static bool uuid4(char out[37]) {
  uint8_t b[16];
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) return false;
  ssize_t r = read(fd, b, sizeof(b));
  close(fd);
  if (r != (ssize_t)sizeof(b)) return false;

  b[6] = (b[6] & 0x0F) | 0x40;
  b[8] = (b[8] & 0x3F) | 0x80;

  snprintf(out, 37,
           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
           b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
           b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
  return true;
}

/* Check binary exists (used to warn if tshark missing) */
static bool has_exec_in_path(const char *name) {
  if (!name || !name[0]) return false;
  if (strchr(name, '/')) return access(name, X_OK) == 0;

  const char *path = getenv("PATH");
  if (!path) return false;

  char *tmp = strdup(path);
  if (!tmp) return false;

  bool ok = false;
  char *p = tmp;
  char *dir;
  while ((dir = strsep(&p, ":")) != NULL) {
    if (!dir[0]) continue;
    char buf[512];
    snprintf(buf, sizeof(buf), "%s/%s", dir, name);
    if (access(buf, X_OK) == 0) { ok = true; break; }
  }
  free(tmp);
  return ok;
}

/* -------------------------
 * Minimal JSON slicing for tshark -T ek output
 *
 * tshark -T ek outputs newline-delimited JSON objects.
 * Each line has a "layers" object containing per-layer decoded fields.
 * We don't use a full JSON parser to keep dependencies tiny.
 * ------------------------- */
static const char *skip_ws(const char *p) {
  while (p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
  return p;
}
static bool find_end_of_string(const char *p, const char **endp) {
  bool esc = false;
  for (p++; *p; p++) {
    if (esc) { esc = false; continue; }
    if (*p == '\\') { esc = true; continue; }
    if (*p == '"') { *endp = p; return true; }
  }
  return false;
}
static bool skip_json_value(const char **pp) {
  const char *p = skip_ws(*pp);
  if (!*p) return false;

  if (*p == '"') {
    const char *e = NULL;
    if (!find_end_of_string(p, &e)) return false;
    *pp = e + 1;
    return true;
  }
  if (*p == '{') {
    int depth = 0;
    bool in_str = false, esc = false;
    for (; *p; p++) {
      char c = *p;
      if (in_str) {
        if (esc) { esc = false; continue; }
        if (c == '\\') { esc = true; continue; }
        if (c == '"') { in_str = false; continue; }
        continue;
      }
      if (c == '"') { in_str = true; continue; }
      if (c == '{') depth++;
      if (c == '}') { depth--; if (depth == 0) { *pp = p + 1; return true; } }
    }
    return false;
  }
  if (*p == '[') {
    int depth = 0;
    bool in_str = false, esc = false;
    for (; *p; p++) {
      char c = *p;
      if (in_str) {
        if (esc) { esc = false; continue; }
        if (c == '\\') { esc = true; continue; }
        if (c == '"') { in_str = false; continue; }
        continue;
      }
      if (c == '"') { in_str = true; continue; }
      if (c == '[') depth++;
      if (c == ']') { depth--; if (depth == 0) { *pp = p + 1; return true; } }
    }
    return false;
  }
  while (*p && *p != ',' && *p != '}' && *p != ']' && *p != '\n' && *p != '\r') p++;
  *pp = p;
  return true;
}

/* Locate a JSON object following a top-level key "key": { ... } and return start pointer + length */
static bool extract_json_object_after_key(
    const char *line,
    const char *key,
    const char **obj_start,
    size_t *obj_len
) {
  if (!line || !key || !obj_start || !obj_len) return false;

  char pat[128];
  snprintf(pat, sizeof(pat), "\"%s\"", key);
  const char *k = strstr(line, pat);
  if (!k) return false;

  const char *p = k + strlen(pat);
  p = skip_ws(p);
  if (*p != ':') return false;
  p++;
  p = skip_ws(p);
  if (*p != '{') return false;

  const char *start = p;
  int depth = 0;
  bool in_str = false, esc = false;

  for (; *p; p++) {
    char c = *p;
    if (in_str) {
      if (esc) { esc = false; continue; }
      if (c == '\\') { esc = true; continue; }
      if (c == '"') { in_str = false; continue; }
      continue;
    }
    if (c == '"') { in_str = true; continue; }
    if (c == '{') depth++;
    if (c == '}') {
      depth--;
      if (depth == 0) {
        *obj_start = start;
        *obj_len = (size_t)(p - start + 1);
        return true;
      }
    }
  }
  return false;
}

/* Copy a named layer object from within the layers JSON object */
static char *extract_layer_object_copy(const char *layers_obj, const char *layer_name) {
  const char *s = NULL;
  size_t n = 0;
  if (!extract_json_object_after_key(layers_obj, layer_name, &s, &n)) return NULL;
  char *out = (char *)malloc(n + 1);
  if (!out) return NULL;
  memcpy(out, s, n);
  out[n] = '\0';
  return out;
}

/* Get a string value for a given key inside a JSON object. Handles value as ["x", ...] by taking first. */
static char *extract_first_string_value(const char *obj, const char *key) {
  if (!obj || !key) return NULL;

  char pat[256];
  snprintf(pat, sizeof(pat), "\"%s\"", key);
  const char *k = strstr(obj, pat);
  if (!k) return NULL;

  const char *p = k + strlen(pat);
  p = skip_ws(p);
  if (*p != ':') return NULL;
  p++;
  p = skip_ws(p);

  if (*p == '[') { p++; p = skip_ws(p); }

  if (*p != '"') return NULL;
  const char *e = NULL;
  if (!find_end_of_string(p, &e)) return NULL;

  size_t n = (size_t)(e - (p + 1));
  char *out = (char *)malloc(n + 1);
  if (!out) return NULL;
  memcpy(out, p + 1, n);
  out[n] = '\0';
  return out;
}

/* -------------------------
 * Flattening tshark layer objects into a single cam_fields dict
 * ------------------------- */
static void to_lower_inplace(char *s) {
  for (; s && *s; s++) if (*s >= 'A' && *s <= 'Z') *s = (char)(*s - 'A' + 'a');
}
static void replace_chars_inplace(char *s) {
  for (; s && *s; s++) if (*s == '.' || *s == ':' || *s == '-' || *s == ' ') *s = '_';
}
static char *normalize_full_key(const char *k) {
  char *tmp = strdup(k ? k : "");
  if (!tmp) return NULL;

  replace_chars_inplace(tmp);

  for (int i = 0; i < 2; i++) {
    if (strncmp(tmp, "its_", 4) == 0) memmove(tmp, tmp + 4, strlen(tmp + 4) + 1);
  }

  char *u = strchr(tmp, '_');
  if (u) {
    size_t tlen = (size_t)(u - tmp);
    char *rest = u + 1;
    if (tlen > 0 && strncmp(rest, tmp, tlen) == 0 && rest[tlen] == '_') {
      memmove(u + 1, rest + tlen + 1, strlen(rest + tlen + 1) + 1);
    }
  }

  to_lower_inplace(tmp);
  return tmp;
}

typedef struct {
  char  *buf;
  size_t len;
  size_t cap;
  bool   first;
} kvb_t;

static bool kvb_init(kvb_t *b) {
  b->cap = 8192;
  b->buf = (char *)malloc(b->cap);
  if (!b->buf) return false;
  b->len = 0;
  b->first = true;
  b->buf[b->len++] = '{';
  b->buf[b->len] = '\0';
  return true;
}
static bool kvb_ensure(kvb_t *b, size_t add) {
  if (b->len + add + 2 <= b->cap) return true;
  size_t ncap = b->cap;
  while (b->len + add + 2 > ncap) ncap *= 2;
  char *tmp = (char *)realloc(b->buf, ncap);
  if (!tmp) return false;
  b->buf = tmp;
  b->cap = ncap;
  return true;
}
static bool kvb_append(kvb_t *b, const char *key_norm, const char *value_json) {
  if (!key_norm || !value_json) return true;

  char *kesc = json_escape(key_norm);
  if (!kesc) return false;

  size_t need = strlen(kesc) + strlen(value_json) + 8;
  if (!kvb_ensure(b, need)) { free(kesc); return false; }

  if (!b->first) b->buf[b->len++] = ',';
  b->first = false;

  b->len += (size_t)snprintf(b->buf + b->len, b->cap - b->len, "\"%s\":%s", kesc, value_json);

  free(kesc);
  return true;
}
static bool should_skip_key(const char *raw_key) {
  if (!raw_key) return true;
  if (strncmp(raw_key, "_ws_", 4) == 0) return true;
  if (strcmp(raw_key, "data") == 0) return true;
  return false;
}
static bool parse_scalar_to_json_string_literal(const char **pp, char **out_json, bool *out_dyn) {
  const char *p = skip_ws(*pp);
  *out_dyn = false;
  *out_json = NULL;

  if (*p == '"') {
    const char *e = NULL;
    if (!find_end_of_string(p, &e)) return false;
    size_t slen = (size_t)(e - p + 1);
    char *s = (char *)malloc(slen + 1);
    if (!s) return false;
    memcpy(s, p, slen);
    s[slen] = '\0';
    *out_json = s;
    *out_dyn = true;
    *pp = e + 1;
    return true;
  }

  const char *t = p;
  while (*t && *t != ',' && *t != '}' && *t != ']' && *t != '\n' && *t != '\r') t++;
  size_t n = (size_t)(t - p);
  while (n && (p[n-1] == ' ' || p[n-1] == '\t')) n--;

  if (n == 4 && strncmp(p, "null", 4) == 0) { *out_json="\"\""; *pp=t; return true; }
  if (n == 4 && strncmp(p, "true", 4) == 0) { *out_json="\"True\""; *pp=t; return true; }
  if (n == 5 && strncmp(p, "false", 5) == 0) { *out_json="\"False\""; *pp=t; return true; }

  char *s = (char *)malloc(n + 3);
  if (!s) return false;
  s[0] = '"';
  memcpy(s + 1, p, n);
  s[1+n] = '"';
  s[2+n] = '\0';
  *out_json = s;
  *out_dyn = true;
  *pp = t;
  return true;
}

static bool flatten_value(const char *prefix, const char **pp, kvb_t *b);

static bool flatten_object(const char *prefix, const char **pp, kvb_t *b) {
  const char *p = skip_ws(*pp);
  if (*p != '{') return false;
  p++;

  while (1) {
    p = skip_ws(p);
    if (!*p) return false;
    if (*p == '}') { p++; break; }
    if (*p == ',') { p++; continue; }

    if (*p != '"') return false;
    const char *ke = NULL;
    if (!find_end_of_string(p, &ke)) return false;

    size_t klen = (size_t)(ke - (p + 1));
    char *kraw = (char *)malloc(klen + 1);
    if (!kraw) return false;
    memcpy(kraw, p + 1, klen);
    kraw[klen] = '\0';
    p = ke + 1;

    p = skip_ws(p);
    if (*p != ':') { free(kraw); return false; }
    p++;
    p = skip_ws(p);

    char *full = NULL;
    if (prefix && prefix[0]) {
      size_t need = strlen(prefix) + 1 + strlen(kraw) + 1;
      full = (char *)malloc(need);
      if (!full) { free(kraw); return false; }
      snprintf(full, need, "%s_%s", prefix, kraw);
    } else {
      full = strdup(kraw);
      if (!full) { free(kraw); return false; }
    }

    if (should_skip_key(kraw)) {
      (void)skip_json_value(&p);
      free(full);
      free(kraw);
      continue;
    }
    free(kraw);

    const char *valp = p;
    bool ok = flatten_value(full, &valp, b);

    (void)skip_json_value(&p);
    free(full);
    if (!ok) return false;
  }

  *pp = p;
  return true;
}

static bool flatten_array(const char *prefix, const char **pp, kvb_t *b) {
  const char *p = skip_ws(*pp);
  if (*p != '[') return false;
  p++;

  p = skip_ws(p);
  if (*p == ']') {
    char *kn = normalize_full_key(prefix);
    if (!kn) return false;
    bool ok = kvb_append(b, kn, "\"\"");
    free(kn);
    return ok;
  }

  if (*p == '{') {
    const char *tmp = p;
    return flatten_object(prefix, &tmp, b);
  }
  if (*p == '[') {
    const char *tmp = p;
    return flatten_array(prefix, &tmp, b);
  }

  char *vjson = NULL;
  bool vdyn = false;
  const char *tmp = p;
  if (!parse_scalar_to_json_string_literal(&tmp, &vjson, &vdyn)) return false;

  char *kn = normalize_full_key(prefix);
  if (!kn) { if (vdyn) free(vjson); return false; }

  bool ok = kvb_append(b, kn, vjson);

  free(kn);
  if (vdyn) free(vjson);
  return ok;
}

static bool flatten_value(const char *prefix, const char **pp, kvb_t *b) {
  const char *p = skip_ws(*pp);
  if (!*p) return false;

  if (*p == '{') {
    const char *tmp = p;
    return flatten_object(prefix, &tmp, b);
  }
  if (*p == '[') {
    const char *tmp = p;
    return flatten_array(prefix, &tmp, b);
  }

  char *vjson = NULL;
  bool vdyn = false;
  const char *tmp = p;
  if (!parse_scalar_to_json_string_literal(&tmp, &vjson, &vdyn)) return false;

  char *kn = normalize_full_key(prefix);
  if (!kn) { if (vdyn) free(vjson); return false; }

  bool ok = kvb_append(b, kn, vjson);

  free(kn);
  if (vdyn) free(vjson);
  return ok;
}

static char *build_cam_fields_from_layers(const char *layers_obj) {
  kvb_t b;
  if (!kvb_init(&b)) return NULL;

  const char *layer_names[] = { "its", "per", "cpm", "cam", NULL };
  for (int i = 0; layer_names[i]; i++) {
    char *obj = extract_layer_object_copy(layers_obj, layer_names[i]);
    if (!obj) continue;

    const char *p = obj;
    const char *prefix = (strcmp(layer_names[i], "its") == 0) ? "" : layer_names[i];

    bool ok = flatten_object(prefix, &p, &b);
    free(obj);

    if (!ok) {
      free(b.buf);
      return NULL;
    }
  }

  if (!kvb_ensure(&b, 2)) { free(b.buf); return NULL; }
  b.buf[b.len++] = '}';
  b.buf[b.len] = '\0';
  return b.buf;
}

static char *extract_stationtype_from_cam_fields_json(const char *cam_fields_json_obj) {
  if (!cam_fields_json_obj) return NULL;
  const char *pat = "\"stationtype\":";
  const char *p = strstr(cam_fields_json_obj, pat);
  if (!p) return NULL;
  p += strlen(pat);
  p = skip_ws(p);
  if (*p != '"') return NULL;
  const char *e = NULL;
  if (!find_end_of_string(p, &e)) return NULL;
  size_t n = (size_t)(e - (p + 1));
  char *out = (char *)malloc(n + 1);
  if (!out) return NULL;
  memcpy(out, p + 1, n);
  out[n] = '\0';
  return out;
}

static char *extract_frame_number_str(const char *layers_obj) {
  char *frame_obj = extract_layer_object_copy(layers_obj, "frame");
  if (!frame_obj) return NULL;

  const char *keys[] = { "frame_frame_number", "frame.number", "frame_number", NULL };
  char *val = NULL;
  for (int i = 0; keys[i]; i++) {
    val = extract_first_string_value(frame_obj, keys[i]);
    if (val && val[0]) break;
    free(val);
    val = NULL;
  }

  free(frame_obj);
  return val;
}

static char *extract_frame_raw_hex_norm(const char *layers_obj) {
  char *frame_obj = extract_layer_object_copy(layers_obj, "frame");
  const char *keys[] = { "frame_raw", "frame.raw", "frame_frame_raw", "frame_raw_hex", "frame_frame_raw_hex", NULL };

  char *raw0 = NULL;
  if (frame_obj) {
    for (int i = 0; keys[i]; i++) {
      raw0 = extract_first_string_value(frame_obj, keys[i]);
      if (raw0 && raw0[0]) break;
      free(raw0);
      raw0 = NULL;
    }
    free(frame_obj);
  }

  if (!raw0) {
    for (int i = 0; keys[i]; i++) {
      raw0 = extract_first_string_value(layers_obj, keys[i]);
      if (raw0 && raw0[0]) break;
      free(raw0);
      raw0 = NULL;
    }
  }

  if (!raw0) return NULL;

  char *norm = normalize_hex(raw0);
  if (!norm) norm = raw0;
  else free(raw0);
  return norm;
}

/* -------------------------
 * Record JSON builder
 * ------------------------- */
static char *build_record_json_from_parts(
    const char *timestamp_iso,
    const char *frame_number_str,
    const char *cam_fields_json_obj,
    const char *raw_hex_or_null,
    bool include_raw_hex
) {
  char *ts_esc = json_escape(timestamp_iso);
  char *fn_esc = json_escape(frame_number_str);
  if (!ts_esc || !fn_esc) { free(ts_esc); free(fn_esc); return NULL; }

  size_t out_sz = strlen(ts_esc) + strlen(fn_esc) + strlen(cam_fields_json_obj)
                + (include_raw_hex && raw_hex_or_null ? strlen(raw_hex_or_null) : 0)
                + 256;

  char *out = (char *)malloc(out_sz);
  if (!out) { free(ts_esc); free(fn_esc); return NULL; }

  if (include_raw_hex && raw_hex_or_null) {
    snprintf(out, out_sz,
      "{\"timestamp\":\"%s\",\"frame_number\":\"%s\",\"cam_layer\":\"its\",\"cam_fields\":%s,\"frame_raw_hex\":\"%s\"}",
      ts_esc, fn_esc, cam_fields_json_obj, raw_hex_or_null);
  } else {
    snprintf(out, out_sz,
      "{\"timestamp\":\"%s\",\"frame_number\":\"%s\",\"cam_layer\":\"its\",\"cam_fields\":%s}",
      ts_esc, fn_esc, cam_fields_json_obj);
  }

  free(ts_esc);
  free(fn_esc);
  return out;
}

/* -------------------------
 * CloudEvents builder (structured, ONLY optional stationtype extension)
 * ------------------------- */
static char *build_cloudevent_structured(
    const char *event_type,
    const char *source,
    const char *id_raw,
    const char *subject_or_null,
    const char *stationtype_or_null,
    const char *data_json_obj,
    size_t *out_len
) {
  const char *id = (id_raw && id_raw[0]) ? id_raw : "00000000-0000-4000-8000-000000000000";

  char tbuf[64];
  now_iso(tbuf);

  char *type_esc = json_escape(event_type);
  char *src_esc  = json_escape(source);
  char *id_esc   = json_escape(id);
  char *time_esc = json_escape(tbuf);

  char *subj_part = NULL;
  if (subject_or_null) {
    char *subj_esc = json_escape(subject_or_null);
    size_t sp_sz = strlen(subj_esc) + 32;
    subj_part = (char *)malloc(sp_sz);
    if (subj_part) snprintf(subj_part, sp_sz, ",\"subject\":\"%s\"", subj_esc);
    free(subj_esc);
  } else {
    subj_part = strdup("");
  }

  char *st_part = NULL;
  if (stationtype_or_null) {
    char *st_esc = json_escape(stationtype_or_null);
    size_t sp_sz = strlen(st_esc) + 40;
    st_part = (char *)malloc(sp_sz);
    if (st_part) snprintf(st_part, sp_sz, ",\"stationtype\":\"%s\"", st_esc);
    free(st_esc);
  } else {
    st_part = strdup("");
  }

  if (!type_esc || !src_esc || !id_esc || !time_esc || !subj_part || !st_part) {
    free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part); free(st_part);
    return NULL;
  }

  size_t body_sz =
    strlen(type_esc) + strlen(src_esc) + strlen(id_esc) + strlen(time_esc) +
    strlen(subj_part) + strlen(st_part) + strlen(data_json_obj) + 256;

  char *body = (char *)malloc(body_sz);
  if (!body) {
    free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part); free(st_part);
    return NULL;
  }

  snprintf(
    body, body_sz,
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
    type_esc, src_esc, id_esc, time_esc,
    subj_part, st_part,
    data_json_obj
  );

  *out_len = strlen(body);

  free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part); free(st_part);
  return body;
}

/* -------------------------
 * Knative PORT listener
 * ------------------------- */
static void *health_thread_fn(void *arg) {
  (void)arg;

  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0) return NULL;

  int one = 1;
  (void)setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)PORT);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(s); return NULL; }
  if (listen(s, 16) < 0) { close(s); return NULL; }

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
    tv.tv_sec = 1; tv.tv_usec = 0;

    int r = select(s + 1, &rfds, NULL, NULL, &tv);
    if (r <= 0) continue;

    int c = accept(s, NULL, NULL);
    if (c < 0) continue;

    char buf[512];
    (void)read(c, buf, sizeof(buf));
    (void)write(c, resp, sizeof(resp) - 1);
    close(c);
  }

  close(s);
  return NULL;
}

/* -------------------------
 * Bounded send queue (with bench metadata)
 * ------------------------- */
typedef struct {
  char *body;
  size_t body_len;

  char ce_id[37];
  uint64_t frame_no;
  int64_t t_capture_unix_ns;
  int64_t t_ce_built_unix_ns;
  int64_t t_enqueue_unix_ns;
} job_t;

typedef struct {
  job_t **items;
  int cap, head, tail, count;
  pthread_mutex_t mu;
  pthread_cond_t cv;
} jobq_t;

static jobq_t g_q;

static void jobq_init(jobq_t *q, int cap) {
  q->items = (job_t **)calloc((size_t)cap, sizeof(job_t *));
  q->cap = cap; q->head = 0; q->tail = 0; q->count = 0;
  pthread_mutex_init(&q->mu, NULL);
  pthread_cond_init(&q->cv, NULL);
}

static void jobq_destroy(jobq_t *q) {
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
  pthread_cond_destroy(&q->cv);
}

static bool jobq_try_push(jobq_t *q, job_t *job) {
  bool ok = false;
  pthread_mutex_lock(&q->mu);
  if (q->count < q->cap) {
    q->items[q->tail] = job;
    q->tail = (q->tail + 1) % q->cap;
    q->count++;
    ok = true;
    pthread_cond_signal(&q->cv);
  }
  pthread_mutex_unlock(&q->mu);
  return ok;
}

static job_t *jobq_pop(jobq_t *q) {
  pthread_mutex_lock(&q->mu);
  while (q->count == 0 && !g_sender_stop) {
    pthread_cond_wait(&q->cv, &q->mu);
  }
  job_t *job = NULL;
  if (q->count > 0) {
    job = q->items[q->head];
    q->items[q->head] = NULL;
    q->head = (q->head + 1) % q->cap;
    q->count--;
  }
  pthread_mutex_unlock(&q->mu);
  return job;
}

/* -------------------------
 * HTTP sender thread (CloudEvents structured + bench NDJSON)
 * ------------------------- */
static void *sender_thread_fn(void *arg) {
  (void)arg;

  CURL *curl = curl_easy_init();
  if (!curl) {
    log_line("[WARN] sender failed: curl_easy_init failed");
    return NULL;
  }

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/cloudevents+json");

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_URL, SINK_URL);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  curl_easy_setopt(curl, CURLOPT_PROXY, "");
  curl_easy_setopt(curl, CURLOPT_NOPROXY, "*");
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

  while (!g_sender_stop) {
    job_t *job = jobq_pop(&g_q);
    if (!job) continue;

    int64_t t_send_start_unix_ns = now_unix_ns();
    int64_t t0m = mono_ns();

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, job->body);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)job->body_len);

    CURLcode rc = curl_easy_perform(curl);

    int64_t t1m = mono_ns();
    int64_t t_send_end_unix_ns = now_unix_ns();

    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

    long long elapsed_ns = (long long)(t1m - t0m);

    log_line("sniffer POST elapsed_ns=%lld status=%ld", elapsed_ns, status);

    if (rc != CURLE_OK) log_line("[WARN] sender failed: %s", curl_easy_strerror(rc));
    else if (status >= 400) log_line("[WARN] sender failed: HTTP %ld", status);

    fprintf(stdout,
      "{\"kind\":\"bench\",\"component\":\"sniffer\",\"node\":\"%s\",\"ce_id\":\"%s\","
      "\"frame_no\":%" PRIu64 ","
      "\"t_capture_unix_ns\":%" PRId64 ","
      "\"t_ce_built_unix_ns\":%" PRId64 ","
      "\"t_enqueue_unix_ns\":%" PRId64 ","
      "\"t_send_start_unix_ns\":%" PRId64 ","
      "\"t_send_end_unix_ns\":%" PRId64 ","
      "\"send_elapsed_ns\":%lld,"
      "\"http_status\":%ld,"
      "\"curl_rc\":%d}\n",
      ENV_NODE[0] ? ENV_NODE : "unknown",
      job->ce_id,
      job->frame_no,
      job->t_capture_unix_ns,
      job->t_ce_built_unix_ns,
      job->t_enqueue_unix_ns,
      t_send_start_unix_ns,
      t_send_end_unix_ns,
      elapsed_ns,
      status,
      (int)rc
    );
    fflush(stdout);

    free(job->body);
    free(job);
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return NULL;
}

/* -------------------------
 * tshark process management
 * ------------------------- */
typedef struct {
  pid_t pid;
  FILE *out;
} tshark_proc_t;

static bool tshark_spawn(tshark_proc_t *tp) {
  if (!tp) return false;

  int pipefd[2];
  if (pipe(pipefd) != 0) {
    log_line("pipe() failed: %s", strerror(errno));
    return false;
  }

  pid_t pid = fork();
  if (pid < 0) {
    log_line("fork() failed: %s", strerror(errno));
    close(pipefd[0]); close(pipefd[1]);
    return false;
  }

  if (pid == 0) {
    dup2(pipefd[1], STDOUT_FILENO);
    close(pipefd[0]);
    close(pipefd[1]);

    char *argv[80];
    size_t ai = 0;
    argv[ai++] = (char *)TSHARK_BIN;
    argv[ai++] = "-l";
    argv[ai++] = "-n";
    argv[ai++] = "-i";
    argv[ai++] = (char *)IFACE;

    if (!PROMISCUOUS) argv[ai++] = "-p";

    argv[ai++] = "-s";
    argv[ai++] = "262144";

    if (BPF && BPF[0]) {
      argv[ai++] = "-f";
      argv[ai++] = (char *)BPF;
    }

    if (DISPLAY_FILTER && DISPLAY_FILTER[0]) {
      argv[ai++] = "-Y";
      argv[ai++] = (char *)DISPLAY_FILTER;
    }

    argv[ai++] = "-T";
    argv[ai++] = "ek";

    if (INCLUDE_RAW_HEX || CE_INCLUDE_RAW_HEX) argv[ai++] = "-x";

    argv[ai] = NULL;
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  FILE *fp = fdopen(pipefd[0], "r");
  if (!fp) {
    log_line("fdopen() failed: %s", strerror(errno));
    close(pipefd[0]);
    kill(pid, SIGTERM);
    return false;
  }

  tp->pid = pid;
  tp->out = fp;
  return true;
}

static void tshark_stop(tshark_proc_t *tp) {
  if (!tp) return;
  if (tp->pid > 0) kill(tp->pid, SIGTERM);
}

static void tshark_wait(tshark_proc_t *tp) {
  if (!tp) return;
  if (tp->out) { fclose(tp->out); tp->out = NULL; }
  if (tp->pid > 0) {
    int st = 0;
    (void)waitpid(tp->pid, &st, 0);
    tp->pid = -1;
  }
}

/* -------------------------
 * Env defaults
 * ------------------------- */
static void set_defaults_from_env(void) {
  IFACE = getenv("IFACE");
  if (!IFACE || !IFACE[0]) IFACE = "eth0";

  BPF = getenv("BPF");
  if (!BPF || !BPF[0]) {
    BPF = "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001";
  }

  DISPLAY_FILTER = getenv("DISPLAY_FILTER");
  if (DISPLAY_FILTER && DISPLAY_FILTER[0] == '\0') DISPLAY_FILTER = NULL;

  const char *le = getenv("LOG_EVERY");
  if (le && le[0]) LOG_EVERY = atoi(le);

  CE_TYPE = getenv("CE_TYPE");
  if (!CE_TYPE || !CE_TYPE[0]) CE_TYPE = "its.cam";

  INCLUDE_RAW_HEX = truthy(getenv("INCLUDE_RAW_HEX")) != NULL;

  const char *cer = getenv("CE_INCLUDE_RAW_HEX");
  if (cer == NULL) CE_INCLUDE_RAW_HEX = true;
  else CE_INCLUDE_RAW_HEX = truthy(cer) != NULL;

  SINK_URL = getenv("K_SINK");
  if (!SINK_URL) SINK_URL = "";

  const char *nd = getenv("STDOUT_NDJSON");
  if (nd && nd[0]) STDOUT_NDJSON = truthy(nd) != NULL;
  else STDOUT_NDJSON = true;

  PROMISCUOUS = truthy(getenv("PROMISCUOUS")) != NULL;

  const char *sqm = getenv("SEND_QUEUE_MAX");
  if (sqm && sqm[0]) SEND_QUEUE_MAX = atoi(sqm);
  if (SEND_QUEUE_MAX <= 0) SEND_QUEUE_MAX = 1000;

  const char *tb = getenv("TSHARK_BIN");
  if (tb && tb[0]) TSHARK_BIN = tb;

  const char *host =
    getenv("K8S_NODE_NAME") ? getenv("K8S_NODE_NAME") :
    getenv("NODE_NAME")     ? getenv("NODE_NAME")     :
    getenv("HOSTNAME")      ? getenv("HOSTNAME")      : "host";

  snprintf(ENV_NODE, sizeof(ENV_NODE), "%s", host);
  snprintf(CE_SOURCE, sizeof(CE_SOURCE), "sniffer://%s/%s", host, IFACE);

  const char *port = getenv("PORT");
  if (port && port[0]) {
    HAS_PORT = true;
    PORT = atoi(port);
    if (PORT <= 0 || PORT > 65535) PORT = 8080;
  }
}

/* -------------------------
 * Main
 * ------------------------- */
int main(void) {
  setvbuf(stdout, NULL, _IOLBF, 0);

  set_defaults_from_env();

  signal(SIGINT, on_sig);
  signal(SIGTERM, on_sig);
  signal(SIGPIPE, SIG_IGN);

  if (!has_exec_in_path(TSHARK_BIN)) {
    log_line("tshark is not installed in the image. Please ensure 'tshark' is present.");
  }

  log_line(">> LIVE capture iface='%s' promisc=%s", IFACE, PROMISCUOUS ? "on" : "off");
  log_line(">> BPF='%s'", BPF);
  if (DISPLAY_FILTER && DISPLAY_FILTER[0]) log_line(">> DISPLAY_FILTER='%s'", DISPLAY_FILTER);

  bool sink_on = (SINK_URL && SINK_URL[0]);
  log_line(">> CloudEvents sink: %s -> %s", sink_on ? "on" : "off", sink_on ? SINK_URL : "-");
  log_line(">> CE_INCLUDE_RAW_HEX=%s (CloudEvents include full frame hex)", CE_INCLUDE_RAW_HEX ? "true" : "false");
  log_line(">> SEND_QUEUE_MAX=%d", SEND_QUEUE_MAX);

  pthread_t health_th;
  bool health_started = false;
  if (HAS_PORT) {
    if (pthread_create(&health_th, NULL, health_thread_fn, NULL) == 0) health_started = true;
  }

  pthread_t sender_th;
  bool sender_started = false;

  jobq_init(&g_q, SEND_QUEUE_MAX);

  if (sink_on) {
    curl_global_init(CURL_GLOBAL_ALL);
    if (pthread_create(&sender_th, NULL, sender_thread_fn, NULL) == 0) sender_started = true;
    else log_line("[WARN] sender failed: failed to start sender thread");
  }

  tshark_proc_t tp = {0};
  if (!tshark_spawn(&tp)) g_stop = 1;

  uint64_t processed = 0;
  uint64_t seq_frame = 0;

  char *line = NULL;
  size_t cap = 0;

  while (!g_stop) {
    ssize_t r = getline(&line, &cap, tp.out);
    if (r < 0) break;

    while (r > 0 && (line[r-1] == '\n' || line[r-1] == '\r')) line[--r] = '\0';
    if (r == 0) continue;

    if (!strstr(line, "\"layers\"")) continue;

    const char *layers_start = NULL;
    size_t layers_len = 0;
    if (!extract_json_object_after_key(line, "layers", &layers_start, &layers_len)) continue;

    char *layers_copy = (char *)malloc(layers_len + 1);
    if (!layers_copy) continue;
    memcpy(layers_copy, layers_start, layers_len);
    layers_copy[layers_len] = '\0';

    char *its_obj = extract_layer_object_copy(layers_copy, "its");
    if (!its_obj) { free(layers_copy); continue; }
    free(its_obj);

    char *frame_no_str = extract_frame_number_str(layers_copy);
    if (!frame_no_str) {
      seq_frame++;
      char tmp[32];
      snprintf(tmp, sizeof(tmp), "%" PRIu64, seq_frame);
      frame_no_str = strdup(tmp);
    }

    uint64_t frame_no_u = 0;
    if (frame_no_str) {
      char *endp = NULL;
      unsigned long long v = strtoull(frame_no_str, &endp, 10);
      if (endp && *endp == '\0') frame_no_u = (uint64_t)v;
    }

    int64_t t_capture_unix_ns = now_unix_ns();

    char *cam_fields = build_cam_fields_from_layers(layers_copy);
    if (!cam_fields) { free(layers_copy); free(frame_no_str); continue; }

    char *stationtype = extract_stationtype_from_cam_fields_json(cam_fields);
    char *raw_hex_norm = extract_frame_raw_hex_norm(layers_copy);

    char rec_ts[64];
    now_iso(rec_ts);

    char *rec_out = build_record_json_from_parts(rec_ts, frame_no_str, cam_fields, raw_hex_norm, INCLUDE_RAW_HEX);
    if (!rec_out) {
      free(stationtype); free(raw_hex_norm); free(cam_fields); free(layers_copy); free(frame_no_str);
      continue;
    }

    if (STDOUT_NDJSON) {
      fputs(rec_out, stdout);
      fputc('\n', stdout);
      fflush(stdout);
    }

    char *rec_ce = rec_out;
    bool rec_ce_separate = false;
    if (sink_on && CE_INCLUDE_RAW_HEX && !INCLUDE_RAW_HEX) {
      rec_ce = build_record_json_from_parts(rec_ts, frame_no_str, cam_fields, raw_hex_norm, true);
      if (rec_ce) rec_ce_separate = true;
      else rec_ce = rec_out;
    }

    if (sink_on && sender_started) {
      char ce_id[37];
      if (!uuid4(ce_id)) snprintf(ce_id, sizeof(ce_id), "00000000-0000-4000-8000-000000000000");

      int64_t t_ce_built_unix_ns = now_unix_ns();

      size_t body_len = 0;
      char *ce = build_cloudevent_structured(CE_TYPE, CE_SOURCE, ce_id, frame_no_str, stationtype, rec_ce, &body_len);
      if (ce) {
        job_t *job = (job_t *)calloc(1, sizeof(job_t));
        if (!job) {
          free(ce);
        } else {
          job->body = ce;
          job->body_len = body_len;
          snprintf(job->ce_id, sizeof(job->ce_id), "%s", ce_id);
          job->frame_no = frame_no_u;
          job->t_capture_unix_ns = t_capture_unix_ns;
          job->t_ce_built_unix_ns = t_ce_built_unix_ns;
          job->t_enqueue_unix_ns = now_unix_ns();

          if (!jobq_try_push(&g_q, job)) {
            log_line("[WARN] SEND_QUEUE full, dropping event");
            free(job->body);
            free(job);
          }
        }
      }
    }

    processed++;
    if (LOG_EVERY > 0 && (processed % (uint64_t)LOG_EVERY) == 0) {
      char ts2[64];
      now_iso(ts2);
      log_line("[%s] processed %" PRIu64 " CAM packets (file=on, sink=%s)", ts2, processed, sink_on ? "on" : "off");
    }

    if (rec_ce_separate) free(rec_ce);
    free(rec_out);
    free(stationtype);
    free(raw_hex_norm);
    free(cam_fields);
    free(layers_copy);
    free(frame_no_str);
  }

  free(line);

  g_stop = 1;

  tshark_stop(&tp);
  tshark_wait(&tp);

  g_sender_stop = 1;
  pthread_mutex_lock(&g_q.mu);
  pthread_cond_broadcast(&g_q.cv);
  pthread_mutex_unlock(&g_q.mu);

  if (sink_on && sender_started) {
    pthread_join(sender_th, NULL);
    curl_global_cleanup();
  }

  if (health_started) pthread_join(health_th, NULL);

  jobq_destroy(&g_q);
  return 0;
}
