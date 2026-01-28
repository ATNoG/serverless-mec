// sniffer_libpcap.c — libpcap + libcurl CloudEvents structured sender
// Build (Alpine): gcc -O2 -Wall -Wextra -pthread sniffer_libpcap.c -lpcap -lcurl -o sniffer_libpcap
#define _GNU_SOURCE
#include <pcap/pcap.h>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h> /* strcasecmp */
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <curl/curl.h>

/* -------------------------
 * Config via environment
 * ------------------------- */
static const char *ENV_IFACE;
static const char *ENV_BPF;
static const char *ENV_DISPLAY_FILTER; /* logged only */
static int   ENV_LOG_EVERY = 10;
static const char *ENV_CE_TYPE;
static bool  ENV_INCLUDE_RAW_HEX = false;     /* stdout NDJSON raw hex */
static bool  ENV_CE_INCLUDE_RAW_HEX = true;   /* CloudEvents data raw hex (default ON) */
static const char *ENV_SINK_URL;              /* K_SINK */
static bool  ENV_STDOUT_NDJSON = true;
static bool  ENV_PROMISCUOUS = false;
static int   ENV_SEND_QUEUE_MAX = 1000;

static char CE_SOURCE[256] = {0};

/* -------------------------
 * Logging
 * ------------------------- */
static void logi(const char *fmt, ...) {
  va_list ap; va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
}
static void logw(const char *fmt, ...) {
  va_list ap; va_start(ap, fmt);
  fprintf(stderr, "[WARN] ");
  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
}
static void loge(const char *fmt, ...) {
  va_list ap; va_start(ap, fmt);
  fprintf(stderr, "[ERROR] ");
  vfprintf(stderr, fmt, ap);
  fprintf(stderr, "\n");
  va_end(ap);
}

/* -------------------------
 * Helpers
 * ------------------------- */
static volatile sig_atomic_t g_stop = 0;
static void on_sig(int sig) { (void)sig; g_stop = 1; }

static void iso8601_from_timeval(const struct timeval *tv, char *out, size_t out_sz) {
  /* UTC ISO8601 with microseconds: YYYY-MM-DDTHH:MM:SS.uuuuuuZ */
  struct tm tm;
  time_t sec = tv->tv_sec;
  gmtime_r(&sec, &tm);
  int n = snprintf(out, out_sz,
                   "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
                   tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                   tm.tm_hour, tm.tm_min, tm.tm_sec, (long)tv->tv_usec);
  if (n < 0 || (size_t)n >= out_sz) {
    snprintf(out, out_sz, "1970-01-01T00:00:00.000000Z");
  }
}

static void mac_to_str(const uint8_t mac[6], char *out, size_t out_sz) {
  snprintf(out, out_sz, "%02x:%02x:%02x:%02x:%02x:%02x",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

static char *hex_encode(const uint8_t *buf, size_t len) {
  static const char *hex = "0123456789abcdef";
  size_t out_len = len * 2;
  char *out = (char *)malloc(out_len + 1);
  if (!out) return NULL;
  for (size_t i = 0; i < len; i++) {
    out[i*2]     = hex[(buf[i] >> 4) & 0xF];
    out[i*2 + 1] = hex[buf[i] & 0xF];
  }
  out[out_len] = '\0';
  return out;
}

static char *json_escape(const char *s) {
  /* Minimal JSON escaping for strings */
  if (!s) s = "";
  size_t n = 0;
  for (const char *p = s; *p; p++) {
    switch (*p) {
      case '\"': case '\\': case '\b': case '\f': case '\n': case '\r': case '\t':
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
      case '\"': *o++='\\'; *o++='\"'; break;
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

static bool uuid4(char out[37]) {
  uint8_t b[16];
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) return false;
  ssize_t r = read(fd, b, sizeof(b));
  close(fd);
  if (r != (ssize_t)sizeof(b)) return false;

  /* RFC 4122 v4 */
  b[6] = (b[6] & 0x0F) | 0x40;
  b[8] = (b[8] & 0x3F) | 0x80;

  snprintf(out, 37,
           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
           b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
           b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
  return true;
}

/* -------------------------
 * Bounded send queue
 * ------------------------- */
typedef struct {
  char *body;
  size_t body_len;
} job_t;

typedef struct {
  job_t **items;
  int cap, head, tail, count;
  pthread_mutex_t mu;
  pthread_cond_t cv_not_empty;
} jobq_t;

static jobq_t g_q;

static void jobq_init(jobq_t *q, int cap) {
  q->items = (job_t **)calloc((size_t)cap, sizeof(job_t *));
  q->cap = cap; q->head = 0; q->tail = 0; q->count = 0;
  pthread_mutex_init(&q->mu, NULL);
  pthread_cond_init(&q->cv_not_empty, NULL);
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
  pthread_cond_destroy(&q->cv_not_empty);
}

static bool jobq_try_push(jobq_t *q, job_t *job) {
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

static job_t *jobq_pop_block(jobq_t *q) {
  pthread_mutex_lock(&q->mu);
  while (q->count == 0 && !g_stop) {
    pthread_cond_wait(&q->cv_not_empty, &q->mu);
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
 * HTTP sender (CloudEvents structured)
 * ------------------------- */
static const char *truthy(const char *s) {
  if (!s) return NULL;
  if (strcmp(s,"1")==0) return s;
  if (strcasecmp(s,"true")==0) return s;
  if (strcasecmp(s,"yes")==0) return s;
  return NULL;
}

static void *sender_thread_fn(void *arg) {
  (void)arg;

  CURL *curl = curl_easy_init();
  if (!curl) {
    loge("curl_easy_init failed");
    return NULL;
  }

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/cloudevents+json");

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_URL, ENV_SINK_URL);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

  /* Like requests.Session(trust_env=False): disable proxy env usage */
  curl_easy_setopt(curl, CURLOPT_PROXY, "");
  curl_easy_setopt(curl, CURLOPT_NOPROXY, "*");

  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);

  while (!g_stop) {
    job_t *job = jobq_pop_block(&g_q);
    if (!job) continue;

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, job->body);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)job->body_len);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    CURLcode rc = curl_easy_perform(curl);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);

    long long elapsed_ns =
      (long long)(t1.tv_sec - t0.tv_sec) * 1000000000LL +
      (long long)(t1.tv_nsec - t0.tv_nsec);

    if (rc != CURLE_OK) {
      logw("sniffer POST elapsed_ns=%lld status=%ld curl_err=%s",
           elapsed_ns, status, curl_easy_strerror(rc));
    } else {
      logi("sniffer POST elapsed_ns=%lld status=%ld", elapsed_ns, status);
    }

    free(job->body);
    free(job);
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return NULL;
}

/* -------------------------
 * Packet parsing (fast metadata fallback)
 * ------------------------- */
#pragma pack(push, 1)
typedef struct {
  uint8_t  dst[6];
  uint8_t  src[6];
  uint16_t ethertype;
} eth_hdr_t;

typedef struct {
  uint16_t tci;
  uint16_t ethertype;
} vlan_hdr_t;

typedef struct {
  uint8_t  ver_ihl;
  uint8_t  tos;
  uint16_t tot_len;
  uint16_t id;
  uint16_t frag_off;
  uint8_t  ttl;
  uint8_t  proto;
  uint16_t csum;
  uint32_t saddr;
  uint32_t daddr;
} ipv4_hdr_t;

typedef struct {
  uint16_t sport;
  uint16_t dport;
  uint16_t len;
  uint16_t csum;
} udp_hdr_t;
#pragma pack(pop)

/* -------------------------
 * ITS CAM decoder (minimal)
 * ------------------------- */

typedef struct {
  const uint8_t *buf;
  size_t len;
  size_t bitpos; /* 0..len*8 */
} bitr_t;

static uint32_t bits_needed_u32(uint32_t x) {
  /* bits to represent values 0..x */
  uint32_t bits = 0;
  while (x > 0) { bits++; x >>= 1; }
  return bits;
}

static bool br_read_bits_u64(bitr_t *br, uint32_t nbits, uint64_t *out) {
  if (!br || !out) return false;
  if (nbits > 64) return false;
  if (br->bitpos + nbits > br->len * 8) return false;

  uint64_t v = 0;
  for (uint32_t i = 0; i < nbits; i++) {
    size_t b = br->bitpos + i;
    uint8_t byte = br->buf[b / 8];
    uint32_t bit = 7u - (uint32_t)(b % 8);
    v = (v << 1) | ((byte >> bit) & 1u);
  }
  br->bitpos += nbits;
  *out = v;
  return true;
}

static bool br_read_bool(bitr_t *br, bool *out) {
  uint64_t v = 0;
  if (!br_read_bits_u64(br, 1, &v)) return false;
  if (out) *out = (v != 0);
  return true;
}

static bool br_skip_bits(bitr_t *br, size_t nbits) {
  if (br->bitpos + nbits > br->len * 8) return false;
  br->bitpos += nbits;
  return true;
}

static void br_align_to_byte(bitr_t *br) {
  size_t rem = br->bitpos % 8;
  if (rem) (void)br_skip_bits(br, 8 - rem);
}

static bool br_read_constrained_i32(bitr_t *br, int32_t minv, int32_t maxv, int32_t *out) {
  if (maxv < minv) return false;
  uint32_t range = (uint32_t)(maxv - minv);
  uint32_t bits = bits_needed_u32(range);
  if (bits == 0) { if (out) *out = minv; return true; }
  uint64_t raw = 0;
  if (!br_read_bits_u64(br, bits, &raw)) return false;
  if (raw > (uint64_t)range) return false;
  if (out) *out = (int32_t)raw + minv;
  return true;
}

static bool br_read_constrained_u32(bitr_t *br, uint32_t minv, uint32_t maxv, uint32_t *out) {
  if (maxv < minv) return false;
  uint32_t range = maxv - minv;
  uint32_t bits = bits_needed_u32(range);
  if (bits == 0) { if (out) *out = minv; return true; }
  uint64_t raw = 0;
  if (!br_read_bits_u64(br, bits, &raw)) return false;
  if (raw > (uint64_t)range) return false;
  if (out) *out = (uint32_t)raw + minv;
  return true;
}

/* UPER normally-small length (common short form only):
 *  0 + 6 bits => value (0..63)
 *  1 + (long form...) => not handled here
 */
static bool br_read_normally_small_len(bitr_t *br, uint32_t *out) {
  uint64_t flag = 0;
  if (!br_read_bits_u64(br, 1, &flag)) return false;
  if (flag != 0) return false; /* long form not supported */
  uint64_t v = 0;
  if (!br_read_bits_u64(br, 6, &v)) return false;
  if (out) *out = (uint32_t)v;
  return true;
}

/* PER length determinant (covers the common cases):
 *  0 + 7 bits => len (0..127)
 *  10 + 14 bits => len (0..16383)
 *  others not handled
 */
static bool br_read_length_det(bitr_t *br, uint32_t *out) {
  uint64_t b1 = 0;
  if (!br_read_bits_u64(br, 1, &b1)) return false;
  if (b1 == 0) {
    uint64_t v = 0;
    if (!br_read_bits_u64(br, 7, &v)) return false;
    if (out) *out = (uint32_t)v;
    return true;
  }
  uint64_t b2 = 0;
  if (!br_read_bits_u64(br, 1, &b2)) return false;
  if (b2 == 0) {
    uint64_t v = 0;
    if (!br_read_bits_u64(br, 14, &v)) return false;
    if (out) *out = (uint32_t)v;
    return true;
  }
  return false;
}

typedef struct {
  bool ok;

  /* header */
  uint8_t  protocol_version;
  uint8_t  message_id;
  uint32_t station_id;
  uint16_t generation_delta_time;

  /* CamParameters presence */
  bool camparams_extension_bit;
  bool camparams_lowfreq_present;
  bool camparams_special_present;

  /* BasicContainer */
  uint8_t station_type;

  /* ReferencePosition */
  bool refpos_extension_bit;
  int32_t latitude;
  int32_t longitude;

  /* PosConfidenceEllipse */
  uint16_t semi_major_axis_length;
  uint16_t semi_minor_axis_length;
  uint16_t semi_major_axis_orientation;

  /* Altitude */
  int32_t altitude_value;
  uint32_t altitude_confidence;

  /* HighFrequencyContainer */
  bool hf_ext_bit;
  uint8_t hf_choice;               /* 0 basicVehicleContainerHF */
  bool bvc_hf_ext_bit;             /* extension present for BVC HF */
  uint8_t bvc_hf_optmap;           /* 7 bits */

  /* BVC HF required */
  uint32_t heading_value;
  uint32_t heading_confidence;
  uint32_t speed_value;
  uint32_t speed_confidence;
  uint32_t drive_direction;        /* allow 0..3 */
  uint32_t vehicle_length_value;
  uint32_t vehicle_length_conf;
  uint32_t vehicle_width;
  int32_t longitudinal_acc_value;
  uint32_t longitudinal_acc_conf;
  bool curvature_ext_bit;
  int32_t curvature_value;
  uint32_t curvature_confidence;
  uint32_t curvature_calc_mode;    /* allow 0..3 */
  int32_t yaw_rate_value;
  uint32_t yaw_rate_confidence;    /* allow 0..15 */

  /* LowFrequencyContainer (optional) */
  bool lf_ext_bit;
  uint8_t lf_choice;               /* 0 basicVehicleContainerLF */
  bool bvc_lf_ext_bit;
  bool path_history_present;
  uint32_t vehicle_role;
  uint8_t exterior_lights;
  uint32_t path_history_len;       /* 0 if absent */
} cam_decoded_t;

/* Find SNAP AA:AA:03 00:00:00 89:47 and return payload after it. */
static bool extract_its_payload(const uint8_t *pkt, size_t pkt_len,
                               const uint8_t **out, size_t *out_len) {
  static const uint8_t snap[] = {0xAA,0xAA,0x03,0x00,0x00,0x00,0x89,0x47};
  if (!pkt || pkt_len < sizeof(snap)) return false;

  for (size_t i = 0; i + sizeof(snap) <= pkt_len; i++) {
    if (memcmp(pkt + i, snap, sizeof(snap)) == 0) {
      *out = pkt + i + sizeof(snap);
      *out_len = pkt_len - (i + sizeof(snap));
      return true;
    }
  }
  return false;
}

/* Prefer BTP destination port 2001 (0x07D1), but also fall back to scanning for 0x02 0x02 header. */
static bool find_cam_start(const uint8_t *its, size_t its_len, size_t *cam_off) {
  if (!its || its_len < 8) return false;

  /* 1) Look for 0x07 0xD1 and then try common offsets for payload start. */
  for (size_t i = 0; i + 8 <= its_len; i++) {
    if (its[i] == 0x07 && its[i+1] == 0xD1) {
      size_t candidates[] = { i + 4, i + 2, i + 6 };
      for (size_t c = 0; c < sizeof(candidates)/sizeof(candidates[0]); c++) {
        size_t off = candidates[c];
        if (off + 2 <= its_len && its[off] == 0x02 && its[off+1] == 0x02) {
          *cam_off = off;
          return true;
        }
      }
    }
  }

  /* 2) Fallback: scan for (protocolVersion=2, messageId=2). */
  for (size_t i = 0; i + 6 <= its_len; i++) {
    if (its[i] == 0x02 && its[i+1] == 0x02) {
      *cam_off = i;
      return true;
    }
  }

  return false;
}

static void cam_decoded_init(cam_decoded_t *d) {
  memset(d, 0, sizeof(*d));
  d->ok = false;
  d->path_history_len = 0;
}

static bool skip_bvc_hf_optional_fields(bitr_t *br, uint8_t optmap7) {
  /* optmap7: 7 bits, msb = first optional field */
  bool present[7] = {0};
  for (int i = 0; i < 7; i++) {
    present[i] = ((optmap7 >> (6 - i)) & 1u) != 0;
  }

  /* Order (ETSI CAM BVC HF):
   * 0 accelerationControl (BIT STRING SIZE(7))
   * 1 lanePosition (INTEGER(-1..14))
   * 2 steeringWheelAngle (INTEGER(-511..512))
   * 3 lateralAcceleration (value -160..161, confidence 0..102)  [approx skip]
   * 4 verticalAcceleration (value -160..161, confidence 0..102) [approx skip]
   * 5 performanceClass (INTEGER(0..7))
   * 6 cenDsrcTollingZone (approx skip: lat, lon, radius, id)
   */
  uint64_t tmp = 0;
  int32_t si = 0;
  uint32_t ui = 0;

  if (present[0]) { if (!br_read_bits_u64(br, 7, &tmp)) return false; }

  if (present[1]) { if (!br_read_constrained_i32(br, -1, 14, &si)) return false; }

  if (present[2]) { if (!br_read_constrained_i32(br, -511, 512, &si)) return false; }

  if (present[3]) {
    if (!br_read_constrained_i32(br, -160, 161, &si)) return false;
    if (!br_read_constrained_u32(br, 0, 102, &ui)) return false;
  }

  if (present[4]) {
    if (!br_read_constrained_i32(br, -160, 161, &si)) return false;
    if (!br_read_constrained_u32(br, 0, 102, &ui)) return false;
  }

  if (present[5]) { if (!br_read_constrained_u32(br, 0, 7, &ui)) return false; }

  if (present[6]) {
    /* Best-effort skip; this rarely appears in typical CAM traffic. */
    if (!br_read_constrained_i32(br, -900000000, 900000001, &si)) return false;
    if (!br_read_constrained_i32(br, -1800000000, 1800000001, &si)) return false;
    if (!br_read_constrained_u32(br, 0, 255, &ui)) return false;      /* radius (guess) */
    if (!br_read_constrained_u32(br, 0, 65535, &ui)) return false;    /* id (guess) */
  }

  return true;
}

static bool skip_sequence_extensions(bitr_t *br) {
  /* For a SEQUENCE with extension bit already read as 1:
   * - normally-small length N of extension bitmap (0..63, short form only)
   * - N presence bits
   * - for each present extension: open type (length determinant + value)
   *
   * We handle N==0 and the common short length determinants.
   */
  uint32_t n = 0;
  if (!br_read_normally_small_len(br, &n)) return false;

  /* read presence bitmap */
  bool any_present = false;
  for (uint32_t i = 0; i < n; i++) {
    bool b = false;
    if (!br_read_bool(br, &b)) return false;
    if (b) any_present = true;
  }

  if (!any_present) return true;

  /* If any are present, we try to skip open types in order.
   * We don't know which ones are present (we didn't store the bitmap), but
   * in practice this is rare for CAM; keeping this simple: bail if any present.
   *
   * If you *need* these, implement bitmap storage and open-type skipping per bit.
   */
  return false;
}

static bool decode_cam_uper_min(const uint8_t *cam_bytes, size_t cam_len, cam_decoded_t *out) {
  cam_decoded_t d;
  cam_decoded_init(&d);

  bitr_t br = { cam_bytes, cam_len, 0 };

  uint64_t u = 0;
  if (!br_read_bits_u64(&br, 8, &u)) return false; d.protocol_version = (uint8_t)u;
  if (!br_read_bits_u64(&br, 8, &u)) return false; d.message_id = (uint8_t)u;
  if (!br_read_bits_u64(&br, 32, &u)) return false; d.station_id = (uint32_t)u;
  if (!br_read_bits_u64(&br, 16, &u)) return false; d.generation_delta_time = (uint16_t)u;

  if (!br_read_bool(&br, &d.camparams_extension_bit)) return false;
  if (!br_read_bool(&br, &d.camparams_lowfreq_present)) return false;
  if (!br_read_bool(&br, &d.camparams_special_present)) return false;

  /* We only handle the common CAM with no special vehicle container. */
  if (d.camparams_special_present) return false;

  if (!br_read_bits_u64(&br, 8, &u)) return false; d.station_type = (uint8_t)u;

  /* ReferencePosition has an extension bit (this was the missing 1-bit in your logs). */
  if (!br_read_bool(&br, &d.refpos_extension_bit)) return false;

  int32_t si = 0;
  uint32_t ui = 0;

  if (!br_read_constrained_i32(&br, -900000000,  900000001, &si)) return false; d.latitude = si;
  if (!br_read_constrained_i32(&br, -1800000000, 1800000001, &si)) return false; d.longitude = si;

  if (!br_read_constrained_u32(&br, 0, 4095, &ui)) return false; d.semi_major_axis_length = (uint16_t)ui;
  if (!br_read_constrained_u32(&br, 0, 4095, &ui)) return false; d.semi_minor_axis_length = (uint16_t)ui;
  if (!br_read_constrained_u32(&br, 0, 3601, &ui)) return false; d.semi_major_axis_orientation = (uint16_t)ui;

  if (!br_read_constrained_i32(&br, -1000, 8001, &si)) return false; d.altitude_value = si;
  if (!br_read_constrained_u32(&br, 0, 15, &ui)) return false; d.altitude_confidence = ui;
  /* per_enum_index in your sample matches altitudeconfidence */
  /* HighFrequencyContainer (CHOICE with extension marker): ext bit + 1-bit index for root alternatives */
  if (!br_read_bool(&br, &d.hf_ext_bit)) return false;
  if (!br_read_bits_u64(&br, 1, &u)) return false; d.hf_choice = (uint8_t)u;

  /* We only handle basicVehicleContainerHighFrequency (choice 0). */
  if (d.hf_choice != 0) return false;

  /* BasicVehicleContainerHighFrequency: extension bit + 7 optional bits */
  if (!br_read_bool(&br, &d.bvc_hf_ext_bit)) return false;
  if (!br_read_bits_u64(&br, 7, &u)) return false; d.bvc_hf_optmap = (uint8_t)u;

  /* Required fields (ranges picked to keep decoding stable across common CAM variants) */
  if (!br_read_constrained_u32(&br, 0, 28801, &ui)) return false; d.heading_value = ui;
  if (!br_read_constrained_u32(&br, 0, 127, &ui)) return false; d.heading_confidence = ui;

  if (!br_read_constrained_u32(&br, 0, 16383, &ui)) return false; d.speed_value = ui;
  if (!br_read_constrained_u32(&br, 0, 127, &ui)) return false; d.speed_confidence = ui;

  if (!br_read_constrained_u32(&br, 0, 3, &ui)) return false; d.drive_direction = ui;

  if (!br_read_constrained_u32(&br, 0, 1023, &ui)) return false; d.vehicle_length_value = ui;
  if (!br_read_constrained_u32(&br, 0, 7, &ui)) return false; d.vehicle_length_conf = ui;

  if (!br_read_constrained_u32(&br, 0, 62, &ui)) return false; d.vehicle_width = ui;

  if (!br_read_constrained_i32(&br, -160, 161, &si)) return false; d.longitudinal_acc_value = si;
  if (!br_read_constrained_u32(&br, 0, 102, &ui)) return false; d.longitudinal_acc_conf = ui;

  /* Curvature: keep extension-present bit (exposed as per_extension_present_bit) */
  if (!br_read_bool(&br, &d.curvature_ext_bit)) return false;
  if (!br_read_constrained_i32(&br, -1023, 1023, &si)) return false; d.curvature_value = si;
  if (!br_read_constrained_u32(&br, 0, 7, &ui)) return false; d.curvature_confidence = ui;

  if (!br_read_constrained_u32(&br, 0, 3, &ui)) return false; d.curvature_calc_mode = ui;

  if (!br_read_constrained_i32(&br, -32766, 32767, &si)) return false; d.yaw_rate_value = si;
  if (!br_read_constrained_u32(&br, 0, 15, &ui)) return false; d.yaw_rate_confidence = ui;

  /* Optional fields (skip values so we land correctly for LowFrequencyContainer). */
  if (!skip_bvc_hf_optional_fields(&br, d.bvc_hf_optmap)) return false;

  /* Extension additions for BVC HF: if ext bit is set, try to skip.
   * Common case: bitmap length = 0 (like your frames).
   * If any present, we currently bail.
   */
  if (d.bvc_hf_ext_bit) {
    if (!skip_sequence_extensions(&br)) return false;
  }

  /* LowFrequencyContainer is present only if camparams_lowfreq_present bit is set */
  if (d.camparams_lowfreq_present) {
    if (!br_read_bool(&br, &d.lf_ext_bit)) return false;
    if (!br_read_bits_u64(&br, 1, &u)) return false; d.lf_choice = (uint8_t)u;
    if (d.lf_choice != 0) return false; /* only basicVehicleContainerLowFrequency */

    if (!br_read_bool(&br, &d.bvc_lf_ext_bit)) return false;
    if (!br_read_bool(&br, &d.path_history_present)) return false;

    if (!br_read_constrained_u32(&br, 0, 15, &ui)) return false; d.vehicle_role = ui;

    if (!br_read_bits_u64(&br, 8, &u)) return false; d.exterior_lights = (uint8_t)u;

    if (d.path_history_present) {
      /* PathHistory is a SEQUENCE OF; for your desired output we only need length */
      if (!br_read_constrained_u32(&br, 0, 40, &ui)) return false;
      d.path_history_len = ui;
      /* Not decoding entries; just skip none (entries decoding omitted). */
    } else {
      d.path_history_len = 0;
    }
  }

  d.ok = true;
  if (out) *out = d;
  return true;
}

static const char *TF(bool b) { return b ? "True" : "False"; }

/* Build cam_fields JSON for the decoded event (all values as strings like your sample). */
static char *build_cam_fields_json_from_decoded(const cam_decoded_t *d) {
  if (!d || !d->ok) return NULL;

  char exterior_hex[3];
  snprintf(exterior_hex, sizeof(exterior_hex), "%02x", (unsigned)d->exterior_lights);

  /* individual exterior light bits (spec mapping is tool-dependent; this matches your sample naming) */
  bool lowbeam  = (d->exterior_lights & (1u<<0)) != 0;
  bool highbeam = (d->exterior_lights & (1u<<1)) != 0;
  bool left     = (d->exterior_lights & (1u<<2)) != 0;
  bool right    = (d->exterior_lights & (1u<<3)) != 0;
  bool drl      = (d->exterior_lights & (1u<<4)) != 0;
  bool reverse  = (d->exterior_lights & (1u<<5)) != 0;
  bool fog      = (d->exterior_lights & (1u<<6)) != 0;
  bool parking  = (d->exterior_lights & (1u<<7)) != 0;

  /* cam_highfrequencycontainer and cam_lowfrequencycontainer are CHOICE indexes in your sample */
  char hf_choice_s[8]; snprintf(hf_choice_s, sizeof(hf_choice_s), "%u", (unsigned)d->hf_choice);
  char lf_choice_s[8]; snprintf(lf_choice_s, sizeof(lf_choice_s), "%u", (unsigned)d->lf_choice);

  /* per_enum_index / per_choice_index etc are debug-ish in your sample */
  char per_enum_index[16]; snprintf(per_enum_index, sizeof(per_enum_index), "%u", (unsigned)d->altitude_confidence);
  char per_choice_index[16]; snprintf(per_choice_index, sizeof(per_choice_index), "%u", (unsigned)d->hf_choice);

  /* per_sequence_of_length + cam_pathhistory in your sample */
  char ph_len_s[16]; snprintf(ph_len_s, sizeof(ph_len_s), "%u", (unsigned)d->path_history_len);

  size_t buf_sz = 4096;
  char *buf = (char *)malloc(buf_sz);
  if (!buf) return NULL;

  /* Keep keys aligned with your example as much as possible. */
  int n = snprintf(
    buf, buf_sz,
    "{"
      "\"itspduheader_element\":\"\","
      "\"protocolversion\":\"%u\","
      "\"messageid\":\"%u\","
      "\"stationid\":\"%u\","
      "\"cam_campayload_element\":\"\","
      "\"cam_generationdeltatime\":\"%u\","
      "\"cam_camparameters_element\":\"\","
      "\"per_extension_bit\":\"%s\","
      "\"per_optional_field_bit\":\"%s\","
      "\"cam_basiccontainer_element\":\"\","
      "\"stationtype\":\"%u\","
      "\"referenceposition_element\":\"\","
      "\"latitude\":\"%d\","
      "\"longitude\":\"%d\","
      "\"positionconfidenceellipse_element\":\"\","
      "\"semimajoraxislength\":\"%u\","
      "\"semiminoraxislength\":\"%u\","
      "\"semimajoraxisorientation\":\"%u\","
      "\"altitude_element\":\"\","
      "\"altitudevalue\":\"%d\","
      "\"per_enum_index\":\"%s\","
      "\"altitudeconfidence\":\"%u\","
      "\"per_choice_index\":\"%s\","
      "\"cam_highfrequencycontainer\":\"%s\","
      "\"cam_basicvehiclecontainerhighfrequency_element\":\"\","
      "\"cam_heading_element\":\"\","
      "\"headingvalue\":\"%u\","
      "\"headingconfidence\":\"%u\","
      "\"cam_speed_element\":\"\","
      "\"speedvalue\":\"%u\","
      "\"speedconfidence\":\"%u\","
      "\"cam_drivedirection\":\"%u\","
      "\"cam_vehiclelength_element\":\"\","
      "\"vehiclelengthvalue\":\"%u\","
      "\"vehiclelengthconfidenceindication\":\"%u\","
      "\"cam_vehiclewidth\":\"%u\","
      "\"cam_longitudinalacceleration_element\":\"\","
      "\"value\":\"%d\","
      "\"confidence\":\"%u\","
      "\"cam_curvature_element\":\"\","
      "\"curvaturevalue\":\"%d\","
      "\"curvatureconfidence\":\"%u\","
      "\"per_extension_present_bit\":\"%s\","
      "\"cam_curvaturecalculationmode\":\"%u\","
      "\"cam_yawrate_element\":\"\","
      "\"yawratevalue\":\"%d\","
      "\"yawrateconfidence\":\"%u\","
      "\"cam_lowfrequencycontainer\":\"%s\","
      "\"cam_basicvehiclecontainerlowfrequency_element\":\"\","
      "\"cam_vehiclerole\":\"%u\","
      "\"cam_exteriorlights\":\"%s\","
      "\"exteriorlights_lowbeamheadlightson\":\"%s\","
      "\"exteriorlights_highbeamheadlightson\":\"%s\","
      "\"exteriorlights_leftturnsignalon\":\"%s\","
      "\"exteriorlights_rightturnsignalon\":\"%s\","
      "\"exteriorlights_daytimerunninglightson\":\"%s\","
      "\"exteriorlights_reverselighton\":\"%s\","
      "\"exteriorlights_foglighton\":\"%s\","
      "\"exteriorlights_parkinglightson\":\"%s\","
      "\"per_sequence_of_length\":\"%s\","
      "\"cam_pathhistory\":\"%s\""
    "}",
    (unsigned)d->protocol_version,
    (unsigned)d->message_id,
    (unsigned)d->station_id,
    (unsigned)d->generation_delta_time,
    TF(d->camparams_extension_bit),
    TF(d->camparams_lowfreq_present),
    (unsigned)d->station_type,
    d->latitude,
    d->longitude,
    (unsigned)d->semi_major_axis_length,
    (unsigned)d->semi_minor_axis_length,
    (unsigned)d->semi_major_axis_orientation,
    (int)d->altitude_value,
    per_enum_index,
    (unsigned)d->altitude_confidence,
    per_choice_index,
    hf_choice_s,
    (unsigned)d->heading_value,
    (unsigned)d->heading_confidence,
    (unsigned)d->speed_value,
    (unsigned)d->speed_confidence,
    (unsigned)d->drive_direction,
    (unsigned)d->vehicle_length_value,
    (unsigned)d->vehicle_length_conf,
    (unsigned)d->vehicle_width,
    (int)d->longitudinal_acc_value,
    (unsigned)d->longitudinal_acc_conf,
    (int)d->curvature_value,
    (unsigned)d->curvature_confidence,
    TF(d->curvature_ext_bit),
    (unsigned)d->curvature_calc_mode,
    (int)d->yaw_rate_value,
    (unsigned)d->yaw_rate_confidence,
    lf_choice_s,
    (unsigned)d->vehicle_role,
    exterior_hex,
    TF(lowbeam),
    TF(highbeam),
    TF(left),
    TF(right),
    TF(drl),
    TF(reverse),
    TF(fog),
    TF(parking),
    ph_len_s,
    ph_len_s
  );

  if (n < 0 || (size_t)n >= buf_sz) {
    free(buf);
    return NULL;
  }
  return buf;
}

/* -------------------------
 * JSON record builder (decoded CAM preferred)
 * ------------------------- */
static char *build_record_json(
    uint64_t frame_no,
    const struct pcap_pkthdr *h,
    const uint8_t *pkt,
    size_t pkt_len,
    bool include_raw_hex,
    cam_decoded_t *decoded_out /* optional, filled if decoded */
) {
  char ts[64];
  iso8601_from_timeval(&h->ts, ts, sizeof(ts));

  /* Always compute raw hex if requested */
  char *raw_hex = NULL;
  if (include_raw_hex) raw_hex = hex_encode(pkt, pkt_len);

  /* Try decode CAM */
  cam_decoded_t d;
  cam_decoded_init(&d);
  bool decoded_ok = false;

  const uint8_t *its = NULL;
  size_t its_len = 0;
  size_t cam_off = 0;

  if (extract_its_payload(pkt, pkt_len, &its, &its_len) &&
      find_cam_start(its, its_len, &cam_off)) {
    const uint8_t *cam = its + cam_off;
    size_t cam_len = its_len - cam_off;
    decoded_ok = decode_cam_uper_min(cam, cam_len, &d);
  }

  if (decoded_ok) {
    /* Build decoded record (your desired shape). */
    char *ts_esc = json_escape(ts);
    if (!ts_esc) { free(raw_hex); return NULL; }

    char *cam_fields = build_cam_fields_json_from_decoded(&d);
    if (!cam_fields) { free(ts_esc); free(raw_hex); return NULL; }

    /* frame_number as string to match your example */
    char frame_s[32];
    snprintf(frame_s, sizeof(frame_s), "%" PRIu64, frame_no);

    size_t buf_sz = 2048 + strlen(ts_esc) + strlen(cam_fields) + (raw_hex ? (strlen(raw_hex) + 64) : 0);
    char *buf = (char *)malloc(buf_sz);
    if (!buf) {
      free(ts_esc); free(cam_fields); free(raw_hex);
      return NULL;
    }

    int n = snprintf(
      buf, buf_sz,
      "{"
        "\"timestamp\":\"%s\","
        "\"frame_number\":\"%s\","
        "\"cam_layer\":\"its\","
        "\"cam_fields\":%s"
        "%s%s%s"
      "}",
      ts_esc,
      frame_s,
      cam_fields,
      raw_hex ? ",\"frame_raw_hex\":\"" : "",
      raw_hex ? raw_hex : "",
      raw_hex ? "\"" : ""
    );

    free(ts_esc);
    free(cam_fields);
    free(raw_hex);

    if (n < 0 || (size_t)n >= buf_sz) {
      free(buf);
      return NULL;
    }

    if (decoded_out) *decoded_out = d;
    return buf;
  }

  /* Fallback: old metadata record */
  char srcmac[32] = {0}, dstmac[32] = {0};
  uint16_t ethertype = 0;
  int vlan_id = -1;

  char srcip[64] = {0}, dstip[64] = {0};
  int srcport = -1, dstport = -1;
  bool is_udp = false;
  bool is_ipv4 = false;
  bool is_etsi_its = false;

  size_t off = 0;
  if (pkt_len >= sizeof(eth_hdr_t)) {
    const eth_hdr_t *eth = (const eth_hdr_t *)(pkt);
    mac_to_str(eth->src, srcmac, sizeof(srcmac));
    mac_to_str(eth->dst, dstmac, sizeof(dstmac));
    ethertype = ntohs(eth->ethertype);
    off = sizeof(eth_hdr_t);

    if ((ethertype == 0x8100 || ethertype == 0x88a8) && pkt_len >= off + sizeof(vlan_hdr_t)) {
      const vlan_hdr_t *v = (const vlan_hdr_t *)(pkt + off);
      uint16_t tci = ntohs(v->tci);
      vlan_id = (int)(tci & 0x0FFF);
      ethertype = ntohs(v->ethertype);
      off += sizeof(vlan_hdr_t);
    }

    if (ethertype == 0x8947) is_etsi_its = true;

    if (ethertype == 0x0800 && pkt_len >= off + sizeof(ipv4_hdr_t)) {
      const ipv4_hdr_t *ip = (const ipv4_hdr_t *)(pkt + off);
      uint8_t ver = (ip->ver_ihl >> 4) & 0xF;
      uint8_t ihl = (ip->ver_ihl & 0xF) * 4;
      if (ver == 4 && pkt_len >= off + ihl) {
        is_ipv4 = true;
        struct in_addr a;
        a.s_addr = ip->saddr; inet_ntop(AF_INET, &a, srcip, sizeof(srcip));
        a.s_addr = ip->daddr; inet_ntop(AF_INET, &a, dstip, sizeof(dstip));

        if (ip->proto == 17 && pkt_len >= off + ihl + sizeof(udp_hdr_t)) {
          const udp_hdr_t *udp = (const udp_hdr_t *)(pkt + off + ihl);
          srcport = (int)ntohs(udp->sport);
          dstport = (int)ntohs(udp->dport);
          is_udp = true;
        }
      }
    }
  }

  char *ts_esc = json_escape(ts);
  char *srcmac_esc = json_escape(srcmac[0] ? srcmac : "");
  char *dstmac_esc = json_escape(dstmac[0] ? dstmac : "");
  char *srcip_esc  = json_escape(srcip[0] ? srcip : "");
  char *dstip_esc  = json_escape(dstip[0] ? dstip : "");

  if (!ts_esc || !srcmac_esc || !dstmac_esc || !srcip_esc || !dstip_esc) {
    free(ts_esc); free(srcmac_esc); free(dstmac_esc); free(srcip_esc); free(dstip_esc);
    free(raw_hex);
    return NULL;
  }

  char vlan_part[64];
  if (vlan_id >= 0) snprintf(vlan_part, sizeof(vlan_part), "%d", vlan_id);
  else snprintf(vlan_part, sizeof(vlan_part), "null");

  char srcip_part[128];
  if (srcip[0]) snprintf(srcip_part, sizeof(srcip_part), "\"%s\"", srcip_esc);
  else snprintf(srcip_part, sizeof(srcip_part), "null");

  char dstip_part[128];
  if (dstip[0]) snprintf(dstip_part, sizeof(dstip_part), "\"%s\"", dstip_esc);
  else snprintf(dstip_part, sizeof(dstip_part), "null");

  char srcport_part[64];
  if (srcport >= 0) snprintf(srcport_part, sizeof(srcport_part), "%d", srcport);
  else snprintf(srcport_part, sizeof(srcport_part), "null");

  char dstport_part[64];
  if (dstport >= 0) snprintf(dstport_part, sizeof(dstport_part), "%d", dstport);
  else snprintf(dstport_part, sizeof(dstport_part), "null");

  size_t buf_sz = 2048
    + strlen(ts_esc) + strlen(srcmac_esc) + strlen(dstmac_esc)
    + strlen(srcip_esc) + strlen(dstip_esc)
    + (raw_hex ? (strlen(raw_hex) + 64) : 0);

  char *buf = (char *)malloc(buf_sz);
  if (!buf) {
    free(ts_esc); free(srcmac_esc); free(dstmac_esc); free(srcip_esc); free(dstip_esc);
    free(raw_hex);
    return NULL;
  }

  int n = snprintf(
    buf, buf_sz,
    "{"
      "\"timestamp\":\"%s\","
      "\"frame_number\":%" PRIu64 ","
      "\"cam_layer\":\"its\","
      "\"pcap_len\":%u,"
      "\"pcap_caplen\":%u,"
      "\"cam_fields\":{"
        "\"src_mac\":\"%s\","
        "\"dst_mac\":\"%s\","
        "\"ethertype\":\"0x%04x\","
        "\"vlan_id\":%s,"
        "\"is_etsi_its_ethertype\":%s,"
        "\"is_ipv4\":%s,"
        "\"is_udp\":%s,"
        "\"src_ip\":%s,"
        "\"dst_ip\":%s,"
        "\"src_port\":%s,"
        "\"dst_port\":%s"
      "}%s%s%s"
    "}",
    ts_esc,
    frame_no,
    h->len,
    h->caplen,
    srcmac_esc,
    dstmac_esc,
    ethertype,
    vlan_part,
    is_etsi_its ? "true" : "false",
    is_ipv4 ? "true" : "false",
    is_udp ? "true" : "false",
    srcip_part,
    dstip_part,
    srcport_part,
    dstport_part,
    raw_hex ? ",\"frame_raw_hex\":\"" : "",
    raw_hex ? raw_hex : "",
    raw_hex ? "\"" : ""
  );

  if (n < 0 || (size_t)n >= buf_sz) {
    free(buf);
    buf = NULL;
  }

  free(ts_esc); free(srcmac_esc); free(dstmac_esc); free(srcip_esc); free(dstip_esc);
  free(raw_hex);
  return buf;
}

static char *build_cloudevent_structured(
    const char *event_type,
    const char *source,
    const char *subject_or_null,
    const char *data_json_obj,   /* must be JSON object string */
    size_t *out_len,
    int stationtype_or_neg       /* >=0 => include as CE extension */
) {
  char id[37];
  if (!uuid4(id)) snprintf(id, sizeof(id), "00000000-0000-4000-8000-000000000000");

  struct timeval tv;
  gettimeofday(&tv, NULL);
  char tbuf[64];
  iso8601_from_timeval(&tv, tbuf, sizeof(tbuf));

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

  char station_part[64];
  station_part[0] = '\0';
  if (stationtype_or_neg >= 0) {
    snprintf(station_part, sizeof(station_part), ",\"stationtype\":%d", stationtype_or_neg);
  }

  if (!type_esc || !src_esc || !id_esc || !time_esc || !subj_part) {
    free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part);
    return NULL;
  }

  size_t body_sz =
    strlen(type_esc) + strlen(src_esc) + strlen(id_esc) + strlen(time_esc) +
    strlen(subj_part) + strlen(station_part) + strlen(data_json_obj) + 256;

  char *body = (char *)malloc(body_sz);
  if (!body) {
    free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part);
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
    type_esc, src_esc, id_esc, time_esc, subj_part, station_part, data_json_obj
  );

  *out_len = strlen(body);

  free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part);
  return body;
}

/* -------------------------
 * libpcap capture
 * ------------------------- */
static uint64_t g_frame_no = 0;
static uint64_t g_processed = 0;

static void on_packet(u_char *user, const struct pcap_pkthdr *h, const u_char *bytes) {
  (void)user;
  if (g_stop) return;

  g_frame_no++;

  cam_decoded_t decoded;
  cam_decoded_init(&decoded);

  /* Build stdout record (may or may not include raw hex) */
  char *rec_out = build_record_json(g_frame_no, h, bytes, h->caplen, ENV_INCLUDE_RAW_HEX, &decoded);
  if (!rec_out) return;

  if (ENV_STDOUT_NDJSON) {
    fputs(rec_out, stdout);
    fputc('\n', stdout);
  }

  bool sink_on = (ENV_SINK_URL && ENV_SINK_URL[0]);

  /* Build CloudEvents record (ensure raw hex included if requested) */
  char *rec_ce = rec_out;
  bool rec_ce_is_separate = false;

  if (sink_on && ENV_CE_INCLUDE_RAW_HEX && !ENV_INCLUDE_RAW_HEX) {
    cam_decoded_t decoded2;
    cam_decoded_init(&decoded2);
    rec_ce = build_record_json(g_frame_no, h, bytes, h->caplen, true, &decoded2);
    if (!rec_ce) {
      rec_ce = rec_out;
    } else {
      rec_ce_is_separate = true;
      decoded = decoded2; /* prefer decoded from CE build (it had raw hex anyway) */
    }
  }

  if (sink_on) {
    char subj[32];
    snprintf(subj, sizeof(subj), "%" PRIu64, g_frame_no);

    int stationtype_ext = -1;
    if (decoded.ok) stationtype_ext = (int)decoded.station_type;

    size_t body_len = 0;
    char *ce = build_cloudevent_structured(ENV_CE_TYPE, CE_SOURCE, subj, rec_ce, &body_len, stationtype_ext);
    if (ce) {
      job_t *job = (job_t *)calloc(1, sizeof(job_t));
      if (!job) {
        free(ce);
      } else {
        job->body = ce;
        job->body_len = body_len;
        if (!jobq_try_push(&g_q, job)) {
          logw("SEND_QUEUE full, dropping event");
          free(job->body);
          free(job);
        }
      }
    }
  }

  if (rec_ce_is_separate) free(rec_ce);
  free(rec_out);

  g_processed++;
  if (ENV_LOG_EVERY > 0 && (g_processed % (uint64_t)ENV_LOG_EVERY) == 0) {
    logi("[pcap] processed %" PRIu64 " packets (sink=%s)", g_processed,
         (ENV_SINK_URL && ENV_SINK_URL[0]) ? "on" : "off");
  }
}

static void set_defaults_from_env(void) {
  ENV_IFACE = getenv("IFACE");
  if (!ENV_IFACE || !ENV_IFACE[0]) ENV_IFACE = "eth0";

  ENV_BPF = getenv("BPF");
  if (!ENV_BPF || !ENV_BPF[0]) {
    /* Note: if your interface is monitor/cooked capture, ether/vlan filters may not match.
       You can also filter later; decoding uses SNAP scan anyway. */
    ENV_BPF = "(ether proto 0x8947 or (vlan and ether[16:2]==0x8947)) or udp port 2001";
  }

  ENV_DISPLAY_FILTER = getenv("DISPLAY_FILTER");

  const char *le = getenv("LOG_EVERY");
  if (le && le[0]) ENV_LOG_EVERY = atoi(le);

  ENV_CE_TYPE = getenv("CE_TYPE");
  if (!ENV_CE_TYPE || !ENV_CE_TYPE[0]) ENV_CE_TYPE = "its.cam";

  ENV_INCLUDE_RAW_HEX = truthy(getenv("INCLUDE_RAW_HEX")) != NULL;

  /* CloudEvents raw frame hex (default ON). If set (even to empty), it is honored. */
  const char *cer = getenv("CE_INCLUDE_RAW_HEX");
  if (cer == NULL) {
    ENV_CE_INCLUDE_RAW_HEX = true;
  } else {
    ENV_CE_INCLUDE_RAW_HEX = truthy(cer) != NULL;
  }

  ENV_SINK_URL = getenv("K_SINK");
  if (!ENV_SINK_URL) ENV_SINK_URL = "";

  const char *nd = getenv("STDOUT_NDJSON");
  if (nd && nd[0]) {
    ENV_STDOUT_NDJSON = truthy(nd) != NULL;
  }

  ENV_PROMISCUOUS = truthy(getenv("PROMISCUOUS")) != NULL;

  const char *sqm = getenv("SEND_QUEUE_MAX");
  if (sqm && sqm[0]) ENV_SEND_QUEUE_MAX = atoi(sqm);
  if (ENV_SEND_QUEUE_MAX <= 0) ENV_SEND_QUEUE_MAX = 1000;

  const char *host =
    getenv("K8S_NODE_NAME") ? getenv("K8S_NODE_NAME") :
    getenv("NODE_NAME")     ? getenv("NODE_NAME")     :
    getenv("HOSTNAME")      ? getenv("HOSTNAME")      : "host";

  snprintf(CE_SOURCE, sizeof(CE_SOURCE), "sniffer://%s/%s", host, ENV_IFACE);
}

int main(void) {
  setvbuf(stdout, NULL, _IOLBF, 0);

  set_defaults_from_env();

  signal(SIGINT, on_sig);
  signal(SIGTERM, on_sig);

  logi(">> LIVE capture iface='%s' promisc=%s", ENV_IFACE, ENV_PROMISCUOUS ? "on" : "off");
  logi(">> BPF='%s'", ENV_BPF);
  if (ENV_DISPLAY_FILTER && ENV_DISPLAY_FILTER[0]) {
    logi(">> DISPLAY_FILTER='%s' (ignored in libpcap version)", ENV_DISPLAY_FILTER);
  }
  logi(">> CloudEvents sink: %s -> %s",
       (ENV_SINK_URL && ENV_SINK_URL[0]) ? "on" : "off",
       (ENV_SINK_URL && ENV_SINK_URL[0]) ? ENV_SINK_URL : "-");
  logi(">> CE_INCLUDE_RAW_HEX=%s (CloudEvents include full frame hex)", ENV_CE_INCLUDE_RAW_HEX ? "true" : "false");
  logi(">> SEND_QUEUE_MAX=%d", ENV_SEND_QUEUE_MAX);

  bool sink_on = (ENV_SINK_URL && ENV_SINK_URL[0]);
  pthread_t sender_th;

  jobq_init(&g_q, ENV_SEND_QUEUE_MAX);

  if (sink_on) {
    curl_global_init(CURL_GLOBAL_ALL);
    if (pthread_create(&sender_th, NULL, sender_thread_fn, NULL) != 0) {
      loge("failed to start sender thread (disabling sink)");
      sink_on = false;
    }
  }

  char errbuf[PCAP_ERRBUF_SIZE] = {0};
  pcap_t *pc = pcap_create(ENV_IFACE, errbuf);
  if (!pc) {
    loge("pcap_create failed: %s", errbuf);
    g_stop = 1;
    goto shutdown;
  }

  pcap_set_snaplen(pc, 262144);
  pcap_set_promisc(pc, ENV_PROMISCUOUS ? 1 : 0);
  pcap_set_timeout(pc, 1000);

  int rc = pcap_activate(pc);
  if (rc < 0) {
    loge("pcap_activate failed: %s", pcap_geterr(pc));
    pcap_close(pc);
    g_stop = 1;
    goto shutdown;
  }

  struct bpf_program fp;
  if (pcap_compile(pc, &fp, ENV_BPF, 1, PCAP_NETMASK_UNKNOWN) < 0) {
    loge("pcap_compile failed: %s", pcap_geterr(pc));
    pcap_close(pc);
    g_stop = 1;
    goto shutdown;
  }
  if (pcap_setfilter(pc, &fp) < 0) {
    loge("pcap_setfilter failed: %s", pcap_geterr(pc));
    pcap_freecode(&fp);
    pcap_close(pc);
    g_stop = 1;
    goto shutdown;
  }
  pcap_freecode(&fp);

  while (!g_stop) {
    int r = pcap_dispatch(pc, 64, on_packet, NULL);
    if (r < 0) {
      loge("pcap_dispatch error: %s", pcap_geterr(pc));
      break;
    }
    /* r==0 is timeout */
  }

  pcap_breakloop(pc);
  pcap_close(pc);

shutdown:
  g_stop = 1;

  /* wake sender thread */
  pthread_mutex_lock(&g_q.mu);
  pthread_cond_broadcast(&g_q.cv_not_empty);
  pthread_mutex_unlock(&g_q.mu);

  if (sink_on) {
    pthread_join(sender_th, NULL);
    curl_global_cleanup();
  }

  jobq_destroy(&g_q);
  return g_stop ? 1 : 0;
}
