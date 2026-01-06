// sniffer_rsock.c — AF_PACKET raw-socket capture + libcurl CloudEvents (structured)
// Build (Alpine): gcc -O2 -Wall -Wextra -pthread sniffer_rsock.c -lcurl -o sniffer

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/if_ether.h>
#include <linux/if_packet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <pthread.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <curl/curl.h>

/* -------------------------
 * Config via environment
 * ------------------------- */
static const char *ENV_IFACE;
static const char *ENV_BPF;            /* parsed loosely, logged always */
static const char *ENV_DISPLAY_FILTER; /* logged only */
static int   ENV_LOG_EVERY = 10;
static const char *ENV_CE_TYPE;
static bool  ENV_INCLUDE_RAW_HEX = false;     /* stdout NDJSON raw hex */
static bool  ENV_CE_INCLUDE_RAW_HEX = true;  /* CloudEvents data raw hex (default ON) */
static const char *ENV_SINK_URL;       /* K_SINK injected by SinkBinding */
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

static const char *truthy(const char *s) {
  if (!s) return NULL;
  if (strcmp(s, "1") == 0) return s;
  if (strcasecmp(s, "true") == 0) return s;
  if (strcasecmp(s, "yes") == 0) return s;
  return NULL;
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
 * CloudEvents structured body builder
 * ------------------------- */
static char *build_cloudevent_structured(
    const char *event_type,
    const char *source,
    const char *subject_or_null, /* frame_number as string or NULL */
    const char *data_json_obj,   /* must be JSON object string */
    size_t *out_len
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

  if (!type_esc || !src_esc || !id_esc || !time_esc || !subj_part) {
    free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part);
    return NULL;
  }

  size_t body_sz =
    strlen(type_esc) + strlen(src_esc) + strlen(id_esc) + strlen(time_esc) +
    strlen(subj_part) + strlen(data_json_obj) + 256;

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
      "%s,"
      "\"data\":%s"
    "}",
    type_esc, src_esc, id_esc, time_esc, subj_part, data_json_obj
  );

  *out_len = strlen(body);

  free(type_esc); free(src_esc); free(id_esc); free(time_esc); free(subj_part);
  return body;
}

/* -------------------------
 * HTTP sender (CloudEvents structured)
 * ------------------------- */
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
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L); /* important in multi-threaded apps */

  /* disable proxy env usage */
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
 * Packet parsing / filtering
 * Supports:
 *   - Ethernet frames (with up to 2 VLAN tags)
 *   - Radiotap + 802.11 data + LLC/SNAP ethertype + optional IPv4/UDP
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

typedef struct {
  uint32_t v_tc_fl;     /* version(4), traffic class(8), flow label(20) */
  uint16_t payload_len;
  uint8_t  next_hdr;
  uint8_t  hop_limit;
  uint8_t  saddr[16];
  uint8_t  daddr[16];
} ipv6_hdr_t;

/* Radiotap (minimal header) */
typedef struct {
  uint8_t  it_version;
  uint8_t  it_pad;
  uint16_t it_len;      /* little-endian on the wire */
  uint32_t it_present;  /* little-endian; may extend, but we only use as heuristic */
} radiotap_hdr_t;
#pragma pack(pop)

typedef struct {
  uint8_t src_mac[6];
  uint8_t dst_mac[6];

  uint16_t ethertype;        /* after vlan unwrapping or after LLC/SNAP */
  bool has_vlan;
  int vlan_id;
  uint16_t outer_ethertype;  /* original ethertype (Ethernet) or SNAP ethertype */

  bool is_etsi_its;
  bool is_ipv4;
  bool is_ipv6;
  bool is_udp;

  char src_ip[64];
  char dst_ip[64];
  int src_port;
  int dst_port;
} pkt_meta_t;

/* radiotap length (LE) */
static bool parse_radiotap_offset(const uint8_t *pkt, size_t pkt_len, size_t *out_off) {
  if (pkt_len < sizeof(radiotap_hdr_t)) return false;

  const radiotap_hdr_t *rt = (const radiotap_hdr_t *)pkt;

  /* Strong-ish heuristics to avoid false positives on Ethernet:
     - version must be 0
     - pad usually 0
     - present bitmap non-zero
     - len reasonable
     - and next bytes look like 802.11 FC version=0 after rt_len */
  if (rt->it_version != 0) return false;
  if (rt->it_pad != 0) return false;

  uint16_t rt_len = (uint16_t)pkt[2] | ((uint16_t)pkt[3] << 8);
  if (rt_len < sizeof(radiotap_hdr_t)) return false;
  if (rt_len > pkt_len) return false;
  if (rt_len > 4096) return false;

  uint32_t present = (uint32_t)pkt[4] |
                     ((uint32_t)pkt[5] << 8) |
                     ((uint32_t)pkt[6] << 16) |
                     ((uint32_t)pkt[7] << 24);
  if (present == 0) return false;

  if (pkt_len < rt_len + 2) return false;
  uint16_t fc = (uint16_t)pkt[rt_len] | ((uint16_t)pkt[rt_len + 1] << 8);
  if ((fc & 0x3) != 0) return false; /* 802.11 version bits must be 0 */

  *out_off = rt_len;
  return true;
}

static size_t wifi_hdr_len(const uint8_t *p, size_t n) {
  if (n < 2) return 0;
  uint16_t fc = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
  uint8_t type = (fc >> 2) & 0x3;
  if (type != 2) return 0; /* data only */

  bool toDS   = (fc & (1u<<8))  != 0;
  bool fromDS = (fc & (1u<<9))  != 0;

  size_t hdr = 24; /* base data header */
  if (toDS && fromDS) hdr += 6; /* addr4 present */

  uint8_t subtype = (fc >> 4) & 0xF;
  bool qos = (subtype & 0x8) != 0;
  if (qos) hdr += 2;

  return (n >= hdr) ? hdr : 0;
}

/* Parse 802.11 data frames + LLC/SNAP to recover ethertype & optional IPv4/UDP */
static void parse_wifi_meta(const uint8_t *p, size_t n, pkt_meta_t *m) {
  if (n < 24) return;

  uint16_t fc = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
  uint8_t type = (fc >> 2) & 0x3;
  if (type != 2) return; /* only data */

  bool toDS   = (fc & (1u<<8))  != 0;
  bool fromDS = (fc & (1u<<9))  != 0;

  const uint8_t *addr1 = p + 4;
  const uint8_t *addr2 = p + 10;
  const uint8_t *addr3 = p + 16;

  const uint8_t *sa = NULL;
  const uint8_t *da = NULL;

  if (!toDS && !fromDS) { da = addr1; sa = addr2; }
  else if (toDS && !fromDS) { sa = addr2; da = addr3; }
  else if (!toDS && fromDS) { da = addr1; sa = addr3; }
  else { /* WDS */
    if (n < 30) return;
    da = addr3;
    sa = p + 24; /* addr4 */
  }

  memcpy(m->dst_mac, da, 6);
  memcpy(m->src_mac, sa, 6);

  size_t hlen = wifi_hdr_len(p, n);
  if (!hlen) return;

  /* LLC/SNAP: AA AA 03, then OUI(3), then ethertype(2) */
  if (n < hlen + 8) return;
  const uint8_t *llc = p + hlen;

  if (llc[0] == 0xAA && llc[1] == 0xAA && llc[2] == 0x03) {
    uint16_t et = ((uint16_t)llc[6] << 8) | (uint16_t)llc[7];
    m->outer_ethertype = et;
    m->ethertype = et;

    if (et == 0x8947) m->is_etsi_its = true;

    const uint8_t *l3 = llc + 8;
    size_t l3n = n - (hlen + 8);

    /* IPv4 UDP */
    if (et == 0x0800 && l3n >= sizeof(ipv4_hdr_t)) {
      const ipv4_hdr_t *ip = (const ipv4_hdr_t *)l3;
      uint8_t ver = (ip->ver_ihl >> 4) & 0xF;
      uint8_t ihl = (ip->ver_ihl & 0xF) * 4;
      if (ver == 4 && l3n >= ihl) {
        m->is_ipv4 = true;
        struct in_addr a;
        a.s_addr = ip->saddr; inet_ntop(AF_INET, &a, m->src_ip, sizeof(m->src_ip));
        a.s_addr = ip->daddr; inet_ntop(AF_INET, &a, m->dst_ip, sizeof(m->dst_ip));

        if (ip->proto == 17 && l3n >= ihl + sizeof(udp_hdr_t)) {
          const udp_hdr_t *udp = (const udp_hdr_t *)(l3 + ihl);
          m->src_port = (int)ntohs(udp->sport);
          m->dst_port = (int)ntohs(udp->dport);
          m->is_udp = true;
        }
      }
    }

    /* IPv6 UDP (simple: next header UDP directly) */
    if (et == 0x86dd && l3n >= sizeof(ipv6_hdr_t)) {
      const ipv6_hdr_t *ip6 = (const ipv6_hdr_t *)l3;
      uint32_t v = ntohl(ip6->v_tc_fl);
      uint8_t ver = (uint8_t)((v >> 28) & 0xF);
      if (ver == 6) {
        m->is_ipv6 = true;
        inet_ntop(AF_INET6, ip6->saddr, m->src_ip, sizeof(m->src_ip));
        inet_ntop(AF_INET6, ip6->daddr, m->dst_ip, sizeof(m->dst_ip));

        if (ip6->next_hdr == 17 && l3n >= sizeof(ipv6_hdr_t) + sizeof(udp_hdr_t)) {
          const udp_hdr_t *udp = (const udp_hdr_t *)(l3 + sizeof(ipv6_hdr_t));
          m->src_port = (int)ntohs(udp->sport);
          m->dst_port = (int)ntohs(udp->dport);
          m->is_udp = true;
        }
      }
    }
  }
}

static void parse_packet_meta(const uint8_t *pkt, size_t pkt_len, pkt_meta_t *m) {
  memset(m, 0, sizeof(*m));
  m->vlan_id = -1;
  m->src_port = -1;
  m->dst_port = -1;

  /* Try radiotap first (monitor-mode interfaces) */
  size_t rt_off = 0;
  if (parse_radiotap_offset(pkt, pkt_len, &rt_off)) {
    if (pkt_len > rt_off) {
      parse_wifi_meta(pkt + rt_off, pkt_len - rt_off, m);
    }
    return;
  }

  /* Ethernet path */
  if (pkt_len < sizeof(eth_hdr_t)) return;

  const eth_hdr_t *eth = (const eth_hdr_t *)pkt;
  memcpy(m->src_mac, eth->src, 6);
  memcpy(m->dst_mac, eth->dst, 6);
  uint16_t et = ntohs(eth->ethertype);
  m->outer_ethertype = et;

  size_t off = sizeof(eth_hdr_t);

  /* Unwrap up to 2 VLAN tags (QinQ) */
  int vlan_depth = 0;
  while ((et == 0x8100 || et == 0x88a8) &&
         pkt_len >= off + sizeof(vlan_hdr_t) &&
         vlan_depth < 2) {
    const vlan_hdr_t *v = (const vlan_hdr_t *)(pkt + off);
    uint16_t tci = ntohs(v->tci);
    if (vlan_depth == 0) {
      m->vlan_id = (int)(tci & 0x0FFF);
      m->has_vlan = true;
    }
    et = ntohs(v->ethertype);
    off += sizeof(vlan_hdr_t);
    vlan_depth++;
  }

  m->ethertype = et;

  if (et == 0x8947) m->is_etsi_its = true;

  /* IPv4 UDP */
  if (et == 0x0800 && pkt_len >= off + sizeof(ipv4_hdr_t)) {
    const ipv4_hdr_t *ip = (const ipv4_hdr_t *)(pkt + off);
    uint8_t ver = (ip->ver_ihl >> 4) & 0xF;
    uint8_t ihl = (ip->ver_ihl & 0xF) * 4;
    if (ver == 4 && pkt_len >= off + ihl) {
      m->is_ipv4 = true;
      struct in_addr a;
      a.s_addr = ip->saddr; inet_ntop(AF_INET, &a, m->src_ip, sizeof(m->src_ip));
      a.s_addr = ip->daddr; inet_ntop(AF_INET, &a, m->dst_ip, sizeof(m->dst_ip));

      if (ip->proto == 17 && pkt_len >= off + ihl + sizeof(udp_hdr_t)) {
        const udp_hdr_t *udp = (const udp_hdr_t *)(pkt + off + ihl);
        m->src_port = (int)ntohs(udp->sport);
        m->dst_port = (int)ntohs(udp->dport);
        m->is_udp = true;
      }
    }
  }

  /* IPv6 UDP (simple case: next header is UDP directly) */
  if (et == 0x86dd && pkt_len >= off + sizeof(ipv6_hdr_t)) {
    const ipv6_hdr_t *ip6 = (const ipv6_hdr_t *)(pkt + off);
    uint32_t v = ntohl(ip6->v_tc_fl);
    uint8_t ver = (uint8_t)((v >> 28) & 0xF);
    if (ver == 6) {
      m->is_ipv6 = true;
      inet_ntop(AF_INET6, ip6->saddr, m->src_ip, sizeof(m->src_ip));
      inet_ntop(AF_INET6, ip6->daddr, m->dst_ip, sizeof(m->dst_ip));

      if (ip6->next_hdr == 17 && pkt_len >= off + sizeof(ipv6_hdr_t) + sizeof(udp_hdr_t)) {
        const udp_hdr_t *udp = (const udp_hdr_t *)(pkt + off + sizeof(ipv6_hdr_t));
        m->src_port = (int)ntohs(udp->sport);
        m->dst_port = (int)ntohs(udp->dport);
        m->is_udp = true;
      }
    }
  }
}

static bool bpf_requests_udp_port(const char *bpf, int *out_port) {
  if (!bpf) return false;
  const char *p = strstr(bpf, "udp port");
  if (!p) return false;
  p += strlen("udp port");
  while (*p == ' ' || *p == '\t') p++;
  if (!*p) return false;
  char *end = NULL;
  long v = strtol(p, &end, 10);
  if (end == p || v <= 0 || v > 65535) return false;
  *out_port = (int)v;
  return true;
}

static bool should_accept_packet(const pkt_meta_t *m) {
  /* If user explicitly set BPF="" => accept EVERYTHING (agnostic mode) */
  if (ENV_BPF && ENV_BPF[0] == '\0') return true;

  bool match_its = m->is_etsi_its;

  int udp_port = 2001;
  if (!bpf_requests_udp_port(ENV_BPF, &udp_port)) udp_port = 2001;

  bool match_udp = (m->is_udp && (m->src_port == udp_port || m->dst_port == udp_port));
  return match_its || match_udp;
}

/* -------------------------
 * Record JSON (NDJSON)
 * ------------------------- */
static char *build_record_json(
    uint64_t frame_no,
    const struct timeval *tv,
    const uint8_t *pkt,
    size_t pkt_len,
    const pkt_meta_t *m,
    bool include_raw_hex
) {
  char ts[64];
  iso8601_from_timeval(tv, ts, sizeof(ts));

  char srcmac[32] = {0}, dstmac[32] = {0};
  mac_to_str(m->src_mac, srcmac, sizeof(srcmac));
  mac_to_str(m->dst_mac, dstmac, sizeof(dstmac));

  char *ts_esc     = json_escape(ts);
  char *srcmac_esc = json_escape(srcmac);
  char *dstmac_esc = json_escape(dstmac);

  char *srcip_esc = json_escape(m->src_ip[0] ? m->src_ip : "");
  char *dstip_esc = json_escape(m->dst_ip[0] ? m->dst_ip : "");

  char *raw_hex = NULL;
  if (include_raw_hex) raw_hex = hex_encode(pkt, pkt_len);

  if (!ts_esc || !srcmac_esc || !dstmac_esc || !srcip_esc || !dstip_esc) {
    free(ts_esc); free(srcmac_esc); free(dstmac_esc); free(srcip_esc); free(dstip_esc);
    free(raw_hex);
    return NULL;
  }

  char vlan_part[64];
  if (m->has_vlan && m->vlan_id >= 0) snprintf(vlan_part, sizeof(vlan_part), "%d", m->vlan_id);
  else snprintf(vlan_part, sizeof(vlan_part), "null");

  char srcip_part[128];
  if (m->src_ip[0]) snprintf(srcip_part, sizeof(srcip_part), "\"%s\"", srcip_esc);
  else snprintf(srcip_part, sizeof(srcip_part), "null");

  char dstip_part[128];
  if (m->dst_ip[0]) snprintf(dstip_part, sizeof(dstip_part), "\"%s\"", dstip_esc);
  else snprintf(dstip_part, sizeof(dstip_part), "null");

  char srcport_part[64];
  if (m->src_port >= 0) snprintf(srcport_part, sizeof(srcport_part), "%d", m->src_port);
  else snprintf(srcport_part, sizeof(srcport_part), "null");

  char dstport_part[64];
  if (m->dst_port >= 0) snprintf(dstport_part, sizeof(dstport_part), "%d", m->dst_port);
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
      "\"cam_layer\":\"raw\","
      "\"frame_len\":%zu,"
      "\"cam_fields\":{"
        "\"src_mac\":\"%s\","
        "\"dst_mac\":\"%s\","
        "\"outer_ethertype\":\"0x%04x\","
        "\"ethertype\":\"0x%04x\","
        "\"vlan_id\":%s,"
        "\"is_etsi_its_ethertype\":%s,"
        "\"is_ipv4\":%s,"
        "\"is_ipv6\":%s,"
        "\"is_udp\":%s,"
        "\"src_ip\":%s,"
        "\"dst_ip\":%s,"
        "\"src_port\":%s,"
        "\"dst_port\":%s"
      "}%s%s%s"
    "}",
    ts_esc,
    frame_no,
    pkt_len,
    srcmac_esc,
    dstmac_esc,
    (unsigned)m->outer_ethertype,
    (unsigned)m->ethertype,
    vlan_part,
    m->is_etsi_its ? "true" : "false",
    m->is_ipv4 ? "true" : "false",
    m->is_ipv6 ? "true" : "false",
    m->is_udp ? "true" : "false",
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

/* -------------------------
 * Env parsing
 * ------------------------- */
static void set_defaults_from_env(void) {
  ENV_IFACE = getenv("IFACE");
  if (!ENV_IFACE || !ENV_IFACE[0]) ENV_IFACE = "eth0";

  /* Important: only use default filter if BPF env var is UNSET.
     If BPF is set to empty string, we keep it empty => accept all. */
  ENV_BPF = getenv("BPF");
  if (ENV_BPF == NULL) {
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
  if (nd && nd[0]) ENV_STDOUT_NDJSON = truthy(nd) != NULL;

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

/* -------------------------
 * Raw-socket setup
 * ------------------------- */
static int open_bound_packet_socket(const char *iface, bool promisc, uint8_t out_mac[6]) {
  int fd = socket(PF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
  if (fd < 0) {
    loge("socket(PF_PACKET) failed: errno=%d (%s)", errno, strerror(errno));
    return -1;
  }

  struct ifreq ifr_idx;
  memset(&ifr_idx, 0, sizeof(ifr_idx));
  strncpy(ifr_idx.ifr_name, iface, IFNAMSIZ - 1);

  if (ioctl(fd, SIOCGIFINDEX, &ifr_idx) < 0) {
    loge("SIOCGIFINDEX failed for iface=%s: errno=%d (%s)", iface, errno, strerror(errno));
    close(fd);
    return -1;
  }

  int ifindex = ifr_idx.ifr_ifindex;

  struct sockaddr_ll sll;
  memset(&sll, 0, sizeof(sll));
  sll.sll_family   = AF_PACKET;
  sll.sll_protocol = htons(ETH_P_ALL);
  sll.sll_ifindex  = ifindex;

  if (bind(fd, (struct sockaddr *)&sll, sizeof(sll)) < 0) {
    loge("bind(AF_PACKET) failed: errno=%d (%s)", errno, strerror(errno));
    close(fd);
    return -1;
  }

  struct ifreq ifr_mac;
  memset(&ifr_mac, 0, sizeof(ifr_mac));
  strncpy(ifr_mac.ifr_name, iface, IFNAMSIZ - 1);

  if (ioctl(fd, SIOCGIFHWADDR, &ifr_mac) == 0) {
    memcpy(out_mac, ifr_mac.ifr_hwaddr.sa_data, 6);
  } else {
    memset(out_mac, 0, 6);
  }

  if (promisc) {
    struct packet_mreq mreq;
    memset(&mreq, 0, sizeof(mreq));
    mreq.mr_ifindex = ifindex;
    mreq.mr_type    = PACKET_MR_PROMISC;

    if (setsockopt(fd, SOL_PACKET, PACKET_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) != 0) {
      logw("failed to enable promiscuous mode (continuing): errno=%d (%s)", errno, strerror(errno));
    }
  }

  return fd;
}

/* -------------------------
 * Main
 * ------------------------- */
int main(void) {
  setvbuf(stdout, NULL, _IOLBF, 0);

  set_defaults_from_env();

  signal(SIGINT, on_sig);
  signal(SIGTERM, on_sig);

  logi(">> LIVE capture iface='%s' promisc=%s", ENV_IFACE, ENV_PROMISCUOUS ? "on" : "off");
  logi(">> BPF='%s' (fast built-in filter; BPF='' means accept-all)", ENV_BPF ? ENV_BPF : "(null)");
  if (ENV_DISPLAY_FILTER && ENV_DISPLAY_FILTER[0]) {
    logi(">> DISPLAY_FILTER='%s' (ignored in raw-socket version)", ENV_DISPLAY_FILTER);
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

  uint8_t iface_mac[6] = {0};
  int fd = open_bound_packet_socket(ENV_IFACE, ENV_PROMISCUOUS, iface_mac);
  if (fd < 0) {
    g_stop = 1;
    goto shutdown;
  }

  const size_t BUFSZ = 262144;
  uint8_t *buf = (uint8_t *)malloc(BUFSZ);
  if (!buf) {
    loge("malloc(%zu) failed", BUFSZ);
    close(fd);
    g_stop = 1;
    goto shutdown;
  }

  uint64_t frame_no = 0;
  uint64_t accepted = 0;
  uint64_t rx_total = 0;
  uint64_t dropped  = 0;

  struct pollfd pfd;
  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  pfd.events = POLLIN;

  while (!g_stop) {
    int prc = poll(&pfd, 1, 1000);
    if (prc < 0) {
      if (errno == EINTR) continue;
      loge("poll failed: errno=%d (%s)", errno, strerror(errno));
      break;
    }
    if (prc == 0) continue;

    if (pfd.revents & POLLIN) {
      ssize_t n = recvfrom(fd, buf, BUFSZ, 0, NULL, NULL);
      if (n < 0) {
        if (errno == EINTR) continue;
        loge("recvfrom failed: errno=%d (%s)", errno, strerror(errno));
        break;
      }

      rx_total++;
      frame_no++;
      size_t pkt_len = (size_t)n;

      pkt_meta_t meta;
      parse_packet_meta(buf, pkt_len, &meta);

      if (!should_accept_packet(&meta)) {
        dropped++;
        if (ENV_LOG_EVERY > 0 && (rx_total % (uint64_t)ENV_LOG_EVERY) == 0) {
          logi("[rsock] rx=%" PRIu64 " accepted=%" PRIu64 " dropped=%" PRIu64 " (sink=%s)",
               rx_total, accepted, dropped, sink_on ? "on" : "off");
        }
        continue;
      }

      struct timeval tv;
      gettimeofday(&tv, NULL);

      /* Build stdout record (may or may not include raw hex) */
      char *rec_out = build_record_json(frame_no, &tv, buf, pkt_len, &meta, ENV_INCLUDE_RAW_HEX);
      if (!rec_out) continue;

      if (ENV_STDOUT_NDJSON) {
        fputs(rec_out, stdout);
        fputc('\n', stdout);
      }

      /* Build CloudEvents record (ensure raw hex included if requested) */
      char *rec_ce = rec_out;
      bool rec_ce_is_separate = false;

      if (sink_on && ENV_CE_INCLUDE_RAW_HEX && !ENV_INCLUDE_RAW_HEX) {
        rec_ce = build_record_json(frame_no, &tv, buf, pkt_len, &meta, true);
        if (!rec_ce) {
          /* fallback: still emit event without raw */
          rec_ce = rec_out;
        } else {
          rec_ce_is_separate = true;
        }
      }

      if (sink_on) {
        char subj[32];
        snprintf(subj, sizeof(subj), "%" PRIu64, frame_no);

        size_t body_len = 0;
        char *ce = build_cloudevent_structured(ENV_CE_TYPE, CE_SOURCE, subj, rec_ce, &body_len);
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

      accepted++;
      if (ENV_LOG_EVERY > 0 && (accepted % (uint64_t)ENV_LOG_EVERY) == 0) {
        logi("[rsock] accepted=%" PRIu64 " rx=%" PRIu64 " dropped=%" PRIu64 " (sink=%s)",
             accepted, rx_total, dropped, sink_on ? "on" : "off");
      }
    }
  }

  free(buf);
  close(fd);

shutdown:
  g_stop = 1;

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
