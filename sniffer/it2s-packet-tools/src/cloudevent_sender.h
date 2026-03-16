#ifndef CLOUDEVENT_SENDER_H
#define CLOUDEVENT_SENDER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *sink_url;          /* K_SINK */
    const char *ce_type;           /* fallback CE type */
    const char *ce_source;         /* optional explicit source */
    const char *component;         /* e.g. "sniffer" */
    const char *iface;             /* e.g. "lo" */
    const char *node_name;         /* K8S_NODE_NAME / NODE_NAME / HOSTNAME */
    int send_queue_max;            /* default 1000 */
    int curl_timeout_sec;          /* default 5 */
    bool enable_health_server;     /* true if PORT set */
    int health_port;               /* default 8080 */
} ce_sender_config_t;

typedef struct {
    char ce_id[37];
    uint64_t seq_no;
    int64_t t_event_unix_ns;
    int64_t t_ce_built_unix_ns;
    int64_t t_enqueue_unix_ns;
} ce_sender_meta_t;

/* Fill config from environment with sensible defaults. */
void ce_sender_config_from_env(ce_sender_config_t *cfg,
                               const char *default_component,
                               const char *default_iface);

/* Initialize sender threads/resources. Safe even if K_SINK is unset. */
int ce_sender_init(const ce_sender_config_t *cfg);

/* Stop sender threads and release resources. */
void ce_sender_shutdown(void);

/* Returns true if CloudEvent sending is actually enabled (K_SINK present). */
bool ce_sender_enabled(void);

/* Build structured CloudEvent and enqueue it for sending.
 *
 * event_type_or_null:
 *   - if non-NULL/non-empty, uses that type (e.g. "its.cam", "its.denm")
 *   - otherwise falls back to cfg->ce_type
 *
 * subject_or_null:
 *   optional CE subject (we use the seq no as string)
 *
 * data_json_obj:
 *   must already be a valid JSON object string
 *
 * stationtype_or_neg:
 *   include CloudEvent extension "stationtype" if >= 0
 *
 * Returns 0 on success, -1 on failure/drop.
 */
int ce_sender_send_json(const char *event_type_or_null,
                        const char *subject_or_null,
                        const char *data_json_obj,
                        int stationtype_or_neg,
                        uint64_t seq_no,
                        int64_t t_event_unix_ns,
                        ce_sender_meta_t *out_meta);

#ifdef __cplusplus
}
#endif

#endif
