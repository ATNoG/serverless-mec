#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <strings.h>

#include <it2s-rsock/rsock.h>
#include <it2s-rsock/utils.h>
#include <it2s-ublox/ublox.h>
#include <it2s-llc.h>
#include <it2s-mac.h>
#include <it2s-gn/gn.h>
#include <it2s-asn/etsi-its-v2/cam/EI2_CAM.h>
#include <it2s-asn/etsi-its-v2/denm/EI2_DENM.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/uper_decoder.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/jer_encoder.h>
#include <it2s-asn/etsi-its-v2/cdd-2.2.1/EI2_MessageId.h>

#include "its_common.h"
#include "logger.h"
#include "cloudevent_sender.h"

static const char *g_interface = "unknown";
static uint64_t g_seq_no = 0;
static int g_ce_include_raw_hex = 1;

static const char *truthy_local(const char *s) {
    if (!s) return NULL;
    if (strcmp(s, "1") == 0) return s;
    if (strcasecmp(s, "true") == 0) return s;
    if (strcasecmp(s, "yes") == 0) return s;
    return NULL;
}

static void load_local_env(void) {
    const char *v = getenv("CE_INCLUDE_RAW_HEX");
    if (v == NULL) {
        g_ce_include_raw_hex = 1;   /* default ON, like your other version */
    } else {
        g_ce_include_raw_hex = truthy_local(v) ? 1 : 0;
    }
}

static int64_t unix_ns_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t) ts.tv_sec * 1000000000LL + (int64_t) ts.tv_nsec;
}

static char *json_escape_local(const char *s) {
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

static char *asn_msg_to_json(asn_TYPE_descriptor_t *desc, void *msg) {
    int json_buffer_len = 2048;

    for (int attempt = 0; attempt < 6; attempt++) {
        char *json_buffer = calloc((size_t) json_buffer_len, 1);
        if (!json_buffer) return NULL;

        asn_enc_rval_t enc =
            asn_encode_to_buffer(NULL, ATS_JER_MINIFIED, desc, msg,
                                 json_buffer, (size_t) json_buffer_len);

        if (enc.encoded >= 0 && enc.encoded < json_buffer_len) {
            json_buffer[json_buffer_len - 1] = '\0';
            return json_buffer;
        }

        free(json_buffer);
        json_buffer_len *= 2;
    }

    return NULL;
}

static char *build_wrapped_event_json(const char *message_kind,
                                      const char *iface,
                                      const char *raw_hex_or_null,
                                      const char *decoded_json,
                                      uint64_t seq_no,
                                      int station_id_or_neg) {
    if (!message_kind || !iface || !decoded_json) return NULL;

    char *kind_esc = json_escape_local(message_kind);
    char *iface_esc = json_escape_local(iface);
    char *raw_hex_esc = NULL;

    if (!kind_esc || !iface_esc) {
        free(kind_esc);
        free(iface_esc);
        return NULL;
    }

    if (raw_hex_or_null && raw_hex_or_null[0]) {
        raw_hex_esc = json_escape_local(raw_hex_or_null);
        if (!raw_hex_esc) {
            free(kind_esc);
            free(iface_esc);
            return NULL;
        }
    }

    char station_id_part[64];
    station_id_part[0] = '\0';
    if (station_id_or_neg >= 0) {
        snprintf(station_id_part, sizeof(station_id_part),
                 ",\"station_id\":%d", station_id_or_neg);
    }

    size_t buf_sz = 512 +
                    strlen(kind_esc) +
                    strlen(iface_esc) +
                    strlen(decoded_json) +
                    strlen(station_id_part) +
                    (raw_hex_esc ? strlen(raw_hex_esc) + 32 : 0);

    char *buf = (char *) malloc(buf_sz);
    if (!buf) {
        free(kind_esc);
        free(iface_esc);
        free(raw_hex_esc);
        return NULL;
    }

    int n = snprintf(
        buf, buf_sz,
        "{"
          "\"timestamp_unix_ns\":%" PRId64 ","
          "\"seq_no\":%" PRIu64 ","
          "\"message_kind\":\"%s\","
          "\"iface\":\"%s\","
          "\"cam_layer\":\"its\""
          "%s"
          "%s%s%s"
          ",\"decoded\":%s"
        "}",
        unix_ns_now(),
        seq_no,
        kind_esc,
        iface_esc,
        station_id_part,
        raw_hex_esc ? ",\"frame_raw_hex\":\"" : "",
        raw_hex_esc ? raw_hex_esc : "",
        raw_hex_esc ? "\"" : "",
        decoded_json
    );

    free(kind_esc);
    free(iface_esc);
    free(raw_hex_esc);

    if (n < 0 || (size_t) n >= buf_sz) {
        free(buf);
        return NULL;
    }

    return buf;
}

static void emit_cam_cloudevent(EI2_CAM_t *cam, const char *decoded_json, const char *raw_hex) {
    if (!ce_sender_enabled()) return;

    uint64_t seq_no = ++g_seq_no;

    char *event_json = build_wrapped_event_json(
        "CAM",
        g_interface,
        g_ce_include_raw_hex ? raw_hex : NULL,
        decoded_json,
        seq_no,
        cam->header.stationId
    );
    if (!event_json) {
        log_error("[cam] failed to build wrapped CloudEvent payload");
        return;
    }

    char subject[32];
    snprintf(subject, sizeof(subject), "%" PRIu64, seq_no);

    int station_type = cam->cam.camParameters.basicContainer.stationType;

    if (ce_sender_send_json(
            "its.cam",
            subject,
            event_json,
            station_type,
            seq_no,
            unix_ns_now(),
            NULL
        ) != 0) {
        log_error("[cam] failed to enqueue CloudEvent");
    }

    free(event_json);
}

static void emit_denm_cloudevent(EI2_DENM_t *denm, const char *decoded_json, const char *raw_hex) {
    if (!ce_sender_enabled()) return;

    uint64_t seq_no = ++g_seq_no;

    char *event_json = build_wrapped_event_json(
        "DENM",
        g_interface,
        g_ce_include_raw_hex ? raw_hex : NULL,
        decoded_json,
        seq_no,
        denm->header.stationId
    );
    if (!event_json) {
        log_error("[denm] failed to build wrapped CloudEvent payload");
        return;
    }

    char subject[32];
    snprintf(subject, sizeof(subject), "%" PRIu64, seq_no);

    if (ce_sender_send_json(
            "its.denm",
            subject,
            event_json,
            -1,
            seq_no,
            unix_ns_now(),
            NULL
        ) != 0) {
        log_error("[denm] failed to enqueue CloudEvent");
    }

    free(event_json);
}

/* Adapt as needed */
void cam_cb(EI2_CAM_t *cam, const char *raw_hex) {
    char *json_buffer = NULL;

    log_info("[cam] latitude: %d", cam->cam.camParameters.basicContainer.referencePosition.latitude);
    log_info("[cam] longitude: %d", cam->cam.camParameters.basicContainer.referencePosition.longitude);

    if (cam->cam.camParameters.highFrequencyContainer.present ==
        EI2_HighFrequencyContainer_PR_basicVehicleContainerHighFrequency) {
        EI2_BasicVehicleContainerHighFrequency_t *bvc_hf =
            &cam->cam.camParameters.highFrequencyContainer.choice.basicVehicleContainerHighFrequency;
        log_info("[cam] speed: %d", bvc_hf->speed.speedValue);
    }

    json_buffer = asn_msg_to_json(&asn_DEF_EI2_CAM, cam);
    if (!json_buffer) {
        log_error("[cam] error encoding CAM to JSON");
        return;
    }

    log_info("[cam] JSON encoded: %s", json_buffer);

    emit_cam_cloudevent(cam, json_buffer, raw_hex);

    free(json_buffer);
}

/* Adapt as needed */
void denm_cb(EI2_DENM_t *denm, const char *raw_hex) {
    char *json_buffer = asn_msg_to_json(&asn_DEF_EI2_DENM, denm);
    if (!json_buffer) {
        log_error("[denm] error encoding DENM to JSON");
        return;
    }

    log_info("[denm] JSON encoded: %s", json_buffer);

    emit_denm_cloudevent(denm, json_buffer, raw_hex);

    free(json_buffer);
}

void generic_cb(int tx_access, uint8_t *packet, size_t packet_len) {
    int rv;
    char *rx_hex = NULL;

    uint8_t bufA[IT2S_WIFI_MAX_PACKET_SIZE];
    uint8_t bufB[IT2S_WIFI_MAX_PACKET_SIZE];
    uint32_t lenA = 0, lenB = 0;
    uint8_t src_mac[6], dst_mac[6];
    uint16_t ether_type = 0;
    uint8_t *gn_buf = NULL;
    uint32_t gn_len = 0;

    rx_hex = malloc(packet_len * 2 + 1);
    if (rx_hex) {
        char *buf_ptr = rx_hex;
        for (size_t i = 0; i < packet_len; i++) {
            buf_ptr += sprintf(buf_ptr, "%02x", packet[i]);
        }
        log_info("[phy] <- received packet | size: %zuB data: %s", packet_len, rx_hex);
    } else {
        log_warn("[phy] failed to allocate raw hex buffer");
    }

    /* rsock: plain 802.3 ethernet frame - dst_mac(6) | src_mac(6) | ethertype BE(2) | GN payload */
    if (tx_access == ACCESS_RSOCK) {
        if (packet_len < 14) {
            log_error("[phy] packet too short");
            goto cleanup;
        }

        memcpy(dst_mac, packet, 6);
        memcpy(src_mac, packet + 6, 6);
        ether_type = ((uint16_t) packet[12] << 8) | packet[13];
        gn_buf = packet + 14;
        gn_len = (uint32_t) packet_len - 14;

        log_debug("[mac] decap ok | src: %02x:%02x:%02x:%02x:%02x:%02x dst: %02x:%02x:%02x:%02x:%02x:%02x ether_type: 0x%04x len: %uB",
                  src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5],
                  dst_mac[0], dst_mac[1], dst_mac[2], dst_mac[3], dst_mac[4], dst_mac[5],
                  ether_type, gn_len);
    }

    /* ublox: full 802.11 frame - MAC decap + LLC decap + FCS strip */
    else if (tx_access == ACCESS_UBLOX) {
        rv = it2s_mac_decap(packet, (uint16_t) packet_len, bufA, &lenA, src_mac, dst_mac);
        if (rv) {
            log_error("[mac] decap failed");
            goto cleanup;
        }

        log_debug("[mac] decap ok | len: %uB src: %02x:%02x:%02x:%02x:%02x:%02x dst: %02x:%02x:%02x:%02x:%02x:%02x",
                  lenA,
                  src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5],
                  dst_mac[0], dst_mac[1], dst_mac[2], dst_mac[3], dst_mac[4], dst_mac[5]);

        rv = it2s_llc_decap(bufA, (uint16_t) lenA, bufB, &lenB, &ether_type);
        if (rv) {
            log_error("[llc] decap failed");
            goto cleanup;
        }

        log_debug("[llc] decap ok | ether_type: 0x%04x len: %uB", ether_type, lenB);

        if (lenB < 4) {
            log_error("[fcs] payload too short to strip FCS");
            goto cleanup;
        }

        lenB -= 4;
        gn_buf = bufB;
        gn_len = lenB;
    } else {
        log_error("[phy] unsupported tx_access: %d", tx_access);
        goto cleanup;
    }

    if (gn_len >= 1 && (gn_buf[0] & M_BNH) == 0x02) {
        log_warn("[gn] secured packet detected - security decap not supported, skipping");
        goto cleanup;
    }

    it2s_gn_header_t *gn = it2s_gn_header_new(0, 0);
    if (!gn) {
        log_error("[gn] header allocation failed");
        goto cleanup;
    }

    rv = it2s_gn_decap(gn_buf, gn_len, bufA, &lenA, gn);
    if (rv) {
        log_error("[gn] decap failed");
        it2s_gn_header_free(gn);
        goto cleanup;
    }

    log_debug("[gn] decap ok | ht: %d hst: %d len: %uB", gn->ch.ht, gn->ch.hst, lenA);
    it2s_gn_header_free(gn);

    if (lenA < L_BTP_B) {
        log_error("[btp] decap failed | payload too short");
        goto cleanup;
    }

    uint16_t btp_dst_port = (bufA[0] << 8) | bufA[1];
    log_debug("[btp] decap ok | dst port: %u", btp_dst_port);

    uint8_t *its_payload = bufA + L_BTP_B;
    uint32_t its_len = lenA - L_BTP_B;

    asn_TYPE_descriptor_t *its_msg_descriptor = NULL;
    void *its_msg = NULL;

    switch (btp_dst_port) {
        case BTP_PORT_CAM: {
            its_msg_descriptor = &asn_DEF_EI2_CAM;
            its_msg = calloc(1, sizeof(EI2_CAM_t));
            break;
        }
        case BTP_PORT_DENM: {
            its_msg_descriptor = &asn_DEF_EI2_DENM;
            its_msg = calloc(1, sizeof(EI2_DENM_t));
            break;
        }
        default:
            log_warn("[btp] unsupported BTP port: %u, skipping...", btp_dst_port);
            goto cleanup;
    }

    if (!its_msg) {
        log_error("[its] allocation failed");
        goto cleanup;
    }

    asn_dec_rval_t dec =
        uper_decode_complete(NULL, its_msg_descriptor, (void **) &its_msg, its_payload, its_len);
    if (dec.code != RC_OK) {
        log_error("[its] UPER decode failed for %s (code: %d)",
                  its_msg_descriptor->name, dec.code);
        ASN_STRUCT_FREE(*its_msg_descriptor, its_msg);
        goto cleanup;
    }

    switch (btp_dst_port) {
        case BTP_PORT_CAM: {
            EI2_CAM_t *cam = (EI2_CAM_t *) its_msg;
            log_debug("[its] UPER decode ok | cam.stationId: %d", cam->header.stationId);
            cam_cb(cam, rx_hex);
            break;
        }
        case BTP_PORT_DENM: {
            EI2_DENM_t *denm = (EI2_DENM_t *) its_msg;
            log_debug("[its] UPER decode ok | denm.stationId: %d", denm->header.stationId);
            denm_cb(denm, rx_hex);
            break;
        }
    }

    ASN_STRUCT_FREE(*its_msg_descriptor, its_msg);

cleanup:
    free(rx_hex);
}

void rsock_cb(it2s_rsock_t *rsock, void *obj, unsigned char *packet, int packet_len) {
    (void) rsock;
    (void) obj;
    generic_cb(ACCESS_RSOCK, (uint8_t *) packet, (size_t) packet_len);
}

void ublox_cb(it2s_ublox_t *ublox, void *obj, uint8_t *packet, uint16_t packet_len) {
    (void) ublox;
    (void) obj;
    generic_cb(ACCESS_UBLOX, (uint8_t *) packet, (size_t) packet_len);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        log_error("missing arguments... usage: its_sniffer <rsock|ublox> <interface>");
        return 1;
    }

    load_local_env();

    char *access_technology = argv[1];
    char *interface = argv[2];
    g_interface = interface;

    ce_sender_config_t ce_cfg;
    ce_sender_config_from_env(&ce_cfg, "sniffer", interface);
    if (ce_sender_init(&ce_cfg) != 0) {
        log_warn("CloudEvent sender init failed, continuing without CloudEvents");
    }

    pthread_t rx_thread;

    if (!strcmp(access_technology, "rsock")) {
        it2s_rsock_t *rsock = it2s_rsock_init((unsigned char *) interface);
        it2s_rsock_rx_packet_callback_set(rsock, rsock_cb);
        pthread_create(&rx_thread, NULL, (void *(*)(void *)) it2s_rsock_loop, rsock);
    }

    else if (!strcmp(access_technology, "ublox")) {
        it2s_ublox_t *ublox =
            it2s_ublox_init(UBLOX_CHANNEL, UBLOX_RADIO_C, UBLOX_CHANNEL_CONFIG,
                            UBLOX_ANTENNA, (char *) UBLOX_MCS, UBLOX_POWER);
        it2s_ublox_rx_packet_callback_set(ublox, ublox_cb);
        pthread_create(&rx_thread, NULL, (void *(*)(void *)) it2s_ublox_loop, ublox);
    }

    else {
        log_error("unsupported access technology: %s", access_technology);
        ce_sender_shutdown();
        return 1;
    }

    pthread_join(rx_thread, NULL);
    ce_sender_shutdown();
    return 0;
}
