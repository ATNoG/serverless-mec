/* BTP-B destination ports */
#define BTP_PORT_CAM  2001
#define BTP_PORT_DENM 2002

/* BTP-B header length: dst port (2B) + dst port info (2B) */
#define L_BTP_B 4

/* Ublox radio configurations */
#define UBLOX_CHANNEL 180
#define UBLOX_RADIO_C 'a'
#define UBLOX_CHANNEL_CONFIG 0
#define UBLOX_ANTENNA 3
#define UBLOX_MCS "MK2MCS_R12QPSK"
#define UBLOX_POWER 46

/* Access options */
#define ACCESS_UBLOX 0
#define ACCESS_RSOCK 1
