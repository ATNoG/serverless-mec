"""
Normalize ITS GeoNetworking captures to Ethernet 0x8947.

- If a packet contains LLC/SNAP with etype 0x8947 (typical for 802.11+radiotap captures),
  extract the SNAP payload (GeoNetworking PDU) and wrap it in an Ethernet frame
  with EtherType 0x8947.

- If a packet is already Ethernet with eth.type 0x8947, pass it through as-is.

- All other packets are skipped (e.g., non-ITS or UDP/2001 files; for those,
  test them directly with a UDP BPF instead of converting).

Output is a pcap (DLT_EN10MB).
"""
import sys
from scapy.all import Ether, Raw
from scapy.layers.l2 import LLC, SNAP
from scapy.utils import PcapReader, PcapWriter

ETHERTYPE_GEONET = 0x8947

def normalize(in_path: str, out_path: str):
    converted = passthrough = skipped = 0
    with PcapReader(in_path) as reader, PcapWriter(out_path, linktype=1, sync=True) as writer:
        for pkt in reader:
            wrote = False

            # 1) Already Ethernet? Pass through if eth.type==0x8947
            if pkt.haslayer(Ether):
                try:
                    eth = pkt[Ether]
                    if getattr(eth, "type", None) == ETHERTYPE_GEONET:
                        # preserve original packet (strip any non-ether stuff scapy might have added)
                        out = Ether(bytes(eth))
                        if hasattr(pkt, "time"):
                            out.time = pkt.time
                        writer.write(out)
                        passthrough += 1
                        wrote = True
                except Exception:
                    pass

            # 2) 802.11+radiotap with LLC/SNAP carrying 0x8947? Re-wrap
            if not wrote and pkt.haslayer(SNAP):
                snap = pkt[SNAP]
                et = getattr(snap, "code", None) or getattr(snap, "etype", None)
                if et == ETHERTYPE_GEONET:
                    payload = bytes(snap.payload)  # GeoNetworking PDU bytes
                    out = Ether(src="02:00:00:00:00:01",
                                dst="02:00:00:00:00:02",
                                type=ETHERTYPE_GEONET) / Raw(payload)
                    if hasattr(pkt, "time"):
                        out.time = pkt.time
                    writer.write(out)
                    converted += 1
                    wrote = True

            if not wrote:
                skipped += 1

    print(f"normalized -> {out_path}")
    print(f"  converted_from_80211={converted}")
    print(f"  passthrough_ethernet_8947={passthrough}")
    print(f"  skipped_other={skipped}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: normalize_to_eth8947.py <in.pcap|pcapng> <out.pcap>", file=sys.stderr)
        sys.exit(1)
    normalize(sys.argv[1], sys.argv[2])
