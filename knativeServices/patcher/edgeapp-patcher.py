import json
import os
from typing import List, Dict, Any, Optional

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# --- Defaults / config ---
K8S_HOST = os.getenv("K8S_HOST", "https://kubernetes.default.svc")
TOKEN_PATH = os.getenv("TOKEN_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token")
CA_PATH = os.getenv("CA_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

EDGEAPP_GROUP = os.getenv("EDGEAPP_GROUP", "mec.atnog.org")
EDGEAPP_VERSION = os.getenv("EDGEAPP_VERSION", "v1alpha1")
EDGEAPP_PLURAL = os.getenv("EDGEAPP_PLURAL", "edgeapplications")

EDGEAPP_NAME = os.getenv("EDGEAPP_NAME", "cam-logger-operator")
EDGEAPP_NAMESPACE = os.getenv("EDGEAPP_NAMESPACE", os.getenv("POD_NAMESPACE", "default"))

TARGET_NODE_LABEL_KEY = os.getenv("TARGET_NODE_LABEL_KEY", "mec.atnog.org/rsu")
TARGET_NODE_LABEL_VALUES = os.getenv("TARGET_NODE_LABEL_VALUES", "rsu-a,rsu-b")

# These indices must match CR structure
NODESELECTORTERM_INDEX = int(os.getenv("NODESELECTORTERM_INDEX", "0"))
MATCHEXPR_INDEX = int(os.getenv("MATCHEXPR_INDEX", "0"))

# Optional extra JSONPatch operations, as a JSON array string
# Example:
#   EXTRA_JSONPATCH='[{"op":"add","path":"/spec/service/foo","value":"bar"}]'
EXTRA_JSONPATCH = os.getenv("EXTRA_JSONPATCH", "").strip()

# Set to "false" only if debugging TLS issues
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() in ("1", "true", "yes")


def read_token() -> str:
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        return f.read().strip()


def parse_values(values_csv: str) -> List[str]:
    return [v.strip() for v in values_csv.split(",") if v.strip()]


def edgeapp_url(namespace: str, name: str) -> str:
    return f"{K8S_HOST}/apis/{EDGEAPP_GROUP}/{EDGEAPP_VERSION}/namespaces/{namespace}/{EDGEAPP_PLURAL}/{name}"


def build_affinity_patch(label_key: str, rsu_values: List[str]) -> List[Dict[str, Any]]:
    base = (
        f"/spec/service/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution"
        f"/nodeSelectorTerms/{NODESELECTORTERM_INDEX}/matchExpressions/{MATCHEXPR_INDEX}"
    )

    # Use "add" for object fields; it will replace if the key already exists
    return [
        {"op": "add", "path": f"{base}/key", "value": label_key},
        {"op": "add", "path": f"{base}/operator", "value": "In"},
        {"op": "add", "path": f"{base}/values", "value": rsu_values},
    ]


def parse_extra_patch(extra: str) -> List[Dict[str, Any]]:
    if not extra:
        return []
    try:
        data = json.loads(extra)
        if not isinstance(data, list):
            raise ValueError("EXTRA_JSONPATCH must be a JSON array")
        return data
    except Exception as e:
        raise ValueError(f"Invalid EXTRA_JSONPATCH: {e}") from e


def do_jsonpatch(url: str, patch_ops: List[Dict[str, Any]]) -> requests.Response:
    token = read_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json-patch+json",
        "Accept": "application/json",
    }

    verify = CA_PATH if VERIFY_TLS else False

    return requests.patch(
        url,
        headers=headers,
        data=json.dumps(patch_ops),
        timeout=10,
        verify=verify,
    )


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.post("/replicate")
def replicate():
    """
    PATCH EdgeApplication to set required nodeAffinity to:
      TARGET_NODE_LABEL_KEY in TARGET_NODE_LABEL_VALUES

    Optional JSON body:
    {
      "edgeappName": "cam-logger-operator",
      "namespace": "default",
      "labelKey": "mec.atnog.org/rsu",
      "values": ["rsu-a", "rsu-b"],
      "extraPatch": [ ... JSONPatch ops ... ]
    }
    """
    body = request.get_json(silent=True) or {}

    name = body.get("edgeappName", EDGEAPP_NAME)
    namespace = body.get("namespace", EDGEAPP_NAMESPACE)

    label_key = body.get("labelKey", TARGET_NODE_LABEL_KEY)

    values = body.get("values")
    if values is None:
        values = parse_values(TARGET_NODE_LABEL_VALUES)
    if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
        return jsonify({"error": "`values` must be a list of strings"}), 400

    patch_ops = build_affinity_patch(label_key, values)

    # Allow extra patch ops via env or request body
    extra_ops: List[Dict[str, Any]] = []
    if EXTRA_JSONPATCH:
        try:
            extra_ops.extend(parse_extra_patch(EXTRA_JSONPATCH))
        except ValueError as e:
            return jsonify({"error": str(e)}), 500

    if "extraPatch" in body:
        if not isinstance(body["extraPatch"], list):
            return jsonify({"error": "`extraPatch` must be a JSONPatch array"}), 400
        extra_ops.extend(body["extraPatch"])

    patch_ops.extend(extra_ops)

    url = edgeapp_url(namespace, name)
    resp = do_jsonpatch(url, patch_ops)

    # Surface useful debugging info
    out = {
        "edgeapp": f"{namespace}/{name}",
        "url": url,
        "status_code": resp.status_code,
        "patch_sent": patch_ops,
    }

    try:
        out["response_json"] = resp.json()
    except Exception:
        out["response_text"] = resp.text[:2000]

    if 200 <= resp.status_code < 300:
        return jsonify(out), 200

    return jsonify(out), 500


if __name__ == "__main__":
    # Local dev only; in container we use gunicorn
    app.run(host="0.0.0.0", port=8080)
