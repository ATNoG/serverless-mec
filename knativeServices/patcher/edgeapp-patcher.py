import json
import os
from typing import List, Dict, Any

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

# Optional extra MERGE patch (dict) as JSON string.
# Example:
#   EXTRA_MERGEPATCH='{"spec":{"service":{"nodeSelector":null}}}'
EXTRA_MERGEPATCH = os.getenv("EXTRA_MERGEPATCH", "").strip()

# Set to "false" only if debugging TLS issues
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() in ("1", "true", "yes")


def read_token() -> str:
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        return f.read().strip()


def parse_values(values_csv: str) -> List[str]:
    return [v.strip() for v in values_csv.split(",") if v.strip()]


def edgeapp_url(namespace: str, name: str) -> str:
    return f"{K8S_HOST}/apis/{EDGEAPP_GROUP}/{EDGEAPP_VERSION}/namespaces/{namespace}/{EDGEAPP_PLURAL}/{name}"


def build_affinity_mergepatch(label_key: str, rsu_values: List[str]) -> Dict[str, Any]:
    """
    Build a JSON MERGE patch that creates missing parent fields automatically.
    Equivalent to:
      kubectl patch edgeapplication ... --type merge -p '{...}'
    """
    return {
        "spec": {
            "service": {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": label_key,
                                            "operator": "In",
                                            "values": rsu_values,
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }
    }


def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge dict b into a recursively (mutates and returns a).
    Lists are replaced (not merged) to keep behavior predictable.
    """
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            deep_merge(a[k], v)
        else:
            a[k] = v
    return a


def parse_extra_mergepatch(extra: str) -> Dict[str, Any]:
    if not extra:
        return {}
    try:
        data = json.loads(extra)
        if not isinstance(data, dict):
            raise ValueError("EXTRA_MERGEPATCH must be a JSON object (dict)")
        return data
    except Exception as e:
        raise ValueError(f"Invalid EXTRA_MERGEPATCH: {e}") from e


def do_mergepatch(url: str, patch_body: Dict[str, Any]) -> requests.Response:
    token = read_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/merge-patch+json",
        "Accept": "application/json",
    }

    verify = CA_PATH if VERIFY_TLS else False

    return requests.patch(
        url,
        headers=headers,
        data=json.dumps(patch_body),
        timeout=10,
        verify=verify,
    )


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.post("/replicate")
def replicate():
    """
    PATCH EdgeApplication (MERGE PATCH) to set required nodeAffinity to:
      TARGET_NODE_LABEL_KEY in TARGET_NODE_LABEL_VALUES

    This uses MERGE PATCH so it works even if:
      spec.service.affinity / nodeAffinity / nodeSelectorTerms don't exist yet.

    Optional JSON body:
    {
      "edgeappName": "cam-logger-operator",
      "namespace": "default",
      "labelKey": "mec.atnog.org/rsu",
      "values": ["rsu-a", "rsu-b"],

      // Optional: extra merge-patch object to merge into the patch
      // (ex: {"spec":{"service":{"nodeSelector":null}}})
      "extraMergePatch": { ... }
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

    patch_body: Dict[str, Any] = build_affinity_mergepatch(label_key, values)

    # Allow extra merge patch via env var
    if EXTRA_MERGEPATCH:
        try:
            deep_merge(patch_body, parse_extra_mergepatch(EXTRA_MERGEPATCH))
        except ValueError as e:
            return jsonify({"error": str(e)}), 500

    # Allow extra merge patch via request body
    if "extraMergePatch" in body:
        if not isinstance(body["extraMergePatch"], dict):
            return jsonify({"error": "`extraMergePatch` must be a JSON object (merge-patch)"}), 400
        deep_merge(patch_body, body["extraMergePatch"])

    url = edgeapp_url(namespace, name)
    resp = do_mergepatch(url, patch_body)

    out = {
        "edgeapp": f"{namespace}/{name}",
        "url": url,
        "status_code": resp.status_code,
        "patch_sent": patch_body,
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
