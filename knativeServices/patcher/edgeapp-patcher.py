import json
import os
from typing import Any, Dict, List, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# --- Cluster access (in-cluster defaults) ---
K8S_HOST = os.getenv("K8S_HOST", "https://kubernetes.default.svc")
TOKEN_PATH = os.getenv("TOKEN_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token")
CA_PATH = os.getenv("CA_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

# --- API group/version for EdgeApplication (CRD) ---
EDGEAPP_GROUP = os.getenv("EDGEAPP_GROUP", "mec.atnog.org")
EDGEAPP_VERSION = os.getenv("EDGEAPP_VERSION", "v1alpha1")
EDGEAPP_PLURAL = os.getenv("EDGEAPP_PLURAL", "edgeapplications")

# --- Knative Serving API ---
KSVC_GROUP = "serving.knative.dev"
KSVC_VERSION = "v1"
KSVC_PLURAL = "services"  # ksvc is "Service" under serving.knative.dev

# --- TLS verify ---
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() in ("1", "true", "yes")


def read_token() -> str:
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        return f.read().strip()


def k8s_verify() -> Any:
    # requests.verify can be: True/False/CA-bundle path
    return CA_PATH if VERIFY_TLS else False


def edgeapp_url(namespace: str, name: str) -> str:
    return (
        f"{K8S_HOST}/apis/{EDGEAPP_GROUP}/{EDGEAPP_VERSION}"
        f"/namespaces/{namespace}/{EDGEAPP_PLURAL}/{name}"
    )


def ksvc_url(namespace: str, name: str) -> str:
    return (
        f"{K8S_HOST}/apis/{KSVC_GROUP}/{KSVC_VERSION}"
        f"/namespaces/{namespace}/{KSVC_PLURAL}/{name}"
    )


def http_get(url: str) -> requests.Response:
    token = read_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    return requests.get(url, headers=headers, timeout=10, verify=k8s_verify())


def http_merge_patch(url: str, patch_obj: Dict[str, Any]) -> requests.Response:
    token = read_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/merge-patch+json",
    }
    return requests.patch(url, headers=headers, data=json.dumps(patch_obj), timeout=10, verify=k8s_verify())


def build_required_node_affinity(label_key: str, values: List[str]) -> Dict[str, Any]:
    """
    Builds:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: <label_key>
                operator: In
                values: [...]
    """
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": label_key,
                                "operator": "In",
                                "values": values,
                            }
                        ]
                    }
                ]
            }
        }
    }


def safe_int(s: Any, default: int = 0) -> Tuple[int, bool]:
    """
    Returns (value, ok). ok=False if parsing failed and default used.
    """
    if s is None:
        return default, True
    try:
        return int(str(s).strip()), True
    except Exception:
        return default, False


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.post("/replicate")
def replicate():
    """
    Body example:
    {
      "namespace": "default",

      "edgeappName": "cam-logger-operator",

      "ksvcName": "cam-logger-operator",
      "ksvcNamespace": "default",   // optional; defaults to namespace

      "affinity": {
        "key": "mec.atnog.org/rsu",
        "values": ["rsu-a", "rsu-b"]
      },

      "nodeSelector": {
        "repeater": "true"
      },

      "minScaleIncrement": 1
    }
    """
    body = request.get_json(silent=True) or {}

    namespace = body.get("namespace")
    edgeapp_name = body.get("edgeappName")
    ksvc_name = body.get("ksvcName")

    if not namespace or not isinstance(namespace, str):
        return jsonify({"error": "`namespace` (string) is required"}), 400
    if not edgeapp_name or not isinstance(edgeapp_name, str):
        return jsonify({"error": "`edgeappName` (string) is required"}), 400
    if not ksvc_name or not isinstance(ksvc_name, str):
        return jsonify({"error": "`ksvcName` (string) is required"}), 400

    ksvc_namespace = body.get("ksvcNamespace", namespace)
    if not isinstance(ksvc_namespace, str) or not ksvc_namespace:
        return jsonify({"error": "`ksvcNamespace` must be a non-empty string if provided"}), 400

    # ---- Parse affinity input (optional) ----
    affinity_patch: Dict[str, Any] = {}
    if "affinity" in body and body["affinity"] is not None:
        affinity = body["affinity"]
        if not isinstance(affinity, dict):
            return jsonify({"error": "`affinity` must be an object"}), 400

        key = affinity.get("key")
        values = affinity.get("values")

        if not isinstance(key, str) or not key:
            return jsonify({"error": "`affinity.key` must be a non-empty string"}), 400
        if not isinstance(values, list) or not all(isinstance(v, str) and v for v in values):
            return jsonify({"error": "`affinity.values` must be a list of non-empty strings"}), 400

        affinity_patch = build_required_node_affinity(key, values)

    # ---- Parse nodeSelector input (optional) ----
    node_selector_patch: Dict[str, str] = {}
    if "nodeSelector" in body and body["nodeSelector"] is not None:
        node_selector = body["nodeSelector"]
        if not isinstance(node_selector, dict):
            return jsonify({"error": "`nodeSelector` must be an object (map of string:string)"}), 400

        # Ensure all keys/values are strings (K8s nodeSelector requires string values)
        for k, v in node_selector.items():
            if not isinstance(k, str) or not k:
                return jsonify({"error": "`nodeSelector` keys must be non-empty strings"}), 400
            if not isinstance(v, str):
                return jsonify({"error": "`nodeSelector` values must be strings"}), 400
        node_selector_patch = node_selector

    # ---- Parse minScaleIncrement (optional) ----
    inc_raw = body.get("minScaleIncrement", 0)
    inc, inc_ok = safe_int(inc_raw, default=0)
    if not inc_ok:
        return jsonify({"error": "`minScaleIncrement` must be an integer"}), 400
    if inc < 0:
        return jsonify({"error": "`minScaleIncrement` must be >= 0"}), 400

    # =========================
    # 1) Patch EdgeApplication
    # =========================
    edgeapp_patch_obj: Dict[str, Any] = {"spec": {"service": {}}}
    if affinity_patch:
        edgeapp_patch_obj["spec"]["service"]["affinity"] = affinity_patch
    if node_selector_patch:
        edgeapp_patch_obj["spec"]["service"]["nodeSelector"] = node_selector_patch

    edgeapp_resp = None
    edgeapp_url_str = edgeapp_url(namespace, edgeapp_name)

    if edgeapp_patch_obj["spec"]["service"]:
        edgeapp_resp = http_merge_patch(edgeapp_url_str, edgeapp_patch_obj)

    # =========================
    # 2) Read + bump KService minScale
    # =========================
    ksvc_url_str = ksvc_url(ksvc_namespace, ksvc_name)

    # GET current ksvc
    ksvc_get_resp = http_get(ksvc_url_str)
    ksvc_obj: Dict[str, Any] = {}
    ksvc_get_json_ok = True
    try:
        ksvc_obj = ksvc_get_resp.json()
    except Exception:
        ksvc_get_json_ok = False

    if not (200 <= ksvc_get_resp.status_code < 300):
        return jsonify(
            {
                "error": "Failed to GET KService",
                "ksvc": f"{ksvc_namespace}/{ksvc_name}",
                "url": ksvc_url_str,
                "status_code": ksvc_get_resp.status_code,
                "response_json": ksvc_obj if ksvc_get_json_ok else None,
                "response_text": None if ksvc_get_json_ok else (ksvc_get_resp.text[:2000] if ksvc_get_resp.text else ""),
                "edgeapp_patch_attempted": edgeapp_resp is not None,
                "edgeapp_patch_status": edgeapp_resp.status_code if edgeapp_resp is not None else None,
            }
        ), 500

    # Extract current minScale annotation
    annotations = (
        ksvc_obj.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("annotations", {})
    )
    current_min_raw = annotations.get("autoscaling.knative.dev/minScale")
    current_min, current_ok = safe_int(current_min_raw, default=0)

    new_min = current_min + inc if inc > 0 else current_min

    ksvc_patch_resp = None
    ksvc_patch_obj: Dict[str, Any] = {}

    if inc > 0:
        # Merge patch to set annotation
        ksvc_patch_obj = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "autoscaling.knative.dev/minScale": str(new_min)
                        }
                    }
                }
            }
        }
        ksvc_patch_resp = http_merge_patch(ksvc_url_str, ksvc_patch_obj)

    # Build output
    out: Dict[str, Any] = {
        "edgeapp": f"{namespace}/{edgeapp_name}",
        "edgeapp_url": edgeapp_url_str,
        "edgeapp_patch_sent": edgeapp_patch_obj if edgeapp_resp is not None else None,
        "edgeapp_patch_status": edgeapp_resp.status_code if edgeapp_resp is not None else None,

        "ksvc": f"{ksvc_namespace}/{ksvc_name}",
        "ksvc_url": ksvc_url_str,
        "minScale": {
            "before": current_min,
            "before_raw": current_min_raw,
            "before_parse_ok": current_ok,
            "increment": inc,
            "after": new_min,
        },
        "ksvc_patch_sent": ksvc_patch_obj if ksvc_patch_resp is not None else None,
        "ksvc_patch_status": ksvc_patch_resp.status_code if ksvc_patch_resp is not None else None,
    }

    # Attach responses (trimmed)
    if edgeapp_resp is not None:
        try:
            out["edgeapp_response_json"] = edgeapp_resp.json()
        except Exception:
            out["edgeapp_response_text"] = (edgeapp_resp.text[:2000] if edgeapp_resp.text else "")

    if ksvc_patch_resp is not None:
        try:
            out["ksvc_patch_response_json"] = ksvc_patch_resp.json()
        except Exception:
            out["ksvc_patch_response_text"] = (ksvc_patch_resp.text[:2000] if ksvc_patch_resp.text else "")

    # If any patch failed, return 500 to make it obvious
    if edgeapp_resp is not None and not (200 <= edgeapp_resp.status_code < 300):
        return jsonify(out), 500
    if ksvc_patch_resp is not None and not (200 <= ksvc_patch_resp.status_code < 300):
        return jsonify(out), 500

    return jsonify(out), 200


if __name__ == "__main__":
    # Local dev only; in container we use gunicorn
    app.run(host="0.0.0.0", port=8080)
