#!/usr/bin/env python3
"""Fetch parameters from an Openflow (NiFi) Parameter Provider via nipyapi.

Uses ParameterProvidersApi.fetch_parameters, then submit_apply_parameters to
update the linked parameter contexts (same as Fetch -> Apply in the UI). Prints the fetched parameter
groups and saves them to <runtime>_parameter_provider_fetch_<ts>.json.

Usage:
  python fetch_parameter_provider.py                 # fetch + apply (update) the first provider
  python fetch_parameter_provider.py --fetch-only    # fetch without applying
  python fetch_parameter_provider.py --list          # just list providers
  python fetch_parameter_provider.py --id <uuid>     # fetch a specific provider
  python fetch_parameter_provider.py --name "Openflow - Snowflake Parameter Provider"

Settings (precedence: --flag > env var / GitHub repo variable > secrets file):
  account         --account         OPENFLOW_ACCOUNT          e.g. sfseeurope-ie-demo99
  runtime_key     --runtime-key     OPENFLOW_RUNTIME_KEY      e.g. demo-100
  ingress_prefix  --ingress-prefix  OPENFLOW_INGRESS_PREFIX   optional, default of2
  token           (no flag)         OPENFLOW_PAT              the PAT (GitHub secret)
  URL = https://<ingress_prefix>--<account>.snowflakecomputing.app/<runtime_key>/nifi-api

Flags:
  --secrets  JSON file with the same keys (default: openflow_parameter_fetch.secrets.json
             next to this script; chmod 600, never commit). Optional if OPENFLOW_PAT is set.
  --url      override the built NiFi API URL
  --runtime  label used in the output filename (default: runtime key)
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

import nipyapi
from nipyapi.nifi import ParameterProvidersApi, FlowApi
from nipyapi.nifi.models import (ParameterProviderParameterApplicationEntity,
                                 ParameterProviderParameterFetchEntity, RevisionDTO)


def relax_enum_validation():
    """nipyapi 1.5's generated models hard-code enum lists that lag the Openflow
    server (e.g. reference_type STATELESS_GROUP, parameter_sensitivities keyed by
    param name) and validate unconditionally. Replace every such setter with a
    plain assignment so responses deserialize."""
    import inspect
    import nipyapi.nifi.models as models

    for cls in vars(models).values():
        if not inspect.isclass(cls):
            continue
        for attr, prop in list(vars(cls).items()):
            if not isinstance(prop, property) or prop.fset is None:
                continue
            try:
                src = inspect.getsource(prop.fset)
            except (OSError, TypeError):
                continue
            if "allowed_values" in src:
                setattr(cls, attr, property(
                    prop.fget,
                    lambda self, v, _n="_" + attr: setattr(self, _n, v),
                    prop.fdel, prop.__doc__))


DEFAULT_SECRETS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "openflow_parameter_fetch.secrets.json")


ENV_KEYS = {"token": "OPENFLOW_PAT", "account": "OPENFLOW_ACCOUNT",
            "runtime_key": "OPENFLOW_RUNTIME_KEY",
            "ingress_prefix": "OPENFLOW_INGRESS_PREFIX"}


def load_secrets(path):
    """Read settings from the local secrets file (keep it chmod 600). Env vars
    (OPENFLOW_PAT, OPENFLOW_ACCOUNT, ...) override it; if OPENFLOW_PAT is set the
    file is optional, which is how the GitHub Action runs."""
    s = {}
    if os.path.exists(path):
        if os.stat(path).st_mode & 0o077:
            print(f"Warning: {path} is readable by others; run chmod 600",
                  file=sys.stderr)
        with open(path) as f:
            s = json.load(f)
    elif not os.environ.get("OPENFLOW_PAT"):
        sys.exit(f"Secrets file not found: {path} (or set OPENFLOW_PAT)")
    for key, env in ENV_KEYS.items():
        if os.environ.get(env):
            s[key] = os.environ[env]
    if not s.get("token"):
        sys.exit("No token in secrets file or OPENFLOW_PAT")
    return s


URL_TEMPLATE = "https://{ingress_prefix}--{account}.snowflakecomputing.app/{runtime_key}/nifi-api"


def build_url(s):
    """Build the NiFi API URL from account + runtime_key (+ ingress_prefix, default of2)."""
    missing = [ENV_KEYS[k] for k in ("account", "runtime_key") if not s.get(k)]
    if missing:
        sys.exit(f"Missing settings: {', '.join(missing)} (set the env var / GitHub "
                 f"repo variable, a --flag, or the secrets file)")
    return URL_TEMPLATE.format(ingress_prefix=s.get("ingress_prefix") or "of2",
                               account=s["account"], runtime_key=s["runtime_key"])


def connect(secrets_path, url=None, overrides=None):
    s = load_secrets(secrets_path)
    s.update({k: v for k, v in (overrides or {}).items() if v})
    cfg = nipyapi.config.nifi_config
    cfg.host = (url or build_url(s)).rstrip("/")
    print(f"NiFi API: {cfg.host}")
    cfg.api_key = {"bearerAuth": s["token"]}
    cfg.api_key_prefix = {"bearerAuth": "Bearer"}
    cfg.verify_ssl = True
    cfg.client_side_validation = False
    nipyapi.config.nifi_config.api_client = None  # force a client with new config
    relax_enum_validation()


def list_providers():
    return FlowApi().get_parameter_providers().parameter_providers or []


def pick(providers, pid=None, name=None):
    for p in providers:
        if (pid and p.id == pid) or (name and p.component.name == name):
            return p
    if pid or name:
        sys.exit(f"Parameter provider not found: {pid or name}")
    if not providers:
        sys.exit("No parameter providers on this runtime.")
    return providers[0]


def fetch(provider, timeout=60):
    api = ParameterProvidersApi()
    body = ParameterProviderParameterFetchEntity(
        id=provider.id,
        revision=RevisionDTO(version=provider.revision.version,
                             client_id=provider.revision.client_id),
    )
    api.fetch_parameters(body=body, id=provider.id)
    # The fetch is applied to the provider; re-read it until groups appear.
    deadline = time.time() + timeout
    while True:
        ent = api.get_parameter_provider(provider.id)
        groups = ent.component.parameter_group_configurations or []
        if groups or time.time() > deadline:
            return ent
        time.sleep(2)


def apply(ent, timeout=300):
    """Apply the fetched parameters to the linked parameter contexts (the UI's
    'Apply' step). NiFi stops/restarts affected components itself."""
    api = ParameterProvidersApi()
    body = ParameterProviderParameterApplicationEntity(
        id=ent.id,
        revision=RevisionDTO(version=ent.revision.version,
                             client_id=ent.revision.client_id),
        parameter_group_configurations=ent.component.parameter_group_configurations,
    )
    req = api.submit_apply_parameters(body=body, provider_id=ent.id).request
    deadline = time.time() + timeout
    try:
        while not req.complete:
            if time.time() > deadline:
                raise TimeoutError(f"Apply request {req.request_id} still running")
            time.sleep(2)
            req = api.get_parameter_provider_apply_parameters_request(
                ent.id, req.request_id).request
            print(f"  {req.percent_completed}% {req.state}")
    finally:
        api.delete_apply_parameters_request(ent.id, req.request_id)
    if req.failure_reason:
        raise RuntimeError(f"Apply failed: {req.failure_reason}")
    return req


def to_dict(ent):
    c = ent.component
    return {
        "provider_id": ent.id,
        "name": c.name,
        "type": c.type,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "parameter_groups": [
            {
                "group_name": g.group_name,
                "parameter_context_name": g.parameter_context_name,
                "synchronized": g.synchronized,
                "sensitivity": g.parameter_sensitivities,
            }
            for g in (c.parameter_group_configurations or [])
        ],
        "referencing_parameter_contexts": [
            r.component.name for r in (c.referencing_parameter_contexts or [])
            if r.component
        ],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--secrets", default=DEFAULT_SECRETS)
    ap.add_argument("--url")
    ap.add_argument("--account", help="overrides OPENFLOW_ACCOUNT / secrets file")
    ap.add_argument("--runtime-key", help="overrides OPENFLOW_RUNTIME_KEY / secrets file")
    ap.add_argument("--ingress-prefix", help="overrides OPENFLOW_INGRESS_PREFIX / secrets file")
    ap.add_argument("--runtime", help="output filename label (default: runtime key)")
    ap.add_argument("--id")
    ap.add_argument("--name")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--fetch-only", action="store_true",
                    help="fetch but do not apply to parameter contexts")
    a = ap.parse_args()

    connect(a.secrets, a.url, {"account": a.account, "runtime_key": a.runtime_key,
                               "ingress_prefix": a.ingress_prefix})
    label = a.runtime or nipyapi.config.nifi_config.host.rstrip("/").split("/")[-2]
    try:
        providers = list_providers()
    except Exception as e:  # nipyapi wraps ApiException in __cause__
        cause = e.__cause__ or e
        sys.exit(f"Failed to reach NiFi API: {getattr(cause, 'status', '')} {cause}")

    for p in providers:
        print(f"{p.id}  {p.component.name}  ({p.component.type.split('.')[-1]})")
    if a.list:
        return

    prov = pick(providers, a.id, a.name)
    print(f"\nFetching parameters from: {prov.component.name}")
    ent = fetch(prov)
    result = to_dict(ent)

    for g in result["parameter_groups"]:
        print(f"\n[{g['group_name']}] -> context: {g['parameter_context_name']} "
              f"(synced={g['synchronized']})")
        for k, v in sorted((g["sensitivity"] or {}).items()):
            print(f"  {k}: {v}")

    if not a.fetch_only:
        print("\nApplying fetched parameters to parameter contexts...")
        req = apply(ent)
        updated = [u.parameter_context.name
                   for u in (req.parameter_context_updates or [])
                   if u.parameter_context]
        result["applied_at"] = datetime.now().isoformat(timespec="seconds")
        result["updated_parameter_contexts"] = updated
        print(f"Applied. Updated contexts: {', '.join(updated) or '(none)'}")

    out = f"{label}_parameter_provider_fetch_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()
