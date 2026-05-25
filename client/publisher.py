"""
publisher.py — Greengrass discovery + mTLS publish to core's Moquette.

Adapted from aws-iot-device-sdk-python-v2 basic_discovery sample.
Publishes a single message to a topic on the discovered core, then disconnects.
"""

import base64
import json
import os
import uuid

from awscrt import io, mqtt
from awscrt.exceptions import AwsCrtError
from awsiot.greengrass_discovery import DiscoveryClient
from awsiot import mqtt_connection_builder


def publish_asc(asc_path, thing_name, topic, ca_file, cert, key, region="us-east-1"):
    tls_options = io.TlsContextOptions.create_client_with_mtls_from_path(cert, key)
    tls_options.override_default_trust_store_from_path(None, ca_file)
    tls_context = io.ClientTlsContext(tls_options)
    socket_options = io.SocketOptions()

    discovery = DiscoveryClient(
        io.ClientBootstrap.get_or_create_static_default(),
        socket_options, tls_context, region,
    )
    resp = discovery.discover(thing_name).result()

    with open(asc_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    payload = json.dumps({
        "encoded": encoded,
        "filename": os.path.basename(asc_path),
        "request_id": str(uuid.uuid4()),
    })

    last_err = None
    for group in resp.gg_groups:
        for core in group.cores:
            for conn in core.connectivity:
                try:
                    mqtt_conn = mqtt_connection_builder.mtls_from_path(
                        endpoint=conn.host_address,
                        port=conn.port,
                        cert_filepath=cert,
                        pri_key_filepath=key,
                        ca_bytes=group.certificate_authorities[0].encode("utf-8"),
                        client_id=f"{thing_name}-pub-{uuid.uuid4().hex[:8]}",
                        clean_session=True,
                        keep_alive_secs=30,
                    )
                    mqtt_conn.connect().result()
                    pub_future, _ = mqtt_conn.publish(
                        topic=topic, payload=payload,
                        qos=mqtt.QoS.AT_LEAST_ONCE)
                    pub_future.result()
                    mqtt_conn.disconnect().result()
                    print(f"[publisher] published {os.path.basename(asc_path)} "
                          f"({len(payload)} bytes) → {conn.host_address}:{conn.port} / {topic}")
                    return True
                except (AwsCrtError, Exception) as e:
                    last_err = e
                    continue

    raise RuntimeError(f"All core connection attempts failed: {last_err}")
