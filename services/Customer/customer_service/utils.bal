import ballerina/time;

function nowIso() returns string => time:utcToString(time:utcNow());

function parseEvent(byte[] raw) returns json|error {
    string s = check string:fromBytes(raw);
    return s.fromJsonString();
}

function getString(json payload, string key) returns string|error {
    map<json> m = check payload.ensureType();
    json v = m[key];
    return v.ensureType();
}

function getOptString(json payload, string key) returns string? {
    map<json>|error m = payload.ensureType();
    if m is error {
        return ();
    }
    json v = m[key];
    return v is string ? v : ();
}

function getOptFloat(json payload, string key) returns float {
    map<json>|error m = payload.ensureType();
    if m is error {
        return 0.0;
    }
    float|error f = m[key].cloneWithType();
    return f is float ? f : 0.0;
}

// Stable id used for idempotency (falls back when the event has no eventId)
function eventKey(string topic, json event) returns string {
    string? id = getOptString(event, "eventId");
    if id is string {
        return id;
    }
    return topic + ":" + (getOptString(event, "orderId") ?: "") + ":"
        + (getOptString(event, "newStatus") ?: getOptString(event, "status") ?: "");
}
