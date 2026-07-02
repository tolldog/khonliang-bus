"""The light log agent (fr_khonliang-bus_70862caa, L1 of docs/log-agent-design.md).

A tiny, separately-supervised fleet member: tails the L0 per-agent log files
the bus writes, forwards them to a pluggable substrate, and answers raw
``log_query`` requests over the normal bus WS protocol. Deliberately imports
NOTHING from ``bus.server`` — the debugging path must not share the bus's
failure modes (stdlib + ``websockets`` + ``sqlite3`` only).
"""
