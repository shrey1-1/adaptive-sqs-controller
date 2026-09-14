from __future__ import annotations

from .fixed import FixedPolicy


class FixedHeartbeatPolicy(FixedPolicy):
    name = "fixed_heartbeat"

    def __init__(
        self,
        timeout: float,
        heartbeat_interval: float,
        max_extensions: int = 3,
        max_total_visibility: float = 120.0,
    ) -> None:
        super().__init__(timeout, self.name)
        if heartbeat_interval >= timeout:
            raise ValueError("heartbeat_interval must be shorter than timeout")
        self.heartbeat_interval = float(heartbeat_interval)
        self.max_extensions = int(max_extensions)
        self.max_total_visibility = float(max_total_visibility)

