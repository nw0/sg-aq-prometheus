import asyncio
import json
import math
import time
from datetime import datetime
from urllib.parse import urlsplit

from workers import Response, WorkerEntrypoint, fetch

DATASETS = {
    "psi": ("psi_twenty_four_hourly", "nea_psi_24_hour"),
    "pm25": ("pm25_one_hourly", "nea_pm25_1_hour_micrograms_per_cubic_meter"),
}
STATUS = {
    "poll_success": "nea_poll_success",
    "last_success": "nea_last_successful_poll_timestamp_seconds",
    "observed_at": "nea_observation_timestamp_seconds",
}
REGIONS = ("north", "south", "east", "west", "central", "national")


async def snapshot(env, dataset):
    return json.loads(await env.AIR_QUALITY.get(dataset) or "{}")


async def poll(env, dataset):
    current = await snapshot(env, dataset)
    try:
        key = getattr(env, "DATA_API_KEY", None)
        async with asyncio.timeout(10):
            response = await fetch(
                f"https://api-open.data.gov.sg/v2/real-time/api/{dataset}",
                headers={"x-api-key": key} if key else {},
            )
            if not response.ok:
                raise ValueError("HTTP failure")
            body = await response.json()
        if body["code"] != 0:
            raise ValueError("API failure")
        latest = max(body["data"]["items"], key=lambda item: item["timestamp"])
        observed_at = datetime.fromisoformat(latest["timestamp"]).timestamp()
        readings = latest["readings"][DATASETS[dataset][0]]
        values = {}
        for region in REGIONS:
            value = readings.get(region)
            if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                values[region] = value
        if not values:
            raise ValueError("Missing readings")
        if observed_at >= current.get("observed_at", 0):
            current.update(values=values, observed_at=observed_at)
        current.update(poll_success=1, last_success=time.time())
    except Exception as exc:
        print(f"Failed to poll {dataset}: {exc!r}")
        current["poll_success"] = 0
    await env.AIR_QUALITY.put(dataset, json.dumps(current))


class Default(WorkerEntrypoint):
    async def scheduled(self, controller, *args):
        await asyncio.gather(*(poll(self.env, dataset) for dataset in DATASETS))

    async def fetch(self, request):
        if urlsplit(request.url).path != "/metrics":
            return Response("Not found\n", status=404)
        if request.method != "GET":
            return Response("Method not allowed\n", status=405, headers={"Allow": "GET"})
        names = [name for _, name in DATASETS.values()] + list(STATUS.values())
        samples = {name: [] for name in names}
        now = time.time()
        for dataset, (_, name) in DATASETS.items():
            current = await snapshot(self.env, dataset)
            age = now - current.get("observed_at", 0)
            if current.get("poll_success") or age <= 3600:
                for region, value in current.get("values", {}).items():
                    samples[name].append(f'{name}{{region="{region}"}} {value}')
            current.setdefault("poll_success", 0)
            for field, metric in STATUS.items():
                if field in current:
                    samples[metric].append(f'{metric}{{dataset="{dataset}"}} {current[field]}')
        lines = []
        for name, values in samples.items():
            lines.append(f"# TYPE {name} gauge")
            lines.extend(values)
        return Response("\n".join(lines) + "\n", headers={
            "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
            "Cache-Control": "no-store",
        })
