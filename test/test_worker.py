import json
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

# The Workers SDK requires Pyodide; stub only the runtime boundary for CPython.
sdk = ModuleType("workers")
sdk.WorkerEntrypoint = object
sdk.fetch = AsyncMock()
sdk.Response = lambda body, status=200, headers=None: SimpleNamespace(body=body, status=status, headers=headers)
with patch.dict(sys.modules, workers=sdk):
    from src import worker


class KV:
    def __init__(self):
        self.data = {}

    async def get(self, key):
        return self.data.get(key)

    async def put(self, key, value):
        self.data[key] = value


def response(dataset, values=None, timestamp="2026-10-11T13:00:00+08:00"):
    field = {"psi": "psi_twenty_four_hourly", "pm25": "pm25_one_hourly"}[dataset]
    body = {"code": 0, "data": {"items": [{
        "timestamp": timestamp, "readings": {field: values if values is not None else {"north": 42, "south": 0}},
    }]}}
    return SimpleNamespace(ok=True, json=AsyncMock(return_value=body))


async def scrape(env):
    entry = worker.Default()
    entry.env = env
    response = await entry.fetch(SimpleNamespace(url="https://example.com/metrics", method="GET"))
    return response.body


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env = SimpleNamespace(AIR_QUALITY=KV(), DATA_API_KEY="test-key")
        clock = patch.object(worker.time, "time", return_value=1791694860)
        self.clock = clock.start()
        self.addCleanup(clock.stop)

    async def test_poll_and_metrics(self):
        async def fetch(url, headers):
            self.assertEqual(headers, {"x-api-key": "test-key"})
            return response(url.rsplit("/", 1)[1])
        with patch.object(worker, "fetch", side_effect=fetch):
            await worker.poll(self.env, "psi")
            await worker.poll(self.env, "pm25")
        text = await scrape(self.env)
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', text)
        self.assertIn('nea_pm25_1_hour_micrograms_per_cubic_meter{region="south",dataset="pm25"} 0', text)
        self.assertIn('nea_poll_success{dataset="psi"} 1', text)
        self.assertIn('nea_observation_timestamp_seconds{dataset="psi"} 1791694800.0', text)
        self.assertIn('# TYPE nea_psi_24_hour gauge', text)
        # Each family starts with its declaration and contains all its samples.
        declared = set()
        family = None
        for line in text.splitlines():
            if line.startswith("# TYPE "):
                family = line.split()[2]
                self.assertNotIn(family, declared)
                declared.add(family)
            else:
                self.assertEqual(line.split("{", 1)[0], family)
        # Each scrape reflects the current stored regions.
        self.env.AIR_QUALITY.data["psi"] = json.dumps({"values": {"nea_psi_24_hour": {"east": 7}}, "observed_at": 1791694800, "poll_success": 1})
        again = await scrape(self.env)
        self.assertNotIn('nea_psi_24_hour{region="north",dataset="psi"}', again)
        self.assertIn('nea_psi_24_hour{region="east",dataset="psi"} 7', again)

    async def test_particulate_metrics(self):
        result = response("psi")
        body = await result.json()
        body["data"]["items"][0]["readings"] = {
            "psi_twenty_four_hourly": {"north": 42, "south": 0},
            "pm10_twenty_four_hourly": {
                "north": 23, "south": 0, "east": -1, "west": None, "central": True,
            },
            "pm25_twenty_four_hourly": {"north": 8.5, "south": 0},
            "pm10_sub_index": {"north": 17, "south": 0},
            "pm25_sub_index": {"north": 35, "south": 0},
        }
        result.json = AsyncMock(return_value=body)
        with patch.object(worker, "fetch", return_value=result):
            await worker.poll(self.env, "psi")
        text = await scrape(self.env)
        expected_samples = {
            'nea_psi_24_hour{region="north",dataset="psi"} 42',
            'nea_psi_24_hour{region="south",dataset="psi"} 0',
            'nea_pm10_24_hour_micrograms_per_cubic_meter{region="north",dataset="psi"} 23',
            'nea_pm10_24_hour_micrograms_per_cubic_meter{region="south",dataset="psi"} 0',
            'nea_pm25_24_hour_micrograms_per_cubic_meter{region="north",dataset="psi"} 8.5',
            'nea_pm25_24_hour_micrograms_per_cubic_meter{region="south",dataset="psi"} 0',
            'nea_pm10_sub_index{region="north",dataset="psi"} 17',
            'nea_pm10_sub_index{region="south",dataset="psi"} 0',
            'nea_pm25_sub_index{region="north",dataset="psi"} 35',
            'nea_pm25_sub_index{region="south",dataset="psi"} 0',
        }
        actual_samples = {
            line for line in text.splitlines()
            if line.startswith(("nea_psi_", "nea_pm10_", "nea_pm25_"))
        }
        self.assertEqual(actual_samples, expected_samples)
        with patch.object(worker, "fetch", return_value=SimpleNamespace(ok=False)):
            await worker.poll(self.env, "psi")
        self.assertIn('nea_pm10_sub_index{region="north",dataset="psi"} 17', await scrape(self.env))
        self.clock.return_value = 1791694800 + 3601
        self.assertNotIn('nea_pm10_sub_index{region=', await scrape(self.env))

    async def test_partial_dataset_readings(self):
        result = response("psi", {})
        body = await result.json()
        body["data"]["items"][0]["readings"]["pm10_sub_index"] = {"north": 12}
        result.json = AsyncMock(return_value=body)
        with patch.object(worker, "fetch", return_value=result):
            await worker.poll(self.env, "psi")
        text = await scrape(self.env)
        self.assertIn('nea_pm10_sub_index{region="north",dataset="psi"} 12', text)
        self.assertIn('nea_poll_success{dataset="psi"} 1', text)
        self.assertNotIn('nea_psi_24_hour{region=', text)

    async def test_failed_poll_preserves_snapshot(self):
        with patch.object(worker, "fetch", return_value=response("psi")):
            await worker.poll(self.env, "psi")
        previous = await worker.snapshot(self.env, "psi")
        with patch.object(worker, "fetch", return_value=SimpleNamespace(ok=False)):
            await worker.poll(self.env, "psi")
        current = await worker.snapshot(self.env, "psi")
        self.assertEqual(current["values"], previous["values"])
        self.assertEqual(current["last_success"], previous["last_success"])
        self.assertEqual(current["poll_success"], 0)
        text = await scrape(self.env)
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', text)
        self.assertIn('nea_poll_success{dataset="psi"} 0', text)
        self.assertIn('nea_observation_timestamp_seconds{dataset="psi"}', text)
        with patch.object(worker, "fetch", return_value=response("psi")):
            await worker.poll(self.env, "psi")
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', await scrape(self.env))

    async def test_invalid_readings_and_optional_key(self):
        del self.env.DATA_API_KEY
        mock = AsyncMock(return_value=response("psi", {"north": None, "south": True, "east": "42", "west": -1}))
        with patch.object(worker, "fetch", mock):
            await worker.poll(self.env, "psi")
        self.assertEqual(mock.call_args.kwargs["headers"], {})
        text = await scrape(self.env)
        self.assertNotIn('nea_psi_24_hour{region=', text)
        self.assertIn('nea_poll_success{dataset="psi"} 0', text)

    async def test_older_observation_does_not_replace_readings(self):
        with patch.object(worker, "fetch", return_value=response("psi")):
            await worker.poll(self.env, "psi")
        with patch.object(worker, "fetch", return_value=response("psi", {"north": 10}, "2026-10-10T13:00:00+08:00")):
            await worker.poll(self.env, "psi")
        self.assertEqual((await worker.snapshot(self.env, "psi"))["values"]["nea_psi_24_hour"]["north"], 42)

    async def test_gauges_are_omitted_only_after_failure_and_one_hour(self):
        for dataset in ("psi", "pm25"):
            with patch.object(worker, "fetch", return_value=response(dataset)):
                await worker.poll(self.env, dataset)
        with patch.object(worker, "fetch", return_value=SimpleNamespace(ok=False)):
            await worker.poll(self.env, "psi")
        self.clock.return_value = 1791694800 + 3600
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', await scrape(self.env))
        self.clock.return_value += 1
        text = await scrape(self.env)
        self.assertNotIn('nea_psi_24_hour{region=', text)
        # Equally old observations remain exposed after a successful poll.
        self.assertIn('nea_pm25_1_hour_micrograms_per_cubic_meter{region="north",dataset="pm25"} 42', text)
        self.assertIn('nea_poll_success{dataset="psi"} 0', text)
        self.assertIn('nea_observation_timestamp_seconds{dataset="psi"}', text)
        with patch.object(worker, "fetch", return_value=response("psi")):
            await worker.poll(self.env, "psi")
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', await scrape(self.env))

    async def test_scrape_uses_one_time_for_both_datasets(self):
        for dataset in ("psi", "pm25"):
            with patch.object(worker, "fetch", return_value=response(dataset)):
                await worker.poll(self.env, dataset)
        for dataset in ("psi", "pm25"):
            with patch.object(worker, "fetch", return_value=SimpleNamespace(ok=False)):
                await worker.poll(self.env, dataset)
        self.clock.side_effect = [1791694800 + 3600, 1791694800 + 3601]
        text = await scrape(self.env)
        self.assertIn('nea_psi_24_hour{region="north",dataset="psi"} 42', text)
        self.assertIn('nea_pm25_1_hour_micrograms_per_cubic_meter{region="north",dataset="pm25"} 42', text)

    async def test_schedule_and_routing(self):
        entry = worker.Default()
        entry.env = self.env
        async def fetch(url, **kwargs):
            if url.endswith("psi"):
                raise TimeoutError()
            return response("pm25")
        with patch.object(worker, "fetch", side_effect=fetch):
            await entry.scheduled(None)
        self.assertEqual((await worker.snapshot(self.env, "psi"))["poll_success"], 0)
        self.assertEqual((await worker.snapshot(self.env, "pm25"))["poll_success"], 1)
        for path, method, status in (("/", "GET", 404), ("/metrics", "POST", 405), ("/metrics", "GET", 200)):
            result = await entry.fetch(SimpleNamespace(url=f"https://example.com{path}", method=method))
            self.assertEqual(result.status, status)
