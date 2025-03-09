from typing import TypeVar, Generic, Callable, Awaitable, List, Dict
from fastapi import FastAPI, Query
from pydantic import BaseModel
import asyncio
from datetime import datetime
import time
import threading
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# Initialize FastAPI app
app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define generic type variables
DataType = TypeVar("DataType", bound=BaseModel)  # Data type is a Pydantic model
KeyType = TypeVar("KeyType")  # Key type can be any type (e.g., int, str)


# Generalized DoubleBufferCache class
class DoubleBufferCache(Generic[KeyType, DataType]):
    def __init__(
        self,
        fetch_function: Callable[[KeyType], Awaitable[DataType]],
        keys_to_refresh: List[KeyType],
        refresh_interval: int = 60,
        fetch_at_init: bool = False,
    ):
        """Initialize the double buffer cache with a custom fetch function and keys to refresh.

        Args:
            fetch_function (Callable[[KeyType], Awaitable[DataType]]): Async function to fetch data.
            keys_to_refresh (List[KeyType]): List of keys to refresh periodically.
            refresh_interval (int): Interval in seconds to refresh the cache (default: 60).
            fetch_at_init (bool): Whether to fetch data for keys at start (default: False - will start fetching after
                refresh_interval).
        """
        self.refresh_interval = refresh_interval
        self.fetch_function = fetch_function
        self.keys_to_refresh = keys_to_refresh
        self.active_cache: Dict[KeyType, DataType] = {}
        self.background_cache: Dict[KeyType, DataType] = {}
        self.last_refresh = 0
        self.lock = threading.Lock()
        self.scheduler = BackgroundScheduler(executors={"default": ThreadPoolExecutor(1)})
        self.loop = asyncio.get_event_loop()
        if fetch_at_init:
            asyncio.create_task(self._refresh_cache())
        self._start_background_refresh()

    async def _refresh_cache(self):
        """Refresh the background cache with data for the specified keys and swap with active cache."""
        try:
            new_data = {}
            combined_keys = set(self.keys_to_refresh).union(self.active_cache.keys())
            for key in combined_keys:
                logger.info(f"Fetching data for key: {key}")
                data = await self.fetch_function(key)
                logger.info(f"Data fetched for key: {key}")
                new_data[key] = data
            with self.lock:
                self.background_cache = new_data
                self.active_cache, self.background_cache = self.background_cache, self.active_cache
                self.last_refresh = time.time()
                logger.info(f"Cache refreshed at {datetime.fromtimestamp(self.last_refresh).isoformat()}")
        except Exception as e:
            logger.error(f"Error refreshing cache: {e}")

    def _start_background_refresh(self):
        """Start the background scheduler to refresh the cache periodically."""
        self.scheduler.add_job(self._run_async_refresh, "interval", seconds=self.refresh_interval)
        self.scheduler.start()

    def _run_async_refresh(self):
        """Run the async refresh task in a synchronous context."""
        asyncio.run_coroutine_threadsafe(self._refresh_cache(), self.loop)

    async def get_data(self, key: KeyType) -> DataType:
        """Retrieve data for a given key, fetching it if not in the active cache.

        Args:
            key (KeyType): The key to retrieve data for.

        Returns:
            DataType: The data associated with the key.
        """
        with self.lock:
            data = self.active_cache.get(key)
        if data is None:
            data = await self.fetch_function(key)
            with self.lock:
                self.active_cache[key] = data
                self.background_cache[key] = data  # Keep both caches in sync
                logger.info(f"New key {key} added to cache")
        return data

    async def clear_cache(self, clear_background_only: bool, fetch_after_clear: bool):
        """Clear the cache.

        Args:
            clear_background_only (bool): Whether to clear only the background cache.
            fetch_after_clear (bool): Whether to start a new request after clearing the cache.
        """
        with self.lock:
            if clear_background_only:
                self.background_cache.clear()
                logger.info("Background cache cleared")
            else:
                self.active_cache.clear()
                self.background_cache.clear()
                logger.info("Both active and background caches cleared")

        if fetch_after_clear:
            await self._refresh_cache()


# Data model 1
class DataResponse1(BaseModel):
    id: int
    value: str
    timestamp: datetime


# Data model 2
class DataResponse2(BaseModel):
    name: str
    data: float
    timestamp: datetime


# Fetch function for DataResponse1
async def _fetch_from_slow_api1(item_id: int) -> DataResponse1:
    await asyncio.sleep(2)  # Simulate slow API
    return DataResponse1(id=item_id, value=f"Data for {item_id}", timestamp=datetime.now())


# Fetch function for DataResponse2
async def _fetch_from_slow_api2(name: str) -> DataResponse2:
    await asyncio.sleep(2)  # Simulate slow API
    return DataResponse2(name=name, data=42.0, timestamp=datetime.now())


# Initialize cache instances
cache1 = DoubleBufferCache[int, DataResponse1](
    fetch_function=_fetch_from_slow_api1, keys_to_refresh=[1, 2, 3], refresh_interval=60, fetch_at_init=True
)

cache2 = DoubleBufferCache[str, DataResponse2](
    fetch_function=_fetch_from_slow_api2, keys_to_refresh=["a", "b", "c"], refresh_interval=90, fetch_at_init=False
)


# API Endpoints
@app.get("/data1/{item_id}", response_model=DataResponse1)
async def get_data1(item_id: int):
    data = await cache1.get_data(item_id)
    return data


@app.get("/data2/{name}", response_model=DataResponse2)
async def get_data2(name: str):
    data = await cache2.get_data(name)
    return data


@app.get("/cache_status")
async def get_cache_status():
    last_refresh1 = datetime.fromtimestamp(cache1.last_refresh).isoformat() if cache1.last_refresh else None
    last_refresh2 = datetime.fromtimestamp(cache2.last_refresh).isoformat() if cache2.last_refresh else None
    return {
        "cache1": {
            "last_refresh": last_refresh1,
            "active_cache_size": len(cache1.active_cache),
            "background_cache_size": len(cache1.background_cache),
        },
        "cache2": {
            "last_refresh": last_refresh2,
            "active_cache_size": len(cache2.active_cache),
            "background_cache_size": len(cache2.background_cache),
        },
    }


@app.post("/clear_cache")
async def clear_cache(
    clear_background_only: bool = Query(False, description="Clear only the background cache"),
    fetch_after_clear: bool = Query(False, description="Fetch data after clearing the cache"),
):
    await cache1.clear_cache(clear_background_only, fetch_after_clear)
    await cache2.clear_cache(clear_background_only, fetch_after_clear)
    return {"message": "Cache cleared"}
