# Double Buffer Cache with FastAPI

This is a demo to showcase double buffer cache using FastAPI. Use for use-cases where you need to periodically refresh cached data from slow APIs while ensuring minimal disruption to active cache usage.

## Features

- Double buffer cache implementation
- Periodic cache refresh using APScheduler
- Fetch data from slow APIs
- Clear cache via API endpoint
- Supports multiple data models and fetch functions

> The current implementation uses in-memory caching with a dictionary. For more robust use cases, consider using Redis or a similar solution.

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/emenems/fast-cache-demo.git
    cd fast-cache-demo
    ```

2. Create a virtual environment and activate it:
    ```sh
    python -m venv env
    source env/bin/activate  # On Windows use `env\Scripts\activate`
    ```

3. Install the dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Run the FastAPI application:
    ```sh
    uvicorn main:app --reload
    ```

2. Access the API documentation at `http://127.0.0.1:8000/docs`.

## API Endpoints

- `GET /data1/{item_id}`: Retrieve data for a given `item_id` using the first cache.
- `GET /data2/{name}`: Retrieve data for a given `name` using the second cache.
- `GET /cache_status`: Get the status of both caches.
- `POST /clear_cache`: Clear the cache with options to clear only the background cache or both caches, and to start a new request after clearing the cache.

