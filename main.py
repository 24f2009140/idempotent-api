
import math
import secrets
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI()

TOTAL_ORDERS = 55
RATE_LIMIT = 18
WINDOW_SECONDS = 10

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Idempotency storage
idempotency_store = {}
idempotency_lock = Lock()

# Independent rate-limit bucket per client
client_requests = defaultdict(deque)
rate_lock = Lock()

# Fixed catalog, IDs 1 through 55
catalog = [
    {
        "id": i,
        "status": "completed" if i % 3 == 0 else "pending",
        "amount": round(19.99 + i * 2.5, 2),
    }
    for i in range(1, TOTAL_ORDERS + 1)
]


class OrderCreate(BaseModel):
    product: str = "standard"
    quantity: int = Field(default=1, ge=1, le=100)


def check_rate_limit(client_id: str):
    now = time.monotonic()

    with rate_lock:
        bucket = client_requests[client_id]

        while bucket and now - bucket[0] >= WINDOW_SECONDS:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1,
                math.ceil(WINDOW_SECONDS - (now - bucket[0])),
            )
            return retry_after

        bucket.append(now)
        return None



@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Always let browser CORS preflight requests through.
    if request.method == "OPTIONS":
        return await call_next(request)

    if request.url.path == "/healthz":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    retry_after = check_rate_limit(client_id)

    if retry_after is not None:
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded"},
            headers={
                "Retry-After": str(retry_after),
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "*",
            },
        )

    return await call_next(request)


@app.post("/orders", status_code=201)
def create_order(
    body: OrderCreate = OrderCreate(),
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
):
    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required",
        )

    with idempotency_lock:
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]

        order = {
            "id": secrets.token_hex(8),
            "product": body.product,
            "quantity": body.quantity,
            "status": "created",
        }

        idempotency_store[idempotency_key] = order
        return order


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=55),
    cursor: str | None = None,
):
    # Cursor is the ID of the last item returned on the previous page.
    start_id = 1

    if cursor:
        try:
            start_id = int(cursor) + 1
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    if start_id < 1 or start_id > TOTAL_ORDERS + 1:
        raise HTTPException(status_code=400, detail="Invalid cursor")

    items = catalog[start_id - 1 : start_id - 1 + limit]

    if items and items[-1]["id"] < TOTAL_ORDERS:
        next_cursor = str(items[-1]["id"])
    else:
        next_cursor = None

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
