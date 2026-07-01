"""
worker.py — Priority Queue Worker com Sleep Dinâmico
=====================================================
Fluxo:
  1. Na inicialização → lê o banco e monta a fila ordenada por execute_at
  2. Loop principal  → calcula sleep até o próximo evento, dorme
  3. Acorda          → dispara o webhook via HTTP POST (httpx)
  4. daily=True      → reagenda para +24h
  5. API faz POST    → chama notify_new_schedule() para acordar o worker
"""

import asyncio
import json
import heapq
import httpx
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from database import get_connection

load_dotenv()

# ---------------------------------------------------------------------------
# Timezone — lido do .env
# Aceita nome IANA:   TIMEZONE=America/Sao_Paulo
# Aceita offset horas: TIMEZONE=-3  ou  TIMEZONE=+5.5
# ---------------------------------------------------------------------------

def _load_timezone() -> timezone | ZoneInfo:
    tz_env = os.getenv("TIMEZONE", "UTC").strip()
    try:
        offset_hours = float(tz_env)
        return timezone(timedelta(hours=offset_hours))
    except ValueError:
        return ZoneInfo(tz_env)

LOCAL_TZ = _load_timezone()


def _now() -> datetime:
    """Retorna o datetime atual sempre com timezone."""
    return datetime.now(tz=LOCAL_TZ)


def _aware(dt: datetime) -> datetime:
    """
    Garante que um datetime do banco venha com timezone.
    Se já vier com tz → converte para LOCAL_TZ.
    Se vier naive (sem tz) → assume que está em LOCAL_TZ.
    """
    if dt is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)


# ---------------------------------------------------------------------------
# Evento global: a API chama notify_new_schedule() após cada POST bem-sucedido
# ---------------------------------------------------------------------------
_wakeup_event = asyncio.Event()


def notify_new_schedule():
    """
    Chamado pela API (main.py) sempre que um novo schedule é criado.
    Acorda o worker para que ele recalcule o próximo sleep_time.
    """
    _wakeup_event.set()


# ---------------------------------------------------------------------------
# Estrutura interna da fila
# ---------------------------------------------------------------------------
_queue: list[tuple[datetime, int, dict]] = []


def _load_from_db() -> None:
    global _queue
    _queue = []

    now = _now()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, execute_at, webhook_url, payload, daily
                FROM webhooks_schedule
            """)
            rows = cur.fetchall()

    for row in rows:
        db_id, name, execute_at, webhook_url, payload, daily = row

        execute_at = _aware(execute_at)  # ← garante timezone consistente

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}

        item = {
            "id":          db_id,
            "name":        name,
            "execute_at":  execute_at,
            "webhook_url": webhook_url,
            "payload":     payload,
            "daily":       daily,
        }

        if execute_at <= now:
            if daily:
                item["execute_at"] = _next_daily(execute_at, now)
            else:
                print(f"[WORKER] ⚠️  Schedule '{name}' (id={db_id}) expirado — ignorado.")
                continue

        heapq.heappush(_queue, (item["execute_at"], db_id, item))

    print(f"[WORKER] 🔄 Fila reconstruída com {len(_queue)} item(s).")
    _print_queue_state()


def _next_daily(execute_at: datetime, now: datetime) -> datetime:
    next_dt = execute_at
    while next_dt <= now:
        next_dt += timedelta(days=1)
    return next_dt


def _print_queue_state() -> None:
    if not _queue:
        print("[WORKER] 📭 Fila vazia.")
        return
    print("[WORKER] 📋 Estado da fila:")
    for i, (dt, db_id, item) in enumerate(sorted(_queue)):
        delta = dt - _now()
        total_secs = int(delta.total_seconds())
        print(f"  [{i+1}] id={db_id} | '{item['name']}' | em {dt} (~{total_secs}s)")


# ---------------------------------------------------------------------------
# Disparo real do webhook via httpx
# ---------------------------------------------------------------------------

async def _fire_webhook(item: dict) -> None:
    print("\n" + "=" * 60)
    print(f"[WORKER] 🚀 DISPARANDO WEBHOOK — {_now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Schedule : '{item['name']}' (id={item['id']})")
    print(f"  URL      : {item['webhook_url']}")
    print(f"  Payload  : {json.dumps(item['payload'], indent=4, ensure_ascii=False)}")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                item["webhook_url"],
                json=item["payload"],
                headers={"Content-Type": "application/json"}
            )
        print(f"  Resposta : {response.status_code} {response.reason_phrase}")

    except httpx.TimeoutException:
        print(f"  ❌ Erro   : Timeout — o webhook não respondeu em 30s")

    except httpx.RequestError as e:
        print(f"  ❌ Erro   : {type(e).__name__} — {e}")

    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Loop principal do worker
# ---------------------------------------------------------------------------

async def run_worker() -> None:
    print(f"[WORKER] ✅ Worker iniciado. Timezone: {LOCAL_TZ}")
    _load_from_db()

    while True:
        _wakeup_event.clear()

        if not _queue:
            print("[WORKER] 💤 Fila vazia. Aguardando novos schedules...")
            await _wakeup_event.wait()
            print("[WORKER] 🔔 Acordado! Reconstruindo fila...")
            _load_from_db()
            continue

        next_dt, next_id, next_item = _queue[0]
        now = _now()
        sleep_secs = (next_dt - now).total_seconds()

        if sleep_secs <= 0:
            heapq.heappop(_queue)
            await _fire_webhook(next_item)

            if next_item["daily"]:
                new_dt = _next_daily(next_dt, _now())
                next_item["execute_at"] = new_dt
                heapq.heappush(_queue, (new_dt, next_id, next_item))
                print(f"[WORKER] 🔁 '{next_item['name']}' reagendado para {new_dt}")

            _print_queue_state()
            continue

        print(f"[WORKER] ⏳ Próximo: '{next_item['name']}' em {next_dt} (sleeping {sleep_secs:.1f}s)")

        try:
            await asyncio.wait_for(_wakeup_event.wait(), timeout=sleep_secs)
            print("[WORKER] 🔔 Novo schedule detectado! Reconstruindo fila...")
            _load_from_db()

        except asyncio.TimeoutError:
            heapq.heappop(_queue)
            await _fire_webhook(next_item)

            if next_item["daily"]:
                new_dt = _next_daily(next_dt, _now())
                next_item["execute_at"] = new_dt
                heapq.heappush(_queue, (new_dt, next_id, next_item))
                print(f"[WORKER] 🔁 '{next_item['name']}' reagendado para {new_dt}")

            _print_queue_state()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_worker())