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
from datetime import datetime, timedelta
from database import get_connection


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
# Estrutura interna da fila — guardamos tuplas para o heapq
# heapq é um min-heap: o menor execute_at fica sempre no topo (índice 0)
# ---------------------------------------------------------------------------
# Formato do item na heap: (execute_at: datetime, id: int, row: dict)

_queue: list[tuple[datetime, int, dict]] = []


def _load_from_db() -> None:
    """
    Lê todos os schedules do banco e reconstrói a fila.
    Schedules com execute_at no passado:
      - daily=True  → reagenda para o próximo ciclo (hoje + 1 dia ou mais)
      - daily=False → ignora (já expirou)
    """
    global _queue
    _queue = []

    now = datetime.now()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, execute_at, webhook_url, payload, daily
                FROM webhooks_schedule
            """)
            rows = cur.fetchall()

    for row in rows:
        db_id, name, execute_at, webhook_url, payload, daily = row

        # Normaliza payload para dict
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

        # Trata schedules no passado
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
    """
    Calcula o próximo horário para um schedule diário.
    Avança de 1 em 1 dia até ficar no futuro.
    """
    next_dt = execute_at
    while next_dt <= now:
        next_dt += timedelta(days=1)
    return next_dt


def _print_queue_state() -> None:
    """Imprime o estado atual da fila de forma legível."""
    if not _queue:
        print("[WORKER] 📭 Fila vazia.")
        return
    print("[WORKER] 📋 Estado da fila:")
    for i, (dt, db_id, item) in enumerate(sorted(_queue)):
        delta = dt - datetime.now()
        total_secs = int(delta.total_seconds())
        print(f"  [{i+1}] id={db_id} | '{item['name']}' | em {dt} (~{total_secs}s)")


# ---------------------------------------------------------------------------
# Disparo real do webhook via httpx
# ---------------------------------------------------------------------------

async def _fire_webhook(item: dict) -> None:
    """
    Faz o POST para o webhook_url com o payload do schedule.
    Loga o resultado (status code) ou o erro caso a requisição falhe.
    """
    print("\n" + "=" * 60)
    print(f"[WORKER] 🚀 DISPARANDO WEBHOOK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    """
    Loop principal assíncrono.

    Lógica de sleep dinâmico:
      - Se a fila estiver vazia → dorme indefinidamente até ser acordado pela API
      - Se houver itens → dorme até o execute_at do primeiro item (ou até ser acordado)
      - Se for acordado antes do tempo (novo POST chegou) → recalcula sem disparar
    """
    print("[WORKER] ✅ Worker iniciado.")
    _load_from_db()

    while True:
        _wakeup_event.clear()

        if not _queue:
            print("[WORKER] 💤 Fila vazia. Aguardando novos schedules...")
            await _wakeup_event.wait()
            print("[WORKER] 🔔 Acordado! Reconstruindo fila...")
            _load_from_db()
            continue

        # Peek no topo da fila sem remover
        next_dt, next_id, next_item = _queue[0]
        now = datetime.now()
        sleep_secs = (next_dt - now).total_seconds()

        if sleep_secs <= 0:
            # Já passou do tempo — dispara imediatamente
            heapq.heappop(_queue)
            await _fire_webhook(next_item)

            if next_item["daily"]:
                new_dt = _next_daily(next_dt, datetime.now())
                next_item["execute_at"] = new_dt
                heapq.heappush(_queue, (new_dt, next_id, next_item))
                print(f"[WORKER] 🔁 '{next_item['name']}' reagendado para {new_dt}")

            _print_queue_state()
            continue

        print(f"[WORKER] ⏳ Próximo: '{next_item['name']}' em {next_dt} (sleeping {sleep_secs:.1f}s)")

        try:
            await asyncio.wait_for(_wakeup_event.wait(), timeout=sleep_secs)

            # Acordado antes do timeout = novo schedule adicionado
            print("[WORKER] 🔔 Novo schedule detectado! Reconstruindo fila...")
            _load_from_db()

        except asyncio.TimeoutError:
            # Timeout expirou = hora de disparar
            heapq.heappop(_queue)
            await _fire_webhook(next_item)

            if next_item["daily"]:
                new_dt = _next_daily(next_dt, datetime.now())
                next_item["execute_at"] = new_dt
                heapq.heappush(_queue, (new_dt, next_id, next_item))
                print(f"[WORKER] 🔁 '{next_item['name']}' reagendado para {new_dt}")

            _print_queue_state()


# ---------------------------------------------------------------------------
# Entry point — útil para testar o worker isolado: `python worker.py`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_worker())