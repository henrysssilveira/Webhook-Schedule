from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel, Field
from models import Schedule
from database import get_connection
import json
from datetime import datetime
from contextlib import asynccontextmanager
from worker import run_worker, notify_new_schedule
import asyncio
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import secrets
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from datetime import timezone, timedelta
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
print(API_KEY)

security = HTTPBearer(auto_error=True)

def _get_tz():
    tz_env = os.getenv("TIMEZONE", "UTC").strip()
    try:
        offset_hours = float(tz_env)
        return timezone(timedelta(hours=offset_hours))
    except ValueError:
        return ZoneInfo(tz_env)

LOCAL_TZ = _get_tz()

def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials

    if not secrets.compare_digest(token, API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
        )

    return token

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia o worker em background quando a API sobe
    asyncio.create_task(run_worker())
    yield
    # (cleanup se precisar no futuro)

app = FastAPI(lifespan=lifespan)

@app.post("/ScheduleCreate/", status_code=status.HTTP_201_CREATED)
async def create_schedule(schedule: Schedule, user=Depends(verify_token)):
    schedule_dict = schedule.model_dump()
    schedule_exec = schedule.execute_at

    hoje = datetime.today()

    ano = schedule.execute_at.date.year
    mes = schedule.execute_at.date.month
    dia = schedule.execute_at.date.day
    segundos = schedule.execute_at.seconds or 0

    # Se não tiver data, usa hoje como base
    if ano and mes and dia:
        dt_execute_at = datetime(
            year=ano, month=mes, day=dia,
            hour=schedule.execute_at.hour,
            minute=schedule.execute_at.minutes,
            second=segundos
            tzinfo=LOCAL_TZ
        )
    else:
        # daily=True sem data → usa hoje como ponto de partida
        # o worker vai recalcular o próximo ciclo automaticamente
        dt_execute_at = datetime(
            year=hoje.year, month=hoje.month, day=hoje.day,
            hour=schedule.execute_at.hour,
            minute=schedule.execute_at.minutes,
            second=segundos
        )

    # 3. Criamos o objeto datetime de forma 100% segura
    dt_execute_at = datetime(
        year=ano,
        month=mes,
        day=dia,
        hour=schedule.execute_at.hour,
        minute=schedule.execute_at.minutes,
        second=segundos
    )
    
    # O payload continua sendo convertido para JSON (pois o banco aceita como Texto ou JSONB)
    payload_db = schedule.payload if isinstance(schedule.payload, str) else json.dumps(schedule.payload)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO webhooks_schedule (name, execute_at, webhook_url, payload, daily)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (
                schedule.name, 
                dt_execute_at,       # Agora enviamos o datetime seguro
                schedule.webhook_url, 
                payload_db,          
                schedule.daily
            ))
            
            new_id = cur.fetchone()[0]

            conn.commit()

    notify_new_schedule()   # <--- acorda o worker

    return {
        "id": new_id,
        "execute_at": schedule_exec, 
        "mensagem": "Schedule Created"
    }

@app.get("/ScheduleList/", response_model=list[Schedule])
async def list_schedules(user=Depends(verify_token)):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM webhooks_schedule
            """)
            
            # 1. Tira o [0] daqui para pegar todas as linhas de verdade
            rows = cur.fetchall()

            schedules = []
            
            # 2. Percorre cada linha que veio do banco
            for row in rows:
                # O psycopg devolve uma tupla, então acessamos pela posição (índice)
                # row[0] = id, row[1] = name, row[2] = execute_at (que é um datetime!), etc.
                
                db_id = row[0]
                name = row[1]
                dt = row[2]  # <--- Este é o objeto datetime vindo do PostgreSQL
                webhook_url = row[3]
                payload = row[4]
                daily = row[5]

                # 3. Fazemos a "Tradução Reversa": montamos o dicionário do TimeConfig a partir do datetime
                time_config_dict = {
                    "hour": dt.hour,
                    "minutes": dt.minute,
                    "seconds": dt.second,
                    "date": {
                        "day": dt.day,
                        "month": dt.month,
                        "year": dt.year
                    }
                }

                # 4. Criamos o modelo Schedule manualmente, passando os campos certos
                schedules.append(
                    Schedule(
                        id=db_id,
                        name=name,
                        execute_at=time_config_dict,
                        webhook_url=webhook_url,
                        payload=payload,
                        daily=daily
                    )
                )

            return schedules

@app.get("/ScheduleGet/{schedule_id}", response_model=Schedule)
async def get_schedule(schedule_id: int, user=Depends(verify_token)):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, execute_at, webhook_url, payload, daily
                FROM webhooks_schedule
                WHERE id = %s
            """, (schedule_id,))
            row = cur.fetchone()
            if row:
                db_id = row[0]
                name = row[1]
                dt = row[2]
                webhook_url = row[3]
                payload = row[4]
                daily = row[5]

                time_config_dict = {
                    "hour": dt.hour,
                    "minutes": dt.minute,
                    "seconds": dt.second,
                    "date": {
                        "day": dt.day,
                        "month": dt.month,
                        "year": dt.year
                    }
                }

                return Schedule(
                    name=name,
                    execute_at=time_config_dict,
                    webhook_url=webhook_url,
                    payload=payload,
                    daily=daily
                )
            else:
                # O jeito FastAPI de retornar erros:
                raise HTTPException(status_code=404, detail="Schedule not found")

@app.delete("/ScheduleDelete/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(schedule_id: int, user=Depends(verify_token)):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM webhooks_schedule
                WHERE id = %s
            """, (schedule_id,))
            conn.commit()

            # Acorda o worker para recalcular a fila
            notify_new_schedule()

            return {
                "message": "Schedule deleted successfully",
                "id": schedule_id
            }