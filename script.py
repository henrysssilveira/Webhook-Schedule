from fastapi import FastAPI, status
from pydantic import BaseModel, Field
from models import Schedule
from database import get_connection

# 1. Initialize your FastAPI app
app = FastAPI()

@app.post("/schedule-add/", status_code=status.HTTP_201_CREATED)
async def create_schedule(schedule: Schedule):
    schedule_dict = schedule.model_dump()
    schedule_exec = schedule.execute_at

    print(type(schedule.name))
    print(type(schedule.execute_at))
    print(type(schedule.webhook_url))
    print(type(schedule.payload))
    print(type(schedule.daily))

    """ with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                INSERT INTO webhooks_schedule (name, execute_at, webhoook_url, payload, daily)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                , (schedule.name, schedule.execute_at, schedule.webhook_url, schedule.payload, schedule.daily))
            
            new_id = cur.fetchone()[0]

            conn.commit()

            return {
                "id": new_id,
                "execute_at" : schedule_exec,
                "mensagem": "Schedule Created"
            } """