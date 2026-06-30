# ============================================================
# PATCH para main.py — adicione/substitua estas partes
# ============================================================

# 1. Adicione este import no topo do arquivo
from contextlib import asynccontextmanager
from worker import run_worker, notify_new_schedule

# 2. Substitua `app = FastAPI()` por isso:

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia o worker em background quando a API sobe
    asyncio.create_task(run_worker())
    yield
    # (cleanup se precisar no futuro)

app = FastAPI(lifespan=lifespan)

# 3. No final do endpoint POST, antes do return, adicione:
#    notify_new_schedule()
#
# Exemplo de como fica o trecho final do create_schedule:
#
#        conn.commit()
#
#        notify_new_schedule()   # <--- acorda o worker
#
#        return {
#            "id": new_id,
#            "execute_at": schedule_exec,
#            "mensagem": "Schedule Created"
#        }