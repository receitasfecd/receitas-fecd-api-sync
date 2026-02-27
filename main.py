import os
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

app = FastAPI(title="FECD Sync API", version="1.0.0")

# CONFIGURAÇÃO DE CORS - LIBERA O FRONTEND PARA CHAMAR O ROBÔ
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Libera para qualquer origem em desenvolvimento, podemos restringir depois
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Variáveis de ambiente do Supabase não configuradas!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class SyncRequest(BaseModel):
    token: str
    mes_referencia: str # Ex: "02/2026"

def run_sync_routine(mes: str):
    """
    Simula uma rotina pesada de sincronização, web scraping ou processamento.
    Na versão final, o Selenium/Requests se comunicará aqui com a prefeitura
    e fará o INSERT massivo no banco de dados.
    """
    print(f"Iniciando rotina pesada de sincronização para o mês: {mes}...")
    
    # Exemplo: Atualizar tabela de log de Sincronização no Supabase (que criaremos depois)
    try:
        data = {
            "resumo": f"Sincronização iniciada para {mes}. Aguardando processamento da fila.",
            "sucesso": True,
            "importados": 0
        }
        # Inserção de mock na tabela "lotes_sincronizacao" ou similar
        print("Sincronização mock disparada.")
    except Exception as e:
        print(f"Erro na rotina: {e}")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "FECD Sync API Online e aguardando requisições."}

@app.post("/sincronizar")
def disparar_sincronizacao(req: SyncRequest, background_tasks: BackgroundTasks):
    """
    Endpoint principal acionado pelo painel web Vercel para iniciar o robô.
    """
    # Validação simples de segurança entre front e back
    if req.token != os.getenv("SYNC_SECRET_TOKEN", "fecd_secreto_123"):
        raise HTTPException(status_code=403, detail="Token de segurança inválido.")
    
    # Dispara a rotina em backgroud para não travar a requisição (timeout)
    background_tasks.add_task(run_sync_routine, req.mes_referencia)
    
    return {"status": "sucesso", "mensagem": f"A sincronização do mês {req.mes_referencia} foi iniciada em segundo plano. Verifique o dashboard de logs em alguns minutos."}
