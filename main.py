import os
import io
import xml.etree.ElementTree as ET
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv
from utils.nfse import NFSeService
from utils.onedrive import onedrive

# Carrega variáveis de ambiente
load_dotenv()

from typing import Dict, Any
import time

SYNC_STATE: Dict[str, Any] = {
    "status": "idle",
    "total_imported": 0,
    "logs": [],
    "start_time": 0,
    "cnpj": "",
    "progress": 0
}

def log_msg(msg: str):
    timestamp = time.strftime('%H:%M:%S')
    SYNC_STATE["logs"].append(f"{timestamp} - {msg}")
    if len(SYNC_STATE["logs"]) > 50:
        SYNC_STATE["logs"].pop(0)
    print(f"[SYNC] {msg}")

app = FastAPI(title="FECD Sync API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Variáveis de ambiente do Supabase não configuradas!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Pasta temporária para certificados
CERT_DIR = "/tmp/certs"
os.makedirs(CERT_DIR, exist_ok=True)

def get_xml_text(elem, tags):
    if elem is None: return None
    for tag in tags:
        res = elem.find(f".//{{*}}{tag}")
        if res is None: res = elem.find(f".//{tag}")
        if res is not None and res.text: return res.text
    return None

def process_sync(mes: str, pfx_data: bytes, pfx_password: str, doc_type: str = "nfse", dt_inicio: str = None, dt_fim: str = None):
    log_msg(f"Iniciando sincronização v6.0 ({doc_type}) para {mes}...")
    SYNC_STATE["status"] = "running"
    SYNC_STATE["progress"] = 5
    try:
        service = NFSeService(pfx_data, pfx_password)
        
        # 1. Carregar Clientes/Projetos
        clis_res = supabase.table("clientes").select("*").execute()
        clientes = clis_res.data or []
        projs_res = supabase.table("projetos").select("*").execute()
        projetos = projs_res.data or []

        importados_sucesso = 0
        docs = []

        if doc_type == "nfse":
            # Tenta buscar por DATA primeiro (padrão ADN v1.2)
            # Se dt_inicio/dt_fim não vierem, calculamos a partir do mes (ex: 02/2026)
            if not dt_inicio or not dt_fim:
                mm, aaaa = mes.split("/")
                import calendar
                last_day = calendar.monthrange(int(aaaa), int(mm))[1]
                dt_inicio = f"{aaaa}-{mm}-01"
                dt_fim = f"{aaaa}-{mm}-{last_day:02d}"

            log_msg(f"Buscando NFS-e por DATA: {dt_inicio} até {dt_fim}")
            res_date = service.search_by_date(dt_inicio, dt_fim)
            if res_date.get("success") and res_date.get("data", {}).get("LoteDFe"):
                docs = res_date["data"]["LoteDFe"]
                log_msg(f"Encontradas {len(docs)} notas via busca por data.")
            else:
                # Fallback NSU Loop (v5.0)
                log_msg(f"Busca por data não retornou nada ou falhou ({res_date.get('error', 'Sem dados')}). Tentando Loop NSU...")
                last_nsu = 0
                max_iterations = 20 # Limite de segurança para não rodar infinito e crachar servidor
                iterations = 0
                while iterations < max_iterations:
                    iterations += 1
                    res_nsu = service.fetch_dfe(last_nsu)
                    if not res_nsu.get("success"):
                        log_msg(f"Fim/Erro na busca NSU ({last_nsu}): {res_nsu.get('error')} - {res_nsu.get('message', '')}")
                        break
                    if not res_nsu.get("data") or not res_nsu.get("data").get("LoteDFe"):
                        log_msg(f"Fim da fila NSU alcançado em {last_nsu}.")
                        break
                    
                    batch = res_nsu["data"]["LoteDFe"]
                    docs.extend(batch)
                    last_nsu = max([int(d.get("NSU", 0)) for d in batch])
                    log_msg(f"NSU Loop: lidos {len(batch)} documentos. Próximo NSU será > {last_nsu}")
                    if len(batch) < 50: 
                        break
        
        elif doc_type == "nfe":
            # Busca NF-e (Produtos) via SOAP (Gemini Advice)
            log_msg("Buscando NF-e (Produtos) via SOAP...")
            res_nfe = service.fetch_nfe(0)
            if res_nfe.get("success"):
                docs = res_nfe["data"]["LoteDFe"]
                log_msg(f"Encontrados {len(docs)} documentos via SOAP.")
            else:
                log_msg(f"Erro na busca SEFAZ: {res_nfe.get('error')}")

        SYNC_STATE["progress"] = 30


        # 2. Processar documentos encontrados
        for doc in docs:
            xml_content = doc.get("xml_decoded")
            if not xml_content: continue
            
            try:
                root = ET.fromstring(xml_content)
                # Extração básica
                val_node = root.find(".//{*}valores") or root.find(".//{*}vLiq") or root.find(".//{*}Valores")
                toma_node = root.find(".//{*}toma") or root.find(".//{*}dest") or root.find(".//{*}tomador") or root.find(".//{*}Tomador")
                serv_node = root.find(".//{*}serv") or root.find(".//{*}det") or root.find(".//{*}Servico") or root.find(".//{*}servico")
                dps_node = root.find(".//{*}infDPS") or root.find(".//{*}infNFe") or root.find(".//{*}InfDeclaracaoPrestacaoServico") or root
                
                numero_nota = get_xml_text(root, ["nNFSe", "nNF", "Numero", "numero"])
                data_emi = get_xml_text(dps_node, ["dhEmi", "dEmi", "dCompet", "DataEmissao", "Competencia"])
                
                # Filtro de mês avançado (apenas se for nfse e não foi filtrado pela API)
                if doc_type == "nfse":
                    if dt_inicio and dt_fim and data_emi:
                        emi_date = data_emi[:10]
                        if not (dt_inicio <= emi_date <= dt_fim):
                            log_msg(f"Ignorando nota {numero_nota}: Fora do periodo configurado ({emi_date})")
                            continue
                    elif mes.split("/")[1] not in (data_emi or ""):
                        log_msg(f"Ignorando nota {numero_nota}: Fora do ano configurado ({data_emi})")
                        continue

                valor_bruto = float(get_xml_text(val_node, ["vLiq", "vNF"]) or 0)
                
                nome_tomador = get_xml_text(toma_node, ["xNome", "RazaoSocial"])
                cnpj_tomador = get_xml_text(toma_node, ["CNPJ", "CPF", "Cnpj", "Cpf"])
                
                if not nome_tomador:
                    log_msg(f"Aviso: Não encontrou nome do tomador para a nota {numero_nota}")
                
                # Vínculo cliente
                tomador_id = None
                clean_cnpj = ''.join(filter(str.isdigit, cnpj_tomador)) if cnpj_tomador else ""
                for c in clientes:
                    if clean_cnpj and clean_cnpj == ''.join(filter(str.isdigit, str(c.get('documento', '')))):
                        tomador_id = c['id']
                        break
                
                if not tomador_id and nome_tomador:
                    try:
                        c_res = supabase.table("clientes").insert({"nome_razao": nome_tomador, "documento": cnpj_tomador, "status": "Ativo"}).execute()
                        if c_res.data:
                            tomador_id = c_res.data[0]['id']
                            clientes.append(c_res.data[0])
                    except: pass

                projeto_id = projetos[0]['id'] if projetos else None
                
                if tomador_id and projeto_id:
                    nota_db = {
                        "numero": numero_nota,
                        "data_emissao": data_emi[:10] if data_emi else None,
                        "tomador_id": tomador_id,
                        "projeto_id": projeto_id,
                        "valor": valor_bruto,
                        "iss": 5.0,
                        "tipo": "Prestada" if doc_type == "nfse" else "Tomada",
                        "status": "Emitida"
                    }
                    try:
                        supabase.table("notas").upsert(nota_db, on_conflict="numero,tomador_id").execute()
                        importados_sucesso += 1
                        log_msg(f"Nota {numero_nota} ({nome_tomador}) importada/atualizada com sucesso no BD.")
                        # OneDrive
                        folder = f"Sincronizacao-{mes.replace('/', '-')}"
                        onedrive.upload_file(xml_content.encode('utf-8'), f"{numero_nota}.xml", subfolder=folder)
                        if doc_type == "nfse":
                            pdf = service.download_pdf(doc.get("ChaveAcesso"))
                            if pdf: onedrive.upload_file(pdf, f"{numero_nota}.pdf", subfolder=folder)
                    except Exception as db_err: 
                        log_msg(f"Erro ao inserir nota {numero_nota} no banco: {db_err}")
                else:
                    if not projeto_id:
                        log_msg(f"Nota {numero_nota} abortada: Seu sistema não possui um Projeto padrão cadastrado na nuvem.")
                    if not tomador_id:
                        log_msg(f"Nota {numero_nota} abortada: Falha ao criar/vincular cliente com CNPJ {cnpj_tomador}")
            except Exception as e:
                log_msg(f"Erro ao processar doc: {e}")

        SYNC_STATE["progress"] = 100
        SYNC_STATE["status"] = "done"
        SYNC_STATE["total_imported"] = importados_sucesso
        log_msg(f"Sincronização Finalizada. {importados_sucesso} notas importadas e sincronizadas.")
    except Exception as e:
        SYNC_STATE["status"] = "error"
        log_msg(f"ERRO CRÍTICO NO ROBÔ: {e}")

@app.get("/status")
async def get_sync_status():
    return SYNC_STATE

@app.post("/sincronizar")
async def disparar_sincronizacao(
    background_tasks: BackgroundTasks,
    token: str = Form(...),
    mes_referencia: str = Form(...),
    senha_pfx: str = Form(...),
    doc_type: str = Form("nfse"),
    data_inicio: str = Form(None),
    data_fim: str = Form(None),
    certificado: UploadFile = File(...)
):
    ALLOWED_TOKEN = os.getenv("SYNC_SECRET_TOKEN", "senha_super_secreta_fecd_render")
    if token != ALLOWED_TOKEN:
        raise HTTPException(status_code=403, detail="Token de segurança inválido.")
    
    pf_data = await certificado.read()
    cnpj_detectado = "Não identificado"
    try:
        test_service = NFSeService(pf_data, senha_pfx)
        cnpj_detectado = test_service.cnpj
        if not cnpj_detectado: raise Exception("PFX inválido.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(process_sync, mes_referencia, pf_data, senha_pfx, doc_type, data_inicio, data_fim)
    
    SYNC_STATE["status"] = "running"
    SYNC_STATE["progress"] = 0
    SYNC_STATE["total_imported"] = 0
    SYNC_STATE["logs"] = []
    SYNC_STATE["cnpj"] = cnpj_detectado
    log_msg(f"Sincronização agendada para o CNPJ {cnpj_detectado}")

    return {
        "status": "sucesso",
        "cnpj": cnpj_detectado,
        "mensagem": f"Robô v6.0 iniciado para o CNPJ {cnpj_detectado}! Buscando {doc_type} de {mes_referencia}."
    }
