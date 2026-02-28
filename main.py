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

def process_sync(mes: str, pfx_data: bytes, pfx_password: str):
    print(f"Iniciando sincronização real (v3.0) para {mes}...")
    
    try:
        service = NFSeService(pfx_data, pfx_password)
        
        # 1. Carregar Clientes e Projetos para de-para inteligente
        clientes = []
        try:
            clis_res = supabase.table("clientes").select("*").execute()
            clientes = clis_res.data or []
        except Exception as e:
            print(f"Erro ao carregar clientes: {e}")

        projetos = []
        try:
            projs_res = supabase.table("projetos").select("*").execute()
            projetos = projs_res.data or []
        except Exception as e:
            print(f"Erro ao carregar projetos: {e}")

        # 2. Consultar Portal Nacional em LOOP (Busca exaustiva)
        last_nsu = 0
        total_notas_detectadas = 0
        importados_sucesso = 0
        
        # Filtro de data: Extrai Ano/Mês
        # mes vem como "02/2026"
        ref_mes, ref_ano = mes.split("/")

        continuar_busca = True
        while continuar_busca:
            print(f"Buscando Lote: NSU > {last_nsu}")
            result = service.fetch_dfe(last_nsu)
            
            if not result.get("success"):
                print(f"Erro no Portal: {result.get('error')}")
                break

            data = result.get("data")
            if not data or not data.get("LoteDFe"):
                print("Chegou ao fim das notas no portal.")
                break

            docs = data.get("LoteDFe")
            print(f"Lote recebido: {len(docs)} documentos.")
            
            for doc in docs:
                nsu = int(doc.get("NSU", 0))
                last_nsu = max(last_nsu, nsu)
                chave = doc.get("ChaveAcesso")
                xml_content = doc.get("xml_decoded")

                if not xml_content: continue

                try:
                    root = ET.fromstring(xml_content)
                    dps_node = root.find(".//{*}infDPS")
                    data_emi = get_xml_text(dps_node, ["dhEmi"]) or get_xml_text(dps_node, ["dCompet"])
                    
                    # Filtra apenas o mês de interesse para o banco de dados
                    # Se data_emi é "2026-02-27T..."
                    if data_emi and f"{ref_ano}-{ref_mes}" in data_emi:
                        total_notas_detectadas += 1
                        print(f"Nota {nsu} detectada para o período {mes}. Processando...")
                        
                        # Processamento da nota (igual v4.1...)
                        val_node = root.find(".//{*}valores")
                        toma_node = root.find(".//{*}toma")
                        serv_node = root.find(".//{*}serv")
                        
                        numero_nota = get_xml_text(root, ["nNFSe"])
                        valor_bruto = float(get_xml_text(val_node, ["vLiq"]) or 0)
                        valor_iss = float(get_xml_text(val_node, ["vISSQN"]) or 0)
                        
                        nome_tomador_xml = get_xml_text(toma_node, ["xNome"])
                        cnpj_tomador_xml = get_xml_text(toma_node, ["CNPJ"]) or get_xml_text(toma_node, ["CPF"])
                        desc_servico = get_xml_text(serv_node, ["xDescServ"]) or ""

                        # 1. Busca/Cadastro Cliente
                        tomador_id = None
                        clean_cnpj_xml = ''.join(filter(str.isdigit, cnpj_tomador_xml)) if cnpj_tomador_xml else None
                        for c in clientes:
                            if clean_cnpj_xml and clean_cnpj_xml == ''.join(filter(str.isdigit, str(c.get('documento', '')) or '')):
                                tomador_id = c['id']
                                break
                        
                        if not tomador_id and nome_tomador_xml:
                            try:
                                res_new = supabase.table("clientes").insert({"nome_razao": nome_tomador_xml, "documento": cnpj_tomador_xml, "status": "Ativo"}).execute()
                                if res_new.data:
                                    tomador_id = res_new.data[0]['id']
                                    clientes.append(res_new.data[0])
                            except: pass

                        # 2. Busca/Cadastro Projeto
                        projeto_id = None
                        if projetos: projeto_id = projetos[0]['id'] # Default

                        if tomador_id and projeto_id:
                            nota_db = {
                                "numero": numero_nota,
                                "data_emissao": data_emi[:10],
                                "tomador_id": tomador_id,
                                "projeto_id": projeto_id,
                                "valor": valor_bruto,
                                "iss": (valor_iss / valor_bruto * 100) if valor_bruto > 0 else 5.0,
                                "tipo": "Prestada",
                                "status": "Emitida"
                            }
                            try:
                                supabase.table("notas").upsert(nota_db, on_conflict="numero,tomador_id").execute()
                                importados_sucesso += 1
                                # Envia PDF/XML ao OneDrive apenas se filter passar
                                folder_path = f"Sincronizacao-{mes.replace('/', '-')}"
                                onedrive.upload_file(xml_content.encode('utf-8'), f"{numero_nota}_{chave}.xml", subfolder=folder_path)
                                pdf = service.download_pdf(chave)
                                if pdf: onedrive.upload_file(pdf, f"{numero_nota}_{chave}.pdf", subfolder=folder_path)
                            except: pass

                except Exception as e:
                    print(f"Erro no parse da nota {nsu}: {e}")

            if len(docs) < 50: # Fim dos registros
                break

        print(f"Fim do Ciclo. Atendidas: {total_notas_detectadas}, Importadas: {importados_sucesso}")

    except Exception as fatal_err:
        print(f"ERRO CRÍTICO: {fatal_err}")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "FECD Sync API v3.0 Online."}

@app.post("/sincronizar")
async def disparar_sincronizacao(
    background_tasks: BackgroundTasks,
    token: str = Form(...),
    mes_referencia: str = Form(...),
    senha_pfx: str = Form(...),
    certificado: UploadFile = File(...)
):
    # Unificado com o site
    ALLOWED_TOKEN = os.getenv("SYNC_SECRET_TOKEN", "senha_super_secreta_fecd_render")
    if token != ALLOWED_TOKEN:
        raise HTTPException(status_code=403, detail="Token de segurança inválido.")
    
    pf_data = await certificado.read()
    
    # Extrai CNPJ para feedback imediato
    cnpj_detectado = "Não identificado"
    try:
        test_service = NFSeService(pf_data, senha_pfx)
        cnpj_detectado = test_service.cnpj
        if not cnpj_detectado:
             raise Exception("PFX não identificado.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler certificado: {str(e)}")

    background_tasks.add_task(process_sync, mes_referencia, pf_data, senha_pfx)
    
    return {
        "status": "sucesso",
        "cnpj": cnpj_detectado,
        "mensagem": f"Robô v4.1 iniciado para o CNPJ {cnpj_detectado}! Buscando notas de {mes_referencia}. Verifique a aba de Notas em instantes."
    }
