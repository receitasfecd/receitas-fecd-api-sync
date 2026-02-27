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

        # 2. Consultar Portal Nacional
        last_nsu = 0
        print(f"Buscando notas no Portal Nacional (CNPJ: {service.cnpj}, NSU > {last_nsu})")
        
        result = service.fetch_dfe(last_nsu)
        if not result.get("success"):
            print(f"Erro no Portal: {result.get('details', result.get('error'))}")
            return

        data = result.get("data")
        if not data or not data.get("LoteDFe"):
            print("Resultado: Nenhuma nota nova disponível no Portal Nacional para este CNPJ.")
            return

        docs = data.get("LoteDFe")
        print(f"Sucesso: {len(docs)} documentos encontrados.")

        importados_sucesso = 0
        for doc in docs:
            nsu = doc.get("NSU")
            chave = doc.get("ChaveAcesso")
            xml_content = doc.get("xml_decoded")

            if not xml_content: continue

            # a. OneDrive (Sempre salva para garantir backup)
            folder_path = f"Sincronizacao-{mes.replace('/', '-')}"
            try:
                onedrive.upload_file(xml_content.encode('utf-8'), f"{nsu}_{chave}.xml", subfolder=folder_path)
                pdf_content = service.download_pdf(chave)
                if pdf_content:
                    onedrive.upload_file(pdf_content, f"{nsu}_{chave}.pdf", subfolder=folder_path)
            except:
                print(f"Aviso: Erro ao enviar arquivos da nota {nsu} para o OneDrive.")

            # b. Persistência no Banco de Dados
            try:
                root = ET.fromstring(xml_content)
                val_node = root.find(".//{*}valores")
                toma_node = root.find(".//{*}toma")
                serv_node = root.find(".//{*}serv")
                dps_node = root.find(".//{*}infDPS")
                
                # Campos básicos
                numero_nota = get_xml_text(root, ["nNFSe"])
                data_emi = get_xml_text(dps_node, ["dhEmi"]) or get_xml_text(dps_node, ["dCompet"])
                valor_bruto = float(get_xml_text(val_node, ["vLiq"]) or 0)
                valor_iss = float(get_xml_text(val_node, ["vISSQN"]) or 0)
                
                # Dados do Tomador no XML
                nome_tomador_xml = get_xml_text(toma_node, ["xNome"])
                cnpj_tomador_xml = get_xml_text(toma_node, ["CNPJ"]) or get_xml_text(toma_node, ["CPF"])
                desc_servico = get_xml_text(serv_node, ["xDescServ"]) or ""

                # Encontrar Cliente no Banco (Prioridade 1: CNPJ/CPF, Prioridade 2: Nome)
                tomador_id = None
                if cnpj_tomador_xml:
                    clean_cnpj = ''.join(filter(str.isdigit, cnpj_tomador_xml))
                    for c in clientes:
                        if clean_cnpj in str(c.get('documento', '')) or clean_cnpj in str(c.get('cnpj', '')):
                            tomador_id = c['id']
                            break
                
                if not tomador_id and nome_tomador_xml:
                    for c in clientes:
                        if nome_tomador_xml.lower() in str(c.get('nome_razao', '')).lower():
                            tomador_id = c['id']
                            break
                
                if not tomador_id:
                    print(f"Aviso: Tomador '{nome_tomador_xml}' não encontrado no cadastro. Pulando inserção da nota {numero_nota} para evitar erro.")
                    continue

                # Vínculo de Projeto
                projeto_id = None
                # Busca código de 8 dígitos na descrição (Padrão FECD)
                import re
                proj_match = re.search(r'\d{8}', desc_servico)
                if proj_match:
                    proj_cod = proj_match.group(0)
                    for p in projetos:
                        if proj_cod in str(p.get('codigo', '')):
                            projeto_id = p['id']
                            break
                
                if not projeto_id:
                    # Tenta pegar o primeiro projeto ativo se for obrigatório ou associar a um projeto "Geral"
                    if projetos:
                        projeto_id = projetos[0]['id']

                nota_db = {
                    "numero": numero_nota,
                    "data_emissao": data_emi[:10] if data_emi else None,
                    "tomador_id": tomador_id,
                    "projeto_id": projeto_id,
                    "valor": valor_bruto,
                    "iss": (valor_iss / valor_bruto * 100) if valor_bruto > 0 and valor_iss > 0 else 5.0,
                    "tipo": "Prestada",
                    "status": "Emitida"
                }
                
                try:
                    supabase.table("notas").insert(nota_db).execute()
                    importados_sucesso += 1
                    print(f"Nota {numero_nota} vinculada ao cliente {tomador_id} e salva!")
                except Exception as db_err:
                    if "409" in str(db_err) or "duplicate" in str(db_err).lower():
                        pass # Já existe
                    else:
                        print(f"Erro ao salvar nota {numero_nota}: {db_err}")
                
            except Exception as parse_err:
                print(f"Erro no parseamento da nota: {parse_err}")

        print(f"Sincronização Finalizada. Total salvo: {importados_sucesso} notas.")

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
    try:
        test_service = NFSeService(pf_data, senha_pfx)
        if not test_service.cnpj:
             raise Exception("PFX não identificado.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Certificado Inválido: {str(e)}")

    background_tasks.add_task(process_sync, mes_referencia, pf_data, senha_pfx)
    
    return {
        "status": "sucesso", 
        "mensagem": f"Robô v3.0 iniciado! O processo levará alguns instantes. Confira a aba Notas Emitidas em breve."    }
