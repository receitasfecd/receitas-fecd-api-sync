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
    print(f"Iniciando sincronização real para o mês {mes}...")
    
    try:
        service = NFSeService(pfx_data, pfx_password)
        
        # 1. Carregar Clientes e Projetos para fazer de-para (Cache simples)
        clientes = supabase.table("clientes").select("id, nome_razao").execute().data or []
        projetos = supabase.table("projetos").select("id, codigo, nome").execute().data or []

        # 2. Buscar último NSU no banco para não baixar duplicado
        last_nsu = 0
        try:
            # Observação: Se a tabela notas não tiver a coluna NSU, isso pode falhar.
            # Vamos tentar buscar ou assumir 0.
            res = supabase.table("notas").select("id").order("created_at", desc=True).limit(1).execute()
            # Se não temos um histórico de NSU gravado, usamos 0 ou buscamos de um arquivo last_nsu.txt persistente
        except:
            pass

        print(f"Buscando no Portal Nacional... CNPJ: {service.cnpj}")
        
        result = service.fetch_dfe(last_nsu)
        if not result.get("success"):
            print(f"Erro ao buscar DFe: {result.get('error')}")
            return

        data = result.get("data")
        if not data or not data.get("LoteDFe"):
            print("Nenhum documento novo encontrado no portal.")
            return

        docs = data.get("LoteDFe")
        print(f"Recebidos {len(docs)} documentos.")

        for doc in docs:
            nsu = doc.get("NSU")
            chave = doc.get("ChaveAcesso")
            xml_content = doc.get("xml_decoded")

            if not xml_content: continue

            # a. Salvar no OneDrive (XML)
            folder_path = f"{mes.replace('/', '-')}"
            onedrive.upload_file(xml_content.encode('utf-8'), f"{nsu}_{chave}.xml", subfolder=folder_path)

            # b. Baixar PDF e salvar no OneDrive
            pdf_content = service.download_pdf(chave)
            if pdf_content:
                onedrive.upload_file(pdf_content, f"{nsu}_{chave}.pdf", subfolder=folder_path)

            # c. Parsear XML para o banco conforme colunas da tabela "notas"
            try:
                root = ET.fromstring(xml_content)
                val_node = root.find(".//{*}valores")
                toma_node = root.find(".//{*}toma")
                serv_node = root.find(".//{*}serv")
                dps_node = root.find(".//{*}infDPS")
                
                # Dados extraídos do XML
                numero_nota = get_xml_text(root, ["nNFSe"])
                data_emi = get_xml_text(dps_node, ["dhEmi"]) or get_xml_text(dps_node, ["dCompet"])
                valor_bruto = float(get_xml_text(val_node, ["vLiq"]) or 0)
                valor_iss = float(get_xml_text(val_node, ["vISSQN"]) or 0)
                nome_tomador = get_xml_text(toma_node, ["xNome"])
                desc_servico = get_xml_text(serv_node, ["xDescServ"]) or ""

                # Tenta encontrar o CLIENTE ID (Tomador)
                tomador_id = None
                if nome_tomador:
                    for c in clientes:
                        if nome_tomador.lower() in c['nome_razao'].lower():
                            tomador_id = c['id']
                            break
                
                # Tenta encontrar o PROJETO ID (via código no comentário ou similar)
                projeto_id = None
                # Exemplo: Procura um código numérico de 8 dígitos na descrição da nota
                import re
                proj_match = re.search(r'\d{8}', desc_servico)
                if proj_match:
                    proj_cod = proj_match.group(0)
                    for p in projetos:
                        if proj_cod in p['codigo']:
                            projeto_id = p['id']
                            break

                nota_db = {
                    "numero": numero_nota,
                    "data_emissao": data_emi[:10] if data_emi else None,
                    "tomador_id": tomador_id,
                    "projeto_id": projeto_id,
                    "valor": valor_bruto,
                    "iss": (valor_iss / valor_bruto * 100) if valor_bruto > 0 else 5.0,
                    "tipo": "Prestada",
                    "status": "Emitida"
                }
                
                # Inserir no banco (tabela "notas")
                # Nota: chave_acesso não existe na tabela "notas" do frontend, 
                # então usamos o número para evitar duplicados se possível ou apenas insert
                supabase.table("notas").insert(nota_db).execute()
                print(f"Nota {numero_nota} processada e salva com sucesso.")
                
            except Exception as e:
                print(f"Erro ao parsear/salvar nota {nsu}: {e}")

    except Exception as e:
        print(f"Erro fatal na rotina de sincronização: {e}")

    except Exception as e:
        print(f"Erro fatal na rotina de sincronização: {e}")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "FECD Sync API 2.0 Online. Pronto para chamadas mTLS."}

@app.post("/sincronizar")
async def disparar_sincronizacao(
    background_tasks: BackgroundTasks,
    token: str = Form(...),
    mes_referencia: str = Form(...),
    senha_pfx: str = Form(...),
    certificado: UploadFile = File(...)
):
    if token != os.getenv("SYNC_SECRET_TOKEN", "fecd_secreto_123"):
        raise HTTPException(status_code=403, detail="Token de segurança inválido.")
    
    # Lê arquivo certificado
    pf_data = await certificado.read()
    
    # Valida certificado (tenta iniciar o service)
    try:
        test_service = NFSeService(pf_data, senha_pfx)
        if not test_service.cnpj:
             raise Exception("PFX ou senha inválidos.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro no certificado: {str(e)}")

    # Inicia rotina em background
    background_tasks.add_task(process_sync, mes_referencia, pf_data, senha_pfx)
    
    return {
        "status": "sucesso", 
        "mensagem": f"Robô iniciado! Buscando notas de {mes_referencia} no Portal Nacional. Os arquivos serão salvos no seu OneDrive em instantes."
    }
