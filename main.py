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
        
        # 1. Carregar Clientes e Projetos para fazer de-para
        clientes = []
        try:
            clis_res = supabase.table("clientes").select("id, nome_razao").execute()
            clientes = clis_res.data or []
        except Exception as e:
            print(f"Erro ao carregar clientes: {e}")

        projetos = []
        try:
            projs_res = supabase.table("projetos").select("id, codigo, nome").execute()
            projetos = projs_res.data or []
        except Exception as e:
            print(f"Erro ao carregar projetos: {e}")

        # 2. Buscar último NSU (Para esta versão, iniciamos em 0 ou usamos o maior id se nsu não existir)
        last_nsu = 0
        
        print(f"Buscando no Portal Nacional... CNPJ: {service.cnpj} (NSU > {last_nsu})")
        
        result = service.fetch_dfe(last_nsu)
        if not result.get("success"):
            error_msg = result.get('error', 'Erro desconhecido')
            details = result.get('details', '')
            print(f"Portal retornou erro: {error_msg} - {details}")
            return

        data = result.get("data")
        if not data or not data.get("LoteDFe"):
            print("Portal: Nenhum documento novo disponível para seu CNPJ.")
            return

        docs = data.get("LoteDFe")
        print(f"Sincronizador: Localizados {len(docs)} documentos.")

        importados_sucesso = 0
        for doc in docs:
            nsu = doc.get("NSU")
            chave = doc.get("ChaveAcesso")
            xml_content = doc.get("xml_decoded")

            if not xml_content: 
                print(f"NSU {nsu}: XML vazio ou não decodificado.")
                continue

            # a. Salvar no OneDrive (XML/PDF)
            folder_path = f"{mes.replace('/', '-')}"
            onedrive.upload_file(xml_content.encode('utf-8'), f"{nsu}_{chave}.xml", subfolder=folder_path)
            
            pdf_content = service.download_pdf(chave)
            if pdf_content:
                onedrive.upload_file(pdf_content, f"{nsu}_{chave}.pdf", subfolder=folder_path)

            # b. Parsear e Salvar no Banco
            try:
                root = ET.fromstring(xml_content)
                val_node = root.find(".//{*}valores")
                toma_node = root.find(".//{*}toma")
                serv_node = root.find(".//{*}serv")
                dps_node = root.find(".//{*}infDPS")
                
                numero_nota = get_xml_text(root, ["nNFSe"])
                data_emi = get_xml_text(dps_node, ["dhEmi"]) or get_xml_text(dps_node, ["dCompet"])
                valor_bruto = float(get_xml_text(val_node, ["vLiq"]) or 0)
                valor_iss = float(get_xml_text(val_node, ["vISSQN"]) or 0)
                nome_tomador = get_xml_text(toma_node, ["xNome"])
                desc_servico = get_xml_text(serv_node, ["xDescServ"]) or ""

                # Vínculo de Cliente
                tomador_id = None
                if nome_tomador:
                    for c in clientes:
                        if nome_tomador.lower() in c['nome_razao'].lower():
                            tomador_id = c['id']
                            break
                
                # Vínculo de Projeto
                projeto_id = None
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
                    "iss": (valor_iss / valor_bruto * 100) if valor_bruto > 0 and valor_iss > 0 else 5.0,
                    "tipo": "Prestada",
                    "status": "Emitida"
                }
                
                # Inserção com tratamento de erro
                try:
                    insert_res = supabase.table("notas").insert(nota_db).execute()
                    importados_sucesso += 1
                    print(f"Nota {numero_nota} importada com sucesso!")
                except Exception as db_err:
                    if "409" in str(db_err) or "duplicate" in str(db_err).lower():
                        print(f"Nota {numero_nota} já existe no banco. Pulando.")
                    else:
                        print(f"Erro DB na nota {numero_nota}: {db_err}")
                
            except Exception as parse_err:
                print(f"Erro ao parsear NSU {nsu}: {parse_err}")

        print(f"Sincronização Finalizada: {importados_sucesso} notas importadas para o banco.")

    except Exception as fatal_err:
        print(f"ERRO CRÍTICO NA SINCRONIZAÇÃO: {fatal_err}")

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
