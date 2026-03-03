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

from typing import Dict, Any, List
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
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"{timestamp} - {msg}"
    
    # Log na memória para o frontend
    SYNC_STATE["logs"].append(log_line)
    if len(SYNC_STATE["logs"]) > 100:
        SYNC_STATE["logs"].pop(0)
    
    # Log no disco (persistente e completo)
    try:
        with open("sync_debug.log", "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except: pass
    
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

def is_nota_cancelada_no_pdf(pdf_bytes, numero_nota):
    """
    Analise profunda do PDF para detectar marcas d'água de cancelamento.
    """
    if not pdf_bytes: return False
    try:
        import io, re
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        full_text = ""
        for page in reader.pages:
            full_text += (page.extract_text() or "")
        
        # 1. Limpeza total (Apenas LETRAS de A-Z)
        clean_text = re.sub(r'[^A-Z]', '', full_text.upper())
        
        # 2. Palavras-Chave Padrão e Espelhadas (comum em marcas d'água diagonais)
        # ADALECNAC = CANCELADA invertido
        keywords = ["CANCELADA", "SUBSTITUIDA", "INVALIDADA", "CANCELADO", "ESTORNADA", "ESTORNADO", "ADALECNAC", "ODALECNAC"]
        
        log_msg(f"Debug Nota {numero_nota}: PDF Text Sample (60 chars): {clean_text[:60]}")
        
        for kw in keywords:
            if kw in clean_text:
                log_msg(f"Nota {numero_nota}: Cancelamento detectado no PDF pelo termo: {kw}")
                return True
        
        # 3. Busca no texto bruto (com espaços)
        raw_upper = full_text.upper()
        if "CANCELADA" in raw_upper or "SUBSTITUIDA" in raw_upper:
            return True
            
    except Exception as e:
        log_msg(f"Erro ao ler PDF da nota {numero_nota}: {e}")
    return False

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

            log_msg(f"Buscando NFS-e EMITIDAS (Receitas) por DATA: {dt_inicio} até {dt_fim}")
            current_page = 1
            while True:
                res_date = service.search_by_date(dt_inicio, dt_fim, doc_type="1", pagina=current_page)
                if res_date.get("success") and res_date.get("data", {}).get("LoteDFe"):
                    page_docs = res_date["data"]["LoteDFe"]
                    docs.extend(page_docs)
                    log_msg(f"Pagina {current_page}: Encontradas {len(page_docs)} notas.")
                    if len(page_docs) < 100: # Fim das páginas
                        break
                    current_page += 1
                    if current_page > 5: break # Limite de segurança de 500 notas
                else:
                    if current_page == 1:
                        log_msg(f"Busca por data não retornou nada ou falhou ({res_date.get('error', 'Sem dados')}). Tentando Loop NSU...")
                    break
            
            if docs:
                log_msg(f"Total de {len(docs)} notas encontradas via busca por data.")
            else:
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
        log_msg(f"Processando {len(docs)} documentos brutos...")

        # 2. Pré-processar Eventos de Cancelamento
        # No padrão Nacional, o cancelamento é um Evento (tpEvento 110111) vinculado à nota.
        notas_canceladas_por_evento = set()
        for d in docs:
            xc = d.get("xml_decoded")
            if xc and "110111" in xc:
                try:
                    r = ET.fromstring(xc)
                    # Tenta pegar o número da nota vinculada ao evento
                    num_vinculo = get_xml_text(r, ["nNFSe", "numero", "nNF"])
                    if num_vinculo:
                        notas_canceladas_por_evento.add(num_vinculo)
                        log_msg(f"Identificado Evento de Cancelamento (110111) para Nota {num_vinculo}")
                except: pass

        # 3. Processar documentos encontrados
        for doc in docs:
            xml_content = doc.get("xml_decoded")
            if not xml_content: continue
            
            try:
                root = ET.fromstring(xml_content)
                # Extração básica
                val_node = root.find(".//{*}valores") or root.find(".//{*}vLiq") or root.find(".//{*}Valores")
                toma_node = root.find(".//{*}toma") or root.find(".//{*}dest") or root.find(".//{*}tomador") or root.find(".//{*}Tomador") or root.find(".//{*}TomadorServico")
                prest_node = root.find(".//{*}prestador") or root.find(".//{*}Prestador") or root.find(".//{*}PrestadorServico") or root.find(".//{*}emit")
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
                
                clean_cnpj_toma = ''.join(filter(str.isdigit, cnpj_tomador)) if cnpj_tomador else ""
                meu_cnpj = service.cnpj or ""
                
                nota_tipo = "Prestada" if doc_type == "nfse" else "Tomada"
                
                # Detecção de Cancelamento / Substituição - ABORDAGEM TOTAL
                is_cancelada = False
                
                # 0. Verificação Global no XML Bruto (SOMENTE EM TAGS DE VALOR, não no texto todo para evitar falsos positivos)
                # Procuramos por <situacao>2</situacao> ou <cSit>2</cSit> etc, de forma mais contida
                xml_raw_upper = xml_content.upper()
                
                # Gatilhos específicos de texto que indicam cancelamento real no XML
                xml_indicators = ["<SITUACAO>2</SITUACAO>", "<SITUACAO>3</SITUACAO>", "<CSITNFSE>2</CSITNFSE>", "<CSITNFSE>3</CSITNFSE>", 
                                 "CANCELAMENTO", "SUBSTITUICAO", "EVENTOCANCELAMENTO"]
                
                if any(ind in xml_raw_upper for ind in xml_indicators):
                    # Não marcamos direto como True para evitar erro da NF1, vamos validar com os outros campos
                    log_msg(f"Nota {numero_nota}: Alerta de cancelamento detectado no XML bruto.")
                    is_cancelada = True

                # 1. Verifica se houve Evento 110111 vinculado
                if not is_cancelada and numero_nota in notas_canceladas_por_evento:
                    is_cancelada = True
                    log_msg(f"Nota {numero_nota}: Status CANCELADA via Evento 110111 pré-identificado.")

                # 2. Códigos técnicos e Tags específicas
                if not is_cancelada:
                    meta_sit = str(doc.get("Situacao", ""))
                    xml_sit = get_xml_text(root, ["cSitNFSe", "situacao", "sit", "cSitConf", "situacaoDFe", "Status", "cSit", "codSit", "cStat", "tipoEvento", "xMotivo", "Motivo", "xJust"])
                    subst_node = root.find(".//{*}nNFSeSubst") or root.find(".//{*}infSubst") or root.find(".//{*}idSubst") or root.find(".//{*}infCanc")
                    
                    cancel_codes = ["2", "3", "4", "9", "99", "101", "102", "110111", "201", "202", "135", "136", "155"]
                    
                    if meta_sit in cancel_codes or xml_sit in cancel_codes or subst_node is not None:
                        is_cancelada = True
                        log_msg(f"Nota {numero_nota}: Cancelamento detectado via Códigos técnicos (Meta: {meta_sit}, Tag: {xml_sit})")

                # 3. VERIFICAÇÃO FINAL NO PDF (Caso o XML falhe)
                pdf_content = None
                if doc_type == "nfse":
                    chave = doc.get("ChaveAcesso")
                    if chave:
                        pdf_content = service.download_pdf(chave)
                        # Se o PDF diz que está cancelada, então está cancelada.
                        if is_nota_cancelada_no_pdf(pdf_content, numero_nota):
                            is_cancelada = True

                if not is_cancelada:
                    log_msg(f"Nota {numero_nota}: NENHUM sinal de cancelamento encontrado. Processando como ATIVA.")
                
                nome_outra_parte = nome_tomador
                cnpj_outra_parte = cnpj_tomador
                
                # Se o CNPJ do tomador for o da FECD, então a FECD é a TOMADORA (Despesa)
                if clean_cnpj_toma and meu_cnpj and clean_cnpj_toma == meu_cnpj:
                    log_msg(f"Ignorando nota {numero_nota}: É uma nota TOMADA (Despesa/Entrada).")
                    continue
                
                # Se passamos pelo filtro acima, é uma nota Prestada (Receita)
                nota_tipo = "Prestada"
                nome_outra_parte = nome_tomador
                cnpj_outra_parte = cnpj_tomador
                
                # Vínculo cliente/fornecedor
                tomador_id = None
                clean_cnpj_outra = ''.join(filter(str.isdigit, cnpj_outra_parte)) if cnpj_outra_parte else ""
                for c in clientes:
                    if clean_cnpj_outra and clean_cnpj_outra == ''.join(filter(str.isdigit, str(c.get('documento', '')))):
                        tomador_id = c['id']
                        break
                
                if not tomador_id:
                    if not nome_outra_parte:
                        log_msg(f"Aviso Nota {numero_nota}: O XML não contem a tag de Nome. Usando Fallback.")
                        nome_outra_parte = "Cliente/Fornecedor Não Identificado" # Fallback de emergência

                    try:
                        # Dupla checagem direto no banco para evitar conflitos/vazio
                        exc_cli = supabase.table("clientes").select("*").eq("documento", cnpj_outra_parte).execute()
                        if exc_cli.data:
                            tomador_id = exc_cli.data[0]['id']
                            clientes.append(exc_cli.data[0])
                        else:
                            tipo_cadastro = "fornecedor" if nota_tipo == "Tomada" else "cliente"
                            c_res = supabase.table("clientes").insert({"nome_razao": nome_outra_parte, "documento": cnpj_outra_parte, "tipo": tipo_cadastro}).execute()
                            if c_res.data:
                                tomador_id = c_res.data[0]['id']
                                clientes.append(c_res.data[0])
                            else:
                                log_msg(f"Nota {numero_nota}: Insert do Cliente falhou silenciosamente. CNPJ: {cnpj_outra_parte}")

                    except Exception as e:
                        log_msg(f"Nota {numero_nota}: Erro Crítico do BD Supabase ao criar Cliente {cnpj_tomador} - {str(e)}")

                current_client = next((c for c in clientes if c['id'] == tomador_id), None)
                projeto_id = None
                if current_client and current_client.get('projeto_padrao_id'):
                    projeto_id = current_client['projeto_padrao_id']
                if not projeto_id:
                    projeto_id = projetos[0]['id'] if projetos else None
                
                if tomador_id and projeto_id:
                    nota_db = {
                        "numero": numero_nota,
                        "data_emissao": data_emi[:10] if data_emi else None,
                        "tomador_id": tomador_id,
                        "projeto_id": projeto_id,
                        "valor": 0.0 if is_cancelada else valor_bruto, # Zera o valor para sair dos somatórios
                        "iss": 5.0,
                        "tipo": nota_tipo,
                        "status": "Cancelada" if is_cancelada else "Emitida"
                    }
                    try:
                        supabase.table("notas").upsert(nota_db, on_conflict="numero,tomador_id").execute()
                        importados_sucesso += 1
                        log_msg(f"Nota {numero_nota} ({nome_outra_parte}) importada/atualizada com sucesso no BD.")
                        # OneDrive
                        folder = f"Sincronizacao-{mes.replace('/', '-')}"
                        onedrive.upload_file(xml_content.encode('utf-8'), f"{numero_nota}.xml", subfolder=folder)
                        
                        # Upload do PDF (Se já baixou para o teste de cancelamento ou se baixar agora)
                        if not pdf_content and doc_type == "nfse":
                             pdf_content = service.download_pdf(doc.get("ChaveAcesso"))
                        
                        if pdf_content:
                            onedrive.upload_file(pdf_content, f"{numero_nota}.pdf", subfolder=folder)
                            
                    except Exception as db_err: 
                        log_msg(f"Erro ao inserir nota {numero_nota} no banco: {db_err}")
                else:
                    if not projeto_id:
                        log_msg(f"Nota {numero_nota} abortada: Seu sistema não possui um Projeto padrão cadastrado na nuvem.")
                    if not tomador_id:
                        log_msg(f"Nota {numero_nota} abortada: Falha ao criar/vincular cliente com CNPJ {cnpj_outra_parte}")
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

class RenameRequest(BaseModel):
    ids: List[str]

@app.post("/notas/rename")
async def renomear_notas(req: RenameRequest):
    if not req.ids:
        return {"status": "vazio"}
    
    # Busca notas com tomadores e projetos
    notas_res = supabase.table("notas")\
        .select("*, tomador:clientes(nome_razao), projeto:projetos(nome)")\
        .in_("id", req.ids)\
        .execute()
    
    if not notas_res.data:
        raise HTTPException(status_code=404, detail="Nenhuma nota encontrada")
    
    renamed_count = 0
    errors = []
    
    for n in notas_res.data:
        try:
            # Formato: 45 [04-02-2026] CRISTALIA - PROJETO - 16.071,89
            num = n.get("numero")
            dt_raw = n.get("data_emissao") # yyyy-mm-dd
            dt_str = "01-01-2026"
            subfolder = ""
            if dt_raw:
                y, m, d = dt_raw.split("-")
                dt_str = f"{d}-{m}-{y}"
                subfolder = f"Sincronizacao-{m}-{y}"
            
            cliente = n.get("tomador", {}).get("nome_razao", "CLIENTE") if n.get("tomador") else "CLIENTE"
            projeto = n.get("projeto", {}).get("nome", "PROJETO") if n.get("projeto") else "PROJETO"
            valor = n.get("valor", 0)
            valor_fmt = "{:,.2f}".format(valor).replace(",", "X").replace(".", ",").replace("X", ".")
            
            new_filename_base = f"{num} [{dt_str}] {cliente} - {projeto} - {valor_fmt}"
            
            # Tentativas para PDF e XML
            exts = [".pdf", ".xml"]
            found_something = False
            for ext in exts:
                old_name = f"{num}{ext}"
                new_name = f"{new_filename_base}{ext}"
                
                # Tenta renomear na pasta original
                ok = onedrive.rename_file(old_name, new_name, subfolder)
                if not ok:
                    # Se falhar, tenta na raiz (fallback)
                    ok = onedrive.rename_file(old_name, new_name, "")
                
                if ok: 
                    found_something = True

            if found_something:
                renamed_count += 1
            else:
                # Se não renomeou nada, talvez já esteja renomeado ou arquivo não existe
                pass
                
        except Exception as e:
            errors.append(f"Erro na nota {n.get('numero')}: {str(e)}")
            
    return {
        "status": "concluido",
        "total": len(req.ids),
        "renomeados": renamed_count,
        "erros": errors
    }

@app.get("/notas/{numero}/link")
async def obter_link_nota(numero: str, mes: str = ""):
    # mes formato esperado: "02-2026"
    subfolder = f"Sincronizacao-{mes}" if mes else ""
    
    # 1. Tenta buscar pelo nome PADRÃO (Numero.pdf/xml)
    filename_pdf = f"{numero}.pdf"
    url = onedrive.get_file_link(filename_pdf, subfolder)
    
    if not url:
        filename_xml = f"{numero}.xml"
        url = onedrive.get_file_link(filename_xml, subfolder)

    # 2. Se não achou, tenta buscar pelo nome RENOMEADO
    if not url:
        # Busca detalhes da nota no Supabase para montar o nome
        not_res = supabase.table("notas")\
            .select("*, tomador:clientes(nome_razao), projeto:projetos(nome)")\
            .eq("numero", numero)\
            .execute()
        
        if not_res.data:
            n = not_res.data[0]
            dt_raw = n.get("data_emissao") # yyyy-mm-dd
            dt_str = "01-01-2026"
            if dt_raw:
                y, m, d = dt_raw.split("-")
                dt_str = f"{d}-{m}-{y}"
            
            cliente = n.get("tomador", {}).get("nome_razao", "CLIENTE") if n.get("tomador") else "CLIENTE"
            projeto = n.get("projeto", {}).get("nome", "PROJETO") if n.get("projeto") else "PROJETO"
            valor = n.get("valor", 0)
            valor_fmt = "{:,.2f}".format(valor).replace(",", "X").replace(".", ",").replace("X", ".")
            
            new_filename_base = f"{numero} [{dt_str}] {cliente} - {projeto} - {valor_fmt}"
            
            # Tenta PDF renomeado
            url = onedrive.get_file_link(f"{new_filename_base}.pdf", subfolder)
            if not url:
                # Tenta XML renomeado
                url = onedrive.get_file_link(f"{new_filename_base}.xml", subfolder)

    # Fallback raizes (Legacy)
    if not url and subfolder:
        url = onedrive.get_file_link(f"{numero}.pdf", "")
        if not url: url = onedrive.get_file_link(f"{numero}.xml", "")

    if url:
        return {"url": url}
        
    raise HTTPException(status_code=404, detail="Nota não encontrada no OneDrive")

@app.get("/debug/onedrive")
async def debug_onedrive(folder: str = ""):
    from urllib.parse import quote
    import requests
    token = onedrive._get_token()
    if not token: return {"error": "no token"}
    
    remote_path = f"{onedrive.remote_root}/{folder}" if folder else onedrive.remote_root
    safe_path = quote(remote_path)
    url = f"https://graph.microsoft.com/v1.0/users/{onedrive.user_id}/drive/root:/{safe_path}:/children"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return {"files": [f["name"] for f in resp.json().get("value", [])]}
    return {"error": resp.status_code, "msg": resp.text}

