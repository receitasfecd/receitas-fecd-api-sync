import os
import base64
import gzip
import tempfile
import requests
from contextlib import contextmanager
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives import serialization
from cryptography import x509

class NFSeService:
    def __init__(self, pfx_data, pfx_password):
        """
        pfx_data: bytes of the .pfx file
        pfx_password: string
        """
        self.pfx_data = pfx_data
        self.pfx_password = pfx_password
        self.base_url = "https://adn.nfse.gov.br"
        self.cnpj = None
        self._extract_cnpj()

    def _extract_cnpj(self):
        try:
            pwd = self.pfx_password.encode() if isinstance(self.pfx_password, str) else self.pfx_password
            _, certificate, _ = pkcs12.load_key_and_certificates(self.pfx_data, pwd)
            
            for attribute in certificate.subject:
                if attribute.oid == x509.NameOID.COMMON_NAME:
                    value = attribute.value
                    parts = value.split(':')
                    for part in parts:
                        clean = ''.join(filter(str.isdigit, part))
                        if len(clean) == 14:
                            self.cnpj = clean
                            return
        except Exception as e:
            print(f"Erro ao extrair CNPJ: {e}")

    @contextmanager
    def _create_pem_context(self):
        tmp_pem = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)
        pem_path = tmp_pem.name
        tmp_pem.close()

        try:
            pwd = self.pfx_password.encode() if isinstance(self.pfx_password, str) else self.pfx_password
            private_key, certificate, additional_certs = pkcs12.load_key_and_certificates(self.pfx_data, pwd)
            
            with open(pem_path, 'wb') as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption()
                ))
                f.write(certificate.public_bytes(serialization.Encoding.PEM))
                if additional_certs:
                    for ca in additional_certs:
                        f.write(ca.public_bytes(serialization.Encoding.PEM))
            
            yield pem_path
        finally:
            if os.path.exists(pem_path):
                try: os.remove(pem_path)
                except: pass

    def test_connection(self):
        try:
            with self._create_pem_context() as cert_path:
                response = requests.get(self.base_url, cert=cert_path, timeout=15, verify=False)
                return {"success": True, "message": f"Conexão OK. Status: {response.status_code}"}
        except Exception as e:
            return {"success": False, "message": f"Falha na conexão: {str(e)}"}

    def fetch_dfe(self, nsu=0):
        if not self.cnpj:
            return {"success": False, "error": "CNPJ não identificado no certificado."}

        nsu_padded = f"{nsu:020d}"
        url = f"{self.base_url}/contribuintes/DFe/{nsu_padded}"
        params = {"cnpj": self.cnpj}
        headers = {"User-Agent": "NFSeApp/2.0", "Accept": "application/json"}

        try:
            with self._create_pem_context() as cert_path:
                response = requests.get(url, cert=cert_path, params=params, headers=headers, timeout=30, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                processed_lote = []
                for doc in data.get("LoteDFe", []):
                    xml_b64 = doc.get("ArquivoXml")
                    if xml_b64:
                        try:
                            xml_bytes = base64.b64decode(xml_b64)
                            try:
                                doc["xml_decoded"] = gzip.decompress(xml_bytes).decode('utf-8')
                            except:
                                doc["xml_decoded"] = xml_bytes.decode('utf-8')
                        except Exception as e:
                            doc["decode_error"] = str(e)
                    processed_lote.append(doc)
                
                data["LoteDFe"] = processed_lote
                return {"success": True, "data": data}
            elif response.status_code == 204:
                return {"success": True, "data": None, "message": "Sem novos documentos."}
            else:
                return {"success": False, "error": f"HTTP {response.status_code}", "details": response.text}
        except Exception as e:
            return {"success": False, "error": f"Erro na requisição: {str(e)}"}

    def download_pdf(self, chave):
        url = f"{self.base_url}/danfse/{chave}"
        try:
            with self._create_pem_context() as cert_path:
                response = requests.get(
                    url, 
                    cert=cert_path, 
                    headers={"Accept": "application/pdf", "User-Agent": "Mozilla/5.0"},
                    timeout=30, 
                    verify=False
                )
                if response.status_code == 200 and b'%PDF' in response.content[:10]:
                    return response.content
        except:
            pass
        return None
