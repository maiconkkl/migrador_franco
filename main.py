import socket
from lxml import objectify
from pymongo import MongoClient
from bson import ObjectId, Regex
from datetime import datetime


class Migrador:
    client: MongoClient = None
    host = ''
    username = 'root'
    password = '|cSFu@5rFv#h8*='
    connectTimeoutMS = 10000
    authMechanism = 'SCRAM-SHA-1'
    authSource = 'admin'
    serverSelectionTimeoutMS = 5000
    port = 12220
    database = None

    client1: MongoClient = None
    host1 = '127.0.0.1'
    username1 = 'root'
    password1 = '|cSFu@5rFv#h8*='
    connectTimeoutMS1 = 10000
    authMechanism1 = 'SCRAM-SHA-1'
    authSource1 = 'admin'
    serverSelectionTimeoutMS1 = 5000
    port1 = 12220
    database1 = None

    def __init__(self):
        f = open(r'C:\DigiSat\SuiteG6\Sistema\ConfiguracaoClient.xml', 'r')
        data = f.read()
        f.close()
        data = data.replace('<?xml version="1.0" encoding="utf-8"?>\n', '')
        data = data.replace('ï»¿', '')
        xml = objectify.fromstring(data)
        self.host = str(xml.Ip) if hasattr(xml, 'Ip') else '127.0.0.1'
        try:
            socket.inet_aton(self.host)
        except socket.error:
            try:
                self.host = socket.gethostbyname(self.host)
            except:
                self.host = '127.0.0.1'

        self.client = MongoClient(
            host=self.host,
            username=self.username,
            password=self.password,
            authSource=self.authSource,
            port=self.port,
            serverSelectionTimeoutMS=self.serverSelectionTimeoutMS,
            connectTimeoutMS=self.connectTimeoutMS,
            authMechanism=self.authMechanism
        )
        self.database = self.client['DigisatServer']

        self.client1 = MongoClient(
            host=self.host1,
            username=self.username1,
            password=self.password1,
            authSource=self.authSource1,
            port=self.port1,
            serverSelectionTimeoutMS=self.serverSelectionTimeoutMS1,
            connectTimeoutMS=self.connectTimeoutMS1,
            authMechanism=self.authMechanism1
        )
        self.database1 = self.client1['parse']

        self.migrar_grupo()
        self.migrar_tributacao_federal()
        self.migrar_pessoas()
        self.migrar_tributacao_estadual()
        self.migrar_unidades_medida()
        self.migrar_produtos()

    def fecha_conexao(self):
        self.client.close()
        return True

    def localizar_empresa_destino(self, cnpj=None, ie=None):
        collection_pessoa = self.database1["Pessoa"]
        query = {"tipoCadastro": Regex(u".*emitente.*", "i"), "ativo": True}
        if cnpj is not None:
            query["documento.cnpj.documento"] = cnpj

        if ie is not None:
            query["documento.ie.documento"] = ie

        pessoa_cursor = collection_pessoa.find_one(query)

        collection_empresa = self.database1["Empresa"]
        query = {"_p_pessoa": u"Pessoa$" + pessoa_cursor['_id']}
        empresa_cursor = collection_empresa.find(query)
        return empresa_cursor

    def migrar_grupo(self):
        empresas = self.localizar_empresa_destino()
        grupos_collection = self.database["Grupos"]
        grupo_collection = self.database1["Grupo"]
        cursor = grupos_collection.find({})

        for doc in cursor:

            modelo = {
                "_id": "",
                "ativo": doc['Ativo'],
                "nome": doc['Descricao'],
                "_p_empresa": "",
                "_created_at": datetime.now(),
                "_updated_at": datetime.now()
            }
            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo['_p_empresa'] = "Empresa$" + empresa['_id']
                grupo_collection.insert_one(modelo)
                for sub_grupo in doc['SubGruposReferencia']:
                    self.migrar_subgrupo(sub_grupo, modelo['_id'])

    def migrar_subgrupo(self, sub_grupo_id, grupo_id):
        sub_grupos_collection = self.database["SubGrupos"]
        sub_grupo_collection = self.database1["Subgrupo"]
        query = {"_id": sub_grupo_id}
        sub_grupo_cursor = sub_grupos_collection.find_one(query)
        modelo = {
            "_id": str(sub_grupo_id),
            "ativo": sub_grupo_cursor['Ativo'],
            "nome": sub_grupo_cursor['Descricao'],
            "_p_grupo": "Grupo$" + grupo_id,
            "_created_at": datetime.now(),
            "_updated_at": datetime.now()
        }
        sub_grupo_collection.insert_one(modelo)

    def migrar_pessoas(self):
        pessoas_collection = self.database["Pessoas"]
        pessoa_collection = self.database1["Pessoa"]
        empresas = self.localizar_empresa_destino()

        cursor = pessoas_collection.find({})
        for doc in cursor:
            pessoa_id = str(ObjectId())
            modelo = {"_id": pessoa_id}
            if 'Classificacao' in doc:
                modelo["classificacao"] = doc['Classificacao']['_t'].lower()
            if 'Cnpj' in doc:
                if 'documento' not in modelo:
                    modelo["documento"] = {}
                modelo["documento"]["cnpj"] = {
                    "tipo": "cnpj",
                    "documento": doc['Cnpj']
                }

            if 'CNAE' in doc:
                if 'documento' not in modelo:
                    modelo["documento"] = {}
                modelo["documento"]["cnae"] = {
                    "tipo": "cnae",
                    "documento": doc['CNAE']
                }

            if 'RegistroMunicipal' in doc:
                if 'documento' not in modelo:
                    modelo["documento"] = {}
                modelo["documento"]["im"] = {
                    "tipo": "im",
                    "documento": doc['RegistroMunicipal']
                }

            if 'Carteira' in doc:
                if 'Ie' in doc['Carteira']:
                    if 'documento' not in modelo:
                        modelo["documento"] = {}
                    modelo["documento"]["cnpj"] = {
                        "tipo": "ie",
                        "indicadorIE": "1",
                        "documento": doc['Carteira']['Ie']['Numero']
                    }

                modelo["endereco"] = {}
                if 'EnderecoPrincipal' in doc['Carteira']:
                    modelo["endereco"]['principal'] = {
                        "tipo": "principal",
                        "cep": doc['Carteira']['EnderecoPrincipal']['Cep'],
                        "bairro": doc['Carteira']['EnderecoPrincipal']['Bairro'],
                        "logradouro": doc['Carteira']['EnderecoPrincipal']['Logradouro'],
                        "uf": doc['Carteira']['EnderecoPrincipal']['Municipio']['Uf']['Sigla'],
                        "codigoMunicipio": str(doc['Carteira']['EnderecoPrincipal']['Municipio']['CodigoIbge']),
                        "municipio": doc['Carteira']['EnderecoPrincipal']['Municipio']['Nome'],
                        "numero": doc['Carteira']['EnderecoPrincipal']['Numero']
                    }
                modelo["contato"] = []
                if 'TelefonePrincipal' in doc['Carteira']:
                    contato = {
                        "tipo": "telefone",
                        "contato": doc['Carteira']['TelefonePrincipal']['Numero']
                    }
                    modelo["contato"].append(contato)

                if 'TelefoneComercial' in doc['Carteira']:
                    contato = {
                        "tipo": "telefone",
                        "contato": doc['Carteira']['TelefoneComercial']['Numero']
                    }
                    modelo["contato"].append(contato)

                if 'TelefoneFax' in doc['Carteira']:
                    contato = {
                        "tipo": "telefone",
                        "contato": doc['Carteira']['TelefoneFax']['Numero']
                    }
                    modelo["contato"].append(contato)

                if 'TelefoneResidencial' in doc['Carteira']:
                    contato = {
                        "tipo": "telefone",
                        "contato": doc['Carteira']['TelefoneResidencial']['Numero']
                    }
                    modelo["contato"].append(contato)

                if 'TelefoneWhatsApp' in doc['Carteira']:
                    contato = {
                        "tipo": "telefone",
                        "contato": doc['Carteira']['TelefoneWhatsApp']['Numero']
                    }
                    modelo["contato"].append(contato)

                if 'Celulares' in doc['Carteira']:
                    for celular in doc['Carteira']['Celulares']:
                        contato = {
                            "tipo": "telefone",
                            "contato": celular['Numero']
                        }
                        modelo["contato"].append(contato)

            modelo["nome"] = doc['Nome']
            if 'NomeFantasia' in doc:
                modelo["fantasia"] = doc['NomeFantasia']
            modelo["tipoCadastro"] = []
            if 'Fornecedor' in doc:
                modelo["tipoCadastro"].append('fornecedor')
            if 'Cliente' in doc:
                modelo["tipoCadastro"].append('cliente')
            if 'Juridica' in doc['_t']:
                modelo["tipo"] = 'juridica'
            elif 'Fisica' in doc['_t']:
                modelo["tipo"] = 'fisica'
            modelo["_created_at"] = datetime.now()
            modelo["_updated_at"] = datetime.now()

            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo["_p_empresa"] = "Empresa$" + empresa['_id']
                pessoa_collection.insert_one(modelo)

    def migrar_tributacao_federal(self):
        trib_federal_collection = self.database["TributacoesFederal"]
        trib_federal1_collection = self.database1["TributacaoFederal"]
        empresas = self.localizar_empresa_destino()
        pipeline = [
            {
                u"$lookup": {
                    u"from": u"TributacoesIPI",
                    u"localField": u"TributacaoIpiReferencia",
                    u"foreignField": u"_id",
                    u"as": u"TributacoesIPI"
                }
            },
            {
                u"$lookup": {
                    u"from": u"TributacoesPISCOFINS",
                    u"localField": u"TributacaoPisCofinsReferencia",
                    u"foreignField": u"_id",
                    u"as": u"TributacoesPISCOFINS"
                }
            }
        ]

        trib_federal_cursor = trib_federal_collection.aggregate(
            pipeline,
            allowDiskUse=False
        )

        for doc in trib_federal_cursor:
            ipi_entrada = doc['TributacoesIPI'][0]['IpiEntrada']
            ipi_saida = doc['TributacoesIPI'][0]['IpiSaida']
            pis_entrada = doc['TributacoesPISCOFINS'][0]['PISEntrada']
            pis_saida = doc['TributacoesPISCOFINS'][0]['PISSaida']
            cst_pis_entrada = pis_entrada['CstPISCOFINS']['Codigo']
            cst_pis_saida = pis_saida['CstPISCOFINS']['Codigo']

            cofins_entrada = doc['TributacoesPISCOFINS'][0]['COFINSEntrada']
            cofins_saida = doc['TributacoesPISCOFINS'][0]['COFINSSaida']
            cst_cofins_entrada = cofins_entrada['CstPISCOFINS']['Codigo']
            cst_cofins_saida = cofins_saida['CstPISCOFINS']['Codigo']

            modelo = {
                "_id": "",
                "ativo": doc['Ativo'],
                "nome": doc['Descricao'],
                "descricao": doc['Descricao'],
                "ipi": {
                    "entrada": {
                        "tipo": "entrada",
                        "cst": str(ipi_entrada['CstIPI']['Codigo']).zfill(2),
                        "codigoEnquadramento": str(ipi_entrada['CodigoEnquadramentoIPI']['Codigo']),
                    },
                    "saida": {
                        "tipo": "saida",
                        "cst": str(ipi_saida['CstIPI']['Codigo']).zfill(2),
                        "codigoEnquadramento": str(ipi_saida['CodigoEnquadramentoIPI']['Codigo']),
                    }
                },
                "pis": {
                    "entrada": {
                        "tipo": "entrada",
                        "cst": str(cst_pis_entrada).zfill(2),
                    },
                    "saida": {
                        "tipo": "saida",
                        "cst": str(cst_pis_saida).zfill(2),
                    }
                },
                "cofins": {
                    "entrada": {
                        "tipo": "entrada",
                        "cst": str(cst_cofins_entrada).zfill(2),
                    },
                    "saida": {
                        "tipo": "saida",
                        "cst": str(cst_cofins_saida).zfill(2),
                    }
                },
                "_p_empresa": ""
            }
            # IPI Entrada
            if 'PercentualBaseCalculo' in ipi_entrada:
                modelo["ipi"]["entrada"]["baseCalculo"] = ipi_entrada['PercentualBaseCalculo']

            if 'Percentual' in ipi_entrada:
                modelo["ipi"]["entrada"]["percentual"] = ipi_entrada['Percentual']
                modelo["ipi"]["entrada"]["valorUnidade"] = 0
                modelo["ipi"]["entrada"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidadeIpi' in ipi_entrada:
                modelo["ipi"]["entrada"]["percentual"] = 0
                modelo["ipi"]["entrada"]["valorUnidade"] = ipi_entrada['ValorUnidadeIpi']
                modelo["ipi"]["entrada"]["tipoCalculo"] = 'unidade'

            # IPI Saida
            if 'PercentualBaseCalculo' in ipi_saida:
                modelo["ipi"]["saida"]["baseCalculo"] = ipi_saida['PercentualBaseCalculo']

            if 'Percentual' in ipi_saida:
                modelo["ipi"]["saida"]["percentual"] = ipi_saida['Percentual']
                modelo["ipi"]["saida"]["valorUnidade"] = 0
                modelo["ipi"]["saida"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidadeIpi' in ipi_saida:
                modelo["ipi"]["saida"]["percentual"] = 0
                modelo["ipi"]["saida"]["valorUnidade"] = ipi_saida['ValorUnidadeIpi']
                modelo["ipi"]["saida"]["tipoCalculo"] = 'unidade'

            # PIS Entrada
            if 'PercentualBaseCalculo' in pis_entrada:
                modelo["pis"]["entrada"]["baseCalculo"] = 0
                modelo["pis"]["entrada"]["baseCalculoST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_pis_entrada in [75]:
                    modelo["pis"]["entrada"]["baseCalculoST"] = pis_entrada['PercentualBaseCalculo']
                else:
                    modelo["pis"]["entrada"]["baseCalculo"] = pis_entrada['PercentualBaseCalculo']

            if 'Percentual' in pis_entrada:
                modelo["pis"]["entrada"]["percentual"] = 0
                modelo["pis"]["entrada"]["percentualST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_pis_entrada in [75]:
                    modelo["pis"]["entrada"]["percentualST"] = pis_entrada['Percentual']
                else:
                    modelo["pis"]["entrada"]["percentual"] = pis_entrada['Percentual']
                modelo["pis"]["entrada"]["valorUnidade"] = 0
                modelo["pis"]["entrada"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidade' in pis_entrada:
                modelo["pis"]["entrada"]["percentual"] = 0
                modelo["pis"]["entrada"]["percentualST"] = 0
                modelo["pis"]["entrada"]["valorUnidade"] = pis_entrada['ValorUnidade']
                modelo["pis"]["entrada"]["tipoCalculo"] = 'unidade'

            # PIS Saida
            if 'PercentualBaseCalculo' in pis_saida:
                modelo["pis"]["saida"]["baseCalculo"] = 0
                modelo["pis"]["saida"]["baseCalculoST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_pis_entrada in [75]:
                    modelo["pis"]["saida"]["baseCalculoST"] = pis_saida['PercentualBaseCalculo']
                else:
                    modelo["pis"]["saida"]["baseCalculo"] = pis_saida['PercentualBaseCalculo']

            if 'Percentual' in pis_saida:
                modelo["pis"]["saida"]["percentual"] = 0
                modelo["pis"]["saida"]["percentualST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_pis_saida in [5]:
                    modelo["pis"]["saida"]["percentualST"] = pis_saida['Percentual']
                else:
                    modelo["pis"]["saida"]["percentual"] = pis_saida['Percentual']
                modelo["pis"]["saida"]["valorUnidade"] = 0
                modelo["pis"]["saida"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidade' in pis_saida:
                modelo["pis"]["saida"]["percentual"] = 0
                modelo["pis"]["saida"]["percentualST"] = 0
                modelo["pis"]["saida"]["valorUnidade"] = pis_saida['ValorUnidade']
                modelo["pis"]["saida"]["tipoCalculo"] = 'unidade'

            # COFINS Entrada
            if 'PercentualBaseCalculo' in cofins_entrada:
                modelo["cofins"]["entrada"]["baseCalculo"] = 0
                modelo["cofins"]["entrada"]["baseCalculoST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_cofins_entrada in [75]:
                    modelo["cofins"]["entrada"]["baseCalculoST"] = cofins_entrada['PercentualBaseCalculo']
                else:
                    modelo["cofins"]["entrada"]["baseCalculo"] = cofins_entrada['PercentualBaseCalculo']

            if 'Percentual' in cofins_entrada:
                modelo["cofins"]["entrada"]["percentual"] = 0
                modelo["cofins"]["entrada"]["percentualST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_cofins_entrada in [75]:
                    modelo["cofins"]["entrada"]["percentualST"] = cofins_entrada['Percentual']
                else:
                    modelo["cofins"]["entrada"]["percentual"] = cofins_entrada['Percentual']
                modelo["cofins"]["entrada"]["valorUnidade"] = 0
                modelo["cofins"]["entrada"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidade' in cofins_entrada:
                modelo["cofins"]["entrada"]["percentual"] = 0
                modelo["cofins"]["entrada"]["percentualST"] = 0
                modelo["cofins"]["entrada"]["valorUnidade"] = cofins_entrada['ValorUnidade']
                modelo["cofins"]["entrada"]["tipoCalculo"] = 'unidade'

            # COFINS Saida
            if 'PercentualBaseCalculo' in cofins_saida:
                modelo["cofins"]["saida"]["baseCalculo"] = 0
                modelo["cofins"]["saida"]["baseCalculoST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_cofins_entrada in [75]:
                    modelo["cofins"]["saida"]["baseCalculoST"] = cofins_saida['PercentualBaseCalculo']
                else:
                    modelo["cofins"]["saida"]["baseCalculo"] = cofins_saida['PercentualBaseCalculo']

            if 'Percentual' in cofins_saida:
                modelo["cofins"]["saida"]["percentual"] = 0
                modelo["cofins"]["saida"]["percentualST"] = 0

                # verificando se bota tags de substituicao ou as normais
                if cst_cofins_saida in [5]:
                    modelo["cofins"]["saida"]["percentualST"] = cofins_saida['Percentual']
                else:
                    modelo["cofins"]["saida"]["percentual"] = cofins_saida['Percentual']
                modelo["cofins"]["saida"]["valorUnidade"] = 0
                modelo["cofins"]["saida"]["tipoCalculo"] = 'percentual'

            if 'ValorUnidade' in cofins_saida:
                modelo["cofins"]["saida"]["percentual"] = 0
                modelo["cofins"]["saida"]["percentualST"] = 0
                modelo["cofins"]["saida"]["valorUnidade"] = cofins_saida['ValorUnidade']
                modelo["cofins"]["saida"]["tipoCalculo"] = 'unidade'

            modelo["_created_at"] = datetime.now()
            modelo["_updated_at"] = datetime.now()
            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo["_p_empresa"] = "Empresa$" + empresa['_id']
                trib_federal1_collection.insert_one(modelo)

    def migrar_tributacao_estadual(self):
        empresas = self.localizar_empresa_destino()
        trib_estadual_colletion = self.database["TributacoesEstadual"]
        trib_estadual1_colletion = self.database["TributacaoEstadual"]
        op_fiscal_colletion = self.database["OperacoesFiscais"]
        cursor = trib_estadual_colletion.find({})

        for doc in cursor:
            nao_contribuinte = []
            contribuinte = []
            industria = []
            publico = []
            for uf_tributacao in doc['UfsTributacao']:
                sigla_uf = uf_tributacao['Uf']['Sigla']
                trib: dict = {}
                modelo_percentual = {
                    sigla_uf: {
                        "bc": 0,  # OK
                        "tipo": "",  # OK
                        "fcpRetido": 0,  # Não identificado
                        "diferimento": 0,  # Ignorar dito por Rick
                        "cst": "",  # OK
                        "creditoIcmsSN": 0,  # OK
                        "origem": "",  # OK
                        "cfop": "",  # OK
                        "bcST": 0,  # OK
                        "bcOperacaoPropria": 0,  # Não usado dito por gabriel
                        "uf": "",  # OK
                        "icms": 0,  # OK
                        "icmsInterno": 0,  # OK
                        "fcp": 0,  # OK
                        "fcpRetidoST": 0,  # Não identificado
                        "icmsEfetivo": 0,  # OK
                        "icmsST": 0,
                        "bcIcmsEfetivo": 0,  # Não usado dito por gabriel
                        "fcpUFDestino": 0,  # OK
                        "icmsInterestadual": 0,  # Não usado dito por gabriel
                        "fcpST": 0,  # OK
                        "mva": 0,  # OK
                        "icmsRetidoST": 0  # OK
                    }
                }
                if 'NaoContribuinte' in uf_tributacao:
                    trib = uf_tributacao['NaoContribuinte']
                    modelo_percentual[sigla_uf]['tipo'] = "naoContribuinte"
                elif 'Contribuinte' in uf_tributacao:
                    trib = uf_tributacao['Contribuinte']
                    modelo_percentual[sigla_uf]['tipo'] = "contribuinte"
                elif 'Industria' in uf_tributacao:
                    trib = uf_tributacao['Industria']
                    modelo_percentual[sigla_uf]['tipo'] = "industria"
                elif 'Publico' in uf_tributacao:
                    trib = uf_tributacao['Publico']
                    modelo_percentual[sigla_uf]['tipo'] = "publico"
                modelo_percentual[sigla_uf]['uf'] = sigla_uf
                modelo_percentual[sigla_uf]['cst'] = str(trib['SituacaoTributaria']['Codigo']).zfill(2)
                modelo_percentual[sigla_uf]['origem'] = str(trib['OrigemMercadoria']['Codigo'])
                op_fiscal = op_fiscal_colletion.find_one({"_id": trib['OperacaoFiscalReferencia']})
                modelo_percentual[sigla_uf]['cfop'] = str(op_fiscal['Cfop']['Codigo'])

                if 'PercentualSimplesNacional' in trib:
                    modelo_percentual[sigla_uf]['creditoIcmsSN'] = trib['PercentualSimplesNacional']

                if 'PercentualIcms' in trib:
                    modelo_percentual[sigla_uf]['icms'] = trib['PercentualIcms']

                if 'PercentualInterno' in trib:
                    modelo_percentual[sigla_uf]['icmsInterno'] = trib['PercentualInterno']

                if 'PercentualIcmsEfetivo' in trib:
                    modelo_percentual[sigla_uf]['icmsEfetivo'] = trib['PercentualIcmsEfetivo']

                if 'PercentualIcms' in trib:
                    modelo_percentual[sigla_uf]['icmsEfetivo'] = trib['PercentualIcms']

                if 'PercentualBaseCalculo' in trib:
                    modelo_percentual[sigla_uf]['bc'] = trib['PercentualBaseCalculo']

                # ST
                if 'PercentualSubstituicaoTributaria' in trib:
                    modelo_percentual[sigla_uf]['icmsST'] = trib['PercentualSubstituicaoTributaria']

                if 'PercentualMva' in trib:
                    modelo_percentual[sigla_uf]['mva'] = trib['PercentualMva']

                if 'PercentualBaseCalculoSt' in trib:
                    modelo_percentual[sigla_uf]['bcST'] = trib['PercentualBaseCalculoSt']

                # FCP
                if 'PercentualFundoCombatePobrezaInterno' in trib:
                    modelo_percentual[sigla_uf]['fcp'] = trib['PercentualFundoCombatePobrezaInterno']

                if 'PercentualFundoCombatePobrezaStInterno' in trib:
                    modelo_percentual[sigla_uf]['fcpST'] = trib['PercentualFundoCombatePobrezaStInterno']

                if 'PercentualFundoCombatePobreza' in trib:
                    modelo_percentual[sigla_uf]['fcpUFDestino'] = trib['PercentualFundoCombatePobreza']

                # Retenção
                if 'ValorIcmsStRetido' in trib:
                    modelo_percentual[sigla_uf]['icmsRetidoST'] = trib['ValorIcmsStRetido']

                if 'NaoContribuinte' in uf_tributacao:
                    nao_contribuinte.append(modelo_percentual)
                elif 'Contribuinte' in uf_tributacao:
                    contribuinte.append(modelo_percentual)
                elif 'Industria' in uf_tributacao:
                    industria.append(modelo_percentual)
                elif 'Publico' in uf_tributacao:
                    publico.append(modelo_percentual)
            modelo = {
                "_id": "",
                "_p_empresa": "",
                "percentual": {},
                "nome": doc['Descricao'],
                "ativo": True,
                "_created_at": datetime.now(),
                "_updated_at": datetime.now(),
                "descricao": doc['Descricao'],
                "percentualUF": [
                    {
                        "uf": doc['Uf']['Sigla'],
                        "tipo": "naoContribuinte",
                        "observacao": "00",
                        "origem": "0",
                        "csosn": "102",
                        "cfop": "5102"
                    }
                ]
            }
            if len(nao_contribuinte) > 0:
                modelo['percentual']['naoContribuinte'] = nao_contribuinte

            if len(contribuinte) > 0:
                modelo['percentual']['contribuinte'] = contribuinte

            if len(industria) > 0:
                modelo['percentual']['industria'] = industria

            if len(publico) > 0:
                modelo['percentual']['industria'] = publico

            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo['_p_empresa'] = "Empresa$" + empresa['_id']
                trib_estadual1_colletion.insert_one(modelo)

    def migrar_unidades_medida(self):
        empresas = self.localizar_empresa_destino()
        uni_med_collection = self.database["UnidadesMedida"]
        uni_med1_collection = self.database1["UnidadeMedida"]

        cursor = uni_med_collection.find({})
        for doc in cursor:
            modelo = {
                "_id": "",
                "ativo": True,
                "nome": doc['Descricao'],
                "sigla": doc['Sigla'],
                "_p_empresa": "",
                "_created_at": datetime.now(),
                "_updated_at": datetime.now()
            }
            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo['_p_empresa'] = "Empresa$" + empresa['_id']
                uni_med1_collection.insert_one(modelo)

    def migrar_produtos(self):
        empresas = self.localizar_empresa_destino()
        produtos_collection = self.database["ProdutosServicosEmpresa"]
        produto_collection = self.database1["Produto"]
        sub_grupo_collection = self.database1["Subgrupo"]
        uni_med1_collection = self.database1["UnidadeMedida"]
        trib_federal1_collection = self.database1["TributacaoFederal"]
        pipeline = [
            {
                u"$lookup": {
                    u"from": u"ProdutosServicos",
                    u"localField": u"ProdutoServicoReferencia",
                    u"foreignField": u"_id",
                    u"as": u"ProdutosServicos"
                }
            },
            {
                u"$lookup": {
                    u"from": u"Precos",
                    u"localField": u"PrecoReferencia",
                    u"foreignField": u"_id",
                    u"as": u"Precos"
                }
            },
            {
                u"$lookup": {
                    u"from": u"Estoques",
                    u"localField": u"EstoqueReferencia",
                    u"foreignField": u"_id",
                    u"as": u"Estoques"
                }
            },
            {
                u"$lookup": {
                    u"from": u"TributacoesFederal",
                    u"localField": u"TributacaoFederalReferencia",
                    u"foreignField": u"_id",
                    u"as": u"TributacoesFederal"
                }
            },
            {
                u"$lookup": {
                    u"from": u"TributacoesEstadual",
                    u"localField": u"TributacaoEstadualReferencia",
                    u"foreignField": u"_id",
                    u"as": u"TributacoesEstadual"
                }
            },
            {
                u"$lookup": {
                    u"from": u"SubGrupos",
                    u"localField": u"ProdutosServicos.0.SubGrupoReferencia",
                    u"foreignField": u"_id",
                    u"as": u"SubGrupos"
                }
            }
        ]

        cursor = produtos_collection.aggregate(
            pipeline,
            allowDiskUse=False
        )
        for doc in cursor:
            query = {
                "ativo": doc['SubGrupos'][0]['Ativo'],
                "nome": doc['SubGrupos'][0]['Descricao']
            }
            sub_grupo = sub_grupo_collection.find_one(query)

            query = {
                "sigla": doc['ProdutosServicos'][0]['UnidadeMedida']['Sigla']
            }
            unidade_medida = uni_med1_collection.find_one(query)

            query = {
                "ativo": doc['TributacoesFederal'][0]['Ativo'],
                "nome": doc['TributacoesFederal'][0]['Descricao']
            }
            tributacao_federal = trib_federal1_collection.find_one(query)

            modelo = {
                "precoAtacado": 0,
                "precoCustoMedio": 0,
                "precoCusto": doc['Precos'][0]['Custo']['Valor'],
                "precoCustoReal": 0,
                "_p_unidadeMedidaTributavel": "UnidadeMedida$"+unidade_medida['_id'],
                "preco": doc['Precos'][0]['Venda']['Valor'],
                "ativo": doc['Ativo'],
                "_p_subgrupo": "Subgrupo$"+sub_grupo['_id'],
                "_p_tributacaoFederal": "TributacaoFederal$"+tributacao_federal['_id'],
                "nome": doc['ProdutosServicos'][0]['Descricao'],
                "_p_unidadeMedida": "UnidadeMedida$"+unidade_medida['_id'],
                "estoque": 0,
                "fator": 1,
                "classificacao": "produto",
                "quantidadeAtacado": 0,
                "codigoInterno": doc['ProdutosServicos'][0]['CodigoInterno'],
                "ncm": doc['NcmNbs']['Codigo'],
                "tipoEstoque": "normal",
                "estoqueMaximo": doc['Estoques'][0]['QuantidadeMaxima'],
                "_p_tributacaoEstadual": "TributacaoEstadual$DV6r7ksZtb",
                "estoqueMinimo": doc['Estoques'][0]['QuantidadeMinima'],
                "_created_at": datetime.now(),
                "_updated_at": datetime.now(),
                "caracteristica": None,
                "cest": None,
                "descontoMaximo": doc['LimiteDesconto'],
                "vendavel": doc['ProdutosServicos'][0]['Vendavel']
            }
            
            if 'Caracteristica' in doc['ProdutosServicos'][0]:
                modelo["caracteristica"] = doc['ProdutosServicos'][0]['Caracteristica']

            if 'CodigoEspecificadorSubstituicaoTributaria' in doc:
                modelo["cest"] = doc['CodigoEspecificadorSubstituicaoTributaria']

            if 'Medicamento' in doc['ProdutosServicos'][0]['_t']:
                modelo["classificacao"] = "medicamento"
                modelo["medicamento"] = {
                    "registroMS": doc['ProdutosServicos'][0]["RegistroMinisterioSaude"],
                }

                if 'Antimicrobiano' == doc['ProdutosServicos'][0]["ClasseTerapeutica"]['_t']:
                    modelo["medicamento"]["classeTerapeutica"] = "1"

                if 'SujeitoAControleEspecial' == doc['ProdutosServicos'][0]["ClasseTerapeutica"]['_t']:
                    modelo["medicamento"]["classeTerapeutica"] = "2"

                if 'PrecoMaximoConsumidor' in doc['ProdutosServicos'][0]:
                    modelo["medicamento"]["PMC"] = doc['ProdutosServicos'][0]['PrecoMaximoConsumidor']

                if 'Sim' == doc['ProdutosServicos'][0]['UsoProlongado']['_t']:
                    modelo["medicamento"]["usoProlongado"] = True
                elif 'Nao' == doc['ProdutosServicos'][0]['UsoProlongado']['_t']:
                    modelo["medicamento"]["usoProlongado"] = False

            if 'CodigoBarras' in doc['ProdutosServicos'][0]:
                modelo["cean"] = [
                    {
                        "cean": doc['ProdutosServicos'][0]['CodigoBarras'],
                        "fator": 1
                    }
                ]

            if 'EstoqueLote' in doc['Estoques'][0]['_t']:
                modelo["tipoEstoque"] = "lote"

            if 'UnidadeMedidaTributavel' in doc['ProdutosServicos'][0]:
                query = {
                    "sigla": doc['ProdutosServicos'][0]['UnidadeMedida']['Sigla']
                }
                unidade_medida = uni_med1_collection.find_one(query)
                modelo['_p_unidadeMedidaTributavel'] = "UnidadeMedida$"+unidade_medida['_id']

            for empresa in empresas:
                modelo['_id'] = str(ObjectId())
                modelo['_p_empresa'] = "Empresa$" + empresa['_id']
                produto_collection.insert_one(modelo)


Migrador()
