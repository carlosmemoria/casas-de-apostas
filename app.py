import pandas as pd
import requests
import io
from flask import Flask, render_template, request

app = Flask(__name__)

URL_AUTORIZADAS = "https://www.gov.br/fazenda/pt-br/composicao/orgaos/secretaria-de-premios-e-apostas/lista-de-empresas/PlanilhadeAutorizaesPublicaonoSiteMF.csv"
URL_JUDICIAL = "https://www.gov.br/fazenda/pt-br/composicao/orgaos/secretaria-de-premios-e-apostas/lista-de-empresas/ProcessosjudiciaisSPAcsv05.06.csv"

def carregar_dados():
    try:
        print("Processando arquivo de 'Empresas Autorizadas'...")
        df_auth = processar_csv_mal_formatado(
            url=URL_AUTORIZADAS,
            termo_no_cabecalho='DENOMINAÇÃO SOCIAL DA EMPRESA',
            colunas_map={
                'DENOMINAÇÃO SOCIAL DA EMPRESA': 'nome',
                'MARCAS': 'marca',
                'DOMÍNIOS': 'site'
            }
        )
        df_auth['status'] = 'Autorizada'
        
        print("\nProcessando arquivo de 'Decisão Judicial'...")
        df_jud = processar_csv_mal_formatado(
            url=URL_JUDICIAL,
            termo_no_cabecalho='DENOMINAÇÃO SOCIAL DA EMPRESA',
            colunas_map={
                'DENOMINAÇÃO SOCIAL DA EMPRESA': 'nome',
                'MARCAS': 'marca',
                'DOMÍNIOS': 'site'
            }
        )
        df_jud['status'] = 'Decisão Judicial'

        df_final = pd.concat([df_auth, df_jud], ignore_index=True)
        df_final.fillna('Não informado', inplace=True)
        
        # --- NOVO: PADRONIZA OS STATUS DE DOMÍNIO PENDENTE ---
        # Lista de todos os valores que queremos substituir
        status_a_mudar = ['não informado', 'a definir', 'à definir', 'nao informado']
        # Cria a máscara para encontrar onde esses valores estão na coluna 'site'
        mask_para_mudar = df_final['site'].str.lower().isin(status_a_mudar)
        # Substitui todos os valores encontrados por 'não registrado'
        df_final.loc[mask_para_mudar, 'site'] = 'não registrado'
        
        df_final.drop_duplicates(inplace=True)
        
        print(f"\nBase de dados completa e limpa carregada! Total de {len(df_final)} linhas únicas encontradas.")
        return df_final

    except Exception as e:
        print(f"ERRO CRÍTICO ao carregar a base de dados: {e}")
        return pd.DataFrame()

def processar_csv_mal_formatado(url, termo_no_cabecalho, colunas_map):
    response = requests.get(url)
    response.raise_for_status()
    content = response.content.decode('latin-1')
    
    linhas = content.splitlines()
    linhas_a_pular = 0
    for i, linha in enumerate(linhas):
        if termo_no_cabecalho in linha:
            linhas_a_pular = i
            break
    else:
        raise ValueError(f"Não foi possível encontrar o cabeçalho com o termo '{termo_no_cabecalho}'")

    df = pd.read_csv(io.StringIO(content), sep=';', skiprows=linhas_a_pular)
    
    for col in colunas_map.keys():
        if col in df.columns:
            df[col] = df[col].ffill()

    df.rename(columns=colunas_map, inplace=True)
    
    colunas_finais = ['nome', 'marca', 'site']
    for col in colunas_finais:
        if col not in df.columns:
            df[col] = ''

    return df[colunas_finais].copy()

DADOS_APOSTAS = carregar_dados()

@app.route('/', methods=['GET'])
def index():
    status_pendentes = ['não registrado']
    mask_pendentes = DADOS_APOSTAS['site'].str.lower().isin(status_pendentes)
    total_validos = len(DADOS_APOSTAS[~mask_pendentes])

    query = request.args.get('q', '').strip()
    resultados = []
    if not DADOS_APOSTAS.empty and query:
        mask_nome = DADOS_APOSTAS['nome'].str.contains(query, case=False, na=False)
        mask_marca = DADOS_APOSTAS['marca'].str.contains(query, case=False, na=False)
        mask_site = DADOS_APOSTAS['site'].str.contains(query, case=False, na=False)
        
        mask_final = mask_nome | mask_marca | mask_site
        
        resultados = DADOS_APOSTAS[mask_final].to_dict('records')
        
    return render_template('index.html', query=query, resultados=resultados, total=total_validos)

if __name__ == '__main__':
    app.run(debug=True)
