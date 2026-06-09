# 🛰️ OrbitalFire — Pipeline de Dados (BDDI)

**Disciplina:** Big Data Architecture & Data Integration (BDDI)
**Global Solution 2026 · 1º Semestre · Indústria Espacial**
**FIAP · Engenharia de Software · 4º Ano · ODS 13 (Ação Climática)**

> Integrantes:
> - Bruno Eduardo Caputo Paulino — RM 558303

Pipeline de engenharia de dados que ingere dados de **risco de queimada via
sensoriamento orbital** (mesmo dataset do módulo de IA OrbitalFire/GAIE) somados a
**clima atual coletado ao vivo por API**, trata, carrega no **Oracle Database** e
disponibiliza consultas analíticas SQL — tudo orquestrado pelo **Apache Airflow**.

---

## 1. Objetivo do pipeline

Transformar dados brutos de focos de calor + clima em uma base analítica confiável
no Oracle, permitindo responder perguntas operacionais (onde, quando e sob quais
condições o risco de queimada é maior) para apoiar alerta precoce e priorização de
combate.

## 2. Fonte de dados

| Fonte | Tipo | Uso |
|---|---|---|
| `data/fire_risk_dataset.csv` | Arquivo CSV (5.000×13) | Fonte principal — observações de risco de foco (clima + NDVI + FWI por bioma) |
| **Open-Meteo API** | API pública ao vivo (sem chave) | Enriquecimento — clima atual de 5 biomas brasileiros |

O CSV é compartilhado com a disciplina **GAIE** (integração entre as entregas). A API
adiciona uma extração externa real; se a rede falhar, a task usa *fallback* e o
pipeline não quebra.

## 3. Arquitetura do pipeline

```
            ┌─────────────────┐     ┌──────────────────────┐
 FONTES     │  CSV OrbitalFire│     │ API Open-Meteo (live)│
            └────────┬────────┘     └──────────┬───────────┘
                     │ extrair_csv             │ extrair_clima_api
                     ▼                         ▼
                 ┌───────────────────────────────┐
 TRATAMENTO      │          transformar          │  limpeza · tipos ·
                 │  (dedup, nulos, padronizacao, │  padronizacao ·
                 │   risco_classe, chave)        │  enriquecimento
                 └───────────────┬───────────────┘
                                 ▼
                 criar_tabelas ─► carregar_oracle  ──► ORACLE (foco_queimada,
                                 (DELETE+INSERT bulk)     clima_atual)
                                 │
                                 ▼
                            analisar (SQL)
```

Fluxo: **fonte → extração → transformação → carga no Oracle → análise SQL**.

## 4. Etapas da DAG (`orbitalfire_bddi_pipeline`)

| Task | Função |
|---|---|
| `extrair_csv` | Lê o CSV e materializa em staging (Parquet) |
| `extrair_clima_api` | Coleta clima atual via Open-Meteo (com fallback) |
| `transformar` | Dedup, tratamento de nulos, conversão de tipos, padronização, deriva `risco_classe`, gera PK |
| `criar_tabelas` | DDL idempotente (drop+create) das duas tabelas |
| `carregar_oracle` | Carga em massa (`executemany`) nas tabelas |
| `analisar` | Roda consultas de validação e loga os resultados |

Dependências: `extrair_csv` + `extrair_clima_api` → `transformar`; `criar_tabelas` →
`carregar_oracle` → `analisar`.

## 5. Transformações realizadas

- Remoção de duplicatas e de registros sem variáveis essenciais
- Conversão de tipos (`mes`, `dias_sem_chuva`, `ocorrencia_foco` → inteiros)
- Padronização textual de `tipo_cobertura` (trim + lowercase)
- Imputação de nulos numéricos remanescentes pela mediana
- **Enriquecimento:** `risco_classe` derivada do `indice_fwi` (baixo <12, moderado <18, alto <24, crítico ≥24 — faixas calibradas nos quartis)
- Geração de chave primária `foco_id`

## 6. Modelagem no Oracle

Ver `sql/01_create_tables.sql`. Duas tabelas: `foco_queimada` (fato, 5.000 linhas) e
`clima_atual` (enriquecimento via API).

## 7. Consultas analíticas

Sete consultas em `sql/02_analytics.sql` (mínimo exigido: 5), cobrindo filtros,
agrupamentos, agregações, ordenação, função de janela e JOIN.

---

## ⚙️ Como executar (Docker + Airflow)

### 1. Construir e subir o container

A partir da raiz do projeto (onde está o `docker-compose.yml`):
```bash
docker compose up -d --build
docker ps -a    # confirma container 'airflow' em execucao
```
> O `Dockerfile` usa a imagem `apache/airflow:2.8.1` e instala os pacotes de
> `requirements.txt` durante o build. O primeiro build demora ~1-2 min.

### 2. Obter a senha do admin

```bash
docker exec airflow cat /opt/airflow/standalone_admin_password.txt
```
Acesse [http://localhost:8080](http://localhost:8080) com usuário `admin` e a senha acima.

### 3. Criar a Connection do Oracle (a senha NÃO vai no código)

Execute em uma única linha (Windows e Linux):
```bash
docker exec airflow airflow connections add oracle_fiap --conn-type oracle --conn-host oracle.fiap.com.br --conn-port 1521 --conn-schema ORCL --conn-login rm558303 --conn-password "SUA_SENHA_AQUI"
```
> A senha fica apenas no metastore do Airflow, nunca no repositório.
> O `oracledb` roda em modo *thin*: não precisa instalar Oracle Client.

### 4. Despausar e disparar a DAG
```bash
docker exec airflow airflow dags unpause orbitalfire_bddi_pipeline
docker exec airflow airflow dags trigger orbitalfire_bddi_pipeline
docker exec airflow airflow dags list-runs -d orbitalfire_bddi_pipeline
```
> O Airflow cria DAGs pausadas por padrão. O `unpause` é necessário apenas
> na primeira execução.

### Prints necessários para o relatório (PDF)
- Grafo da DAG executada (todas as tasks em verde)
- Logs das tasks `transformar` e `analisar`
- Tabelas `foco_queimada` e `clima_atual` populadas no Oracle (SQL Developer/DBeaver)
- Resultado de cada uma das 7 consultas analíticas
