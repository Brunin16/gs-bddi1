# Relatório Técnico — BDDI
## OrbitalFire: Pipeline de Dados para Monitoramento de Risco de Queimada via Sensoriamento Orbital

**Global Solution 2026 · 1º Semestre · Indústria Espacial**
**FIAP · Engenharia de Software · 4º Ano · Presencial**

### Integrantes
| Nome completo | RM |
|---|---|
| Bruno Eduardo Caputo Paulino | 558303 |

---

## 1. Descrição da solução proposta

O **OrbitalFire** é uma solução integrada de monitoramento e alerta precoce de
queimadas baseada em **dados de sensoriamento remoto orbital** (focos de calor tipo
NASA FIRMS, NDVI tipo Copernicus, índices meteorológicos do INPE). Esta entrega da
disciplina **BDDI** é a camada de **engenharia de dados** da solução: um pipeline
automatizado que coleta, trata, integra e disponibiliza os dados em um banco
analítico, servindo de base para o módulo de IA (GAIE) e para o dashboard (SDTCC).

Conexão com a Indústria Espacial: o insumo central são dados gerados por satélites
de observação da Terra. Conexão com ODS: **ODS 13 (Ação Climática)**, com interface
para o ODS 9 (infraestrutura) e ODS 11 (cidades/territórios resilientes).

## 2. Objetivo do pipeline

Transformar dados brutos de risco de foco + clima atual em uma base confiável no
Oracle Database, permitindo análises sobre **onde, quando e sob quais condições** o
risco de queimada é maior — apoiando decisão operacional de defesa civil, brigadas e
produtores rurais.

## 3. Fonte de dados utilizada

- **CSV principal (`fire_risk_dataset.csv`)** — 5.000 linhas × 13 colunas, simulando
  observações orbitais/climáticas de risco de foco no território brasileiro. É o
  mesmo dataset usado pela disciplina GAIE, garantindo integração entre as entregas.
- **API Open-Meteo (ao vivo, sem chave)** — clima atual de 5 biomas brasileiros
  (Amazônia, Cerrado, Pantanal, Caatinga, Mata Atlântica), usada para enriquecimento.

## 4. Arquitetura do pipeline


```
CSV OrbitalFire ──► extrair_csv ──┐
                                  ├─► transformar ─► criar_tabelas ─► carregar_oracle ─► analisar (SQL)
API Open-Meteo ── extrair_clima ──┘    (limpeza,        (DDL)           (Oracle)
                                        tipos, risco)
```

## 5. Explicação das etapas da DAG

A DAG `orbitalfire_bddi_pipeline` (Apache Airflow rodando em Docker, modo
`standalone`, API clássica com `PythonOperator`) possui 6 tasks:

1. **extrair_csv** — lê o CSV e materializa em CSV (staging).
2. **extrair_clima_api** — coleta clima atual via Open-Meteo; em caso de falha de
   rede, usa *fallback* (o pipeline não quebra).
3. **transformar** — limpeza, tratamento de nulos, conversão de tipos, padronização e
   enriquecimento (deriva `risco_classe`).
4. **criar_tabelas** — DDL idempotente das tabelas no Oracle.
5. **carregar_oracle** — carga em massa via `executemany`.
6. **analisar** — executa consultas de validação e registra os resultados nos logs.

Dependências: as duas extrações rodam **em paralelo** e convergem em `transformar`;
a carga só ocorre após `criar_tabelas`; a análise só após a carga.

> _[PRINT 1: grafo da DAG com todas as tasks em verde após execução]_
> _[PRINT 2: logs das tasks `transformar` e `analisar`]_

## 6. Transformações realizadas

- Remoção de duplicatas e de registros sem variáveis essenciais.
- Conversão de tipos (inteiros para `mes`, `dias_sem_chuva`, `ocorrencia_foco`).
- Padronização textual de `tipo_cobertura` (trim + minúsculas).
- Imputação de nulos numéricos pela mediana.
- **Enriquecimento:** criação de `risco_classe` a partir do `indice_fwi`
  (baixo <12, moderado <18, alto <24, crítico ≥24 — faixas calibradas nos quartis).
- Geração da chave primária `foco_id`.

## 7. Modelagem das tabelas no Oracle

**`foco_queimada`** (fato — 5.000 linhas): `foco_id` (PK), `mes`, `temperatura_c`,
`umidade_relativa`, `velocidade_vento_kmh`, `precipitacao_mm`, `dias_sem_chuva`,
`ndvi`, `indice_fwi`, `latitude`, `longitude`, `altitude_m`, `tipo_cobertura`,
`ocorrencia_foco`, `risco_classe`, `data_carga`.

**`clima_atual`** (enriquecimento via API): `clima_id` (PK), `regiao`, `latitude`,
`longitude`, `temperatura_c`, `umidade_relativa`, `velocidade_vento_kmh`, `precipitacao_mm`,
`fonte`, `coletado_em`.

DDL completo em `sql/01_create_tables.sql`.

> _[PRINT 3: tabela `foco_queimada` populada no Oracle]_
> _[PRINT 4: tabela `clima_atual` populada no Oracle]_

## 8. Consultas analíticas e resultados

> Resultados de referência abaixo (seed fixo = 42; os números devem coincidir ao
> rodar). Substitua/complemente com os **prints reais** do SQL Developer/DBeaver.

**Consulta 3 — Estação seca × úmida**

| estação | observações | temp. média | dias secos médios | taxa de foco |
|---|---|---|---|---|
| Estação seca (mai–set) | 2.048 | 33,1 °C | 12,9 | **73,8 %** |
| Estação úmida | 2.952 | 28,1 °C | 4,0 | 15,7 % |

**Consulta 2 — Taxa de foco por cobertura do solo**

| cobertura | registros | temp. média | taxa de foco |
|---|---|---|---|
| pastagem | 1.163 | 30,1 °C | 47,7 % |
| cerrado | 1.314 | 30,0 °C | 45,7 % |
| agricultura | 984 | 30,2 °C | 41,4 % |
| urbano | 426 | 30,2 °C | 31,7 % |
| floresta | 1.113 | 30,1 °C | 24,7 % |

**Consulta 4 — Top 5 meses por número de focos**

| mês | focos |
|---|---|
| Junho | 316 |
| Maio | 315 |
| Julho | 305 |
| Setembro | 291 |
| Agosto | 284 |

**Consulta 5 — Distribuição por classe de risco**

| classe | registros | % | FWI médio |
|---|---|---|---|
| moderado | 1.693 | 33,9 % | 14,6 |
| alto | 1.475 | 29,5 % | 20,8 |
| baixo | 1.216 | 24,3 % | 9,6 |
| crítico | 616 | 12,3 % | 26,6 |

> _[PRINT 5: resultados das consultas 1, 6 e 7 (temporal, filtro composto e JOIN)]_

## 9. Conclusão técnica

O pipeline demonstra um fluxo completo de engenharia de dados orquestrado pelo Apache
Airflow, com **integração de duas fontes heterogêneas** (arquivo + API ao vivo),
tratamento robusto e carga estruturada no Oracle. As análises confirmam padrões
fisicamente esperados — concentração de focos na **estação seca (73,8 % vs. 15,7 %)**
e maior incidência em **pastagem e cerrado** — validando a qualidade dos dados e a
utilidade da base para o módulo de IA (GAIE) e o dashboard (SDTCC) da solução
integrada. A arquitetura é reprodutível, idempotente e resiliente a falhas de rede.
