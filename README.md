# WikiStream Analytics Pipeline

Pipeline de **stream processing** construído com Apache Spark Structured Streaming no Databricks, consumindo o stream público de mudanças em tempo real da Wikimedia e organizando os dados em uma arquitetura Medallion (Raw → Bronze → Silver → Gold).

---

## Arquitetura

```
Wikimedia Recent Changes Stream (SSE)
        ↓
Raw — JSON Lines (Unity Catalog Volume)
        ↓
Bronze — Parquet (conversão de formato via readStream/writeStream)
        ↓
Silver — Parquet validado (filtros, transformações, schema enriquecido)
        ↓
Gold — Parquet agregado (Window Functions + Watermarking)
```

---

## Tecnologias

| Camada | Tecnologia |
|---|---|
| Plataforma | Databricks |
| Processamento | Apache Spark Structured Streaming |
| Armazenamento | Unity Catalog Volume (Managed) |
| Ingestão | Python `requests` + Server-Sent Events (SSE) |
| Formato de saída | Apache Parquet |
| Linguagem | Python (PySpark) |

---

## Unity Catalog

| Recurso | Nome |
|---|---|
| Catalog | `workspace` |
| Schema | `stream_processing_pipelines` |
| Volume | `wikimedia_stream_pipeline` |

---

## O que o pipeline faz

### Ingestão

Consome o endpoint `https://stream.wikimedia.org/v2/stream/recentchange` via SSE usando `requests` com `stream=True`, mantendo a conexão HTTP aberta e iterando as linhas com `response.iter_lines()`.

Parâmetros padrão: `duration_seconds=60`, `max_events=300`.

Cada evento é identificado pelo prefixo `data:`, convertido de JSON para dicionário Python e enriquecido com o campo `_ingestion_timestamp` (ISO 8601 UTC) antes de ser gravado em JSON Lines na camada Raw. A requisição utiliza o header `User-Agent: WikiStreamAnalyticsPipeline/1.0`.

### Camada Bronze

Lê os arquivos JSON da Raw em modo streaming (`readStream`) com schema explícito e grava em Parquet (`writeStream`) com checkpoint incremental. Processa os arquivos em blocos de até 10 por trigger (`maxFilesPerTrigger=10`).

### Camada Silver

Aplica regras de qualidade sobre o stream Bronze:
- Filtra registros com campos obrigatórios ausentes (`id`, `type`, `wiki`, `title`)
- Restringe eventos aos tipos `edit`, `new`, `log` e `categorize`
- Converte timestamp Unix para `TimestampType` (`event_timestamp`)
- Converte `_ingestion_timestamp` string para `TimestampType` (`ingestion_timestamp`)
- Calcula `length_delta` (diferença de tamanho da página quando `length.old` e `length.new` estão presentes)
- Adiciona `processed_at` com o timestamp de processamento

Campos selecionados na Silver: `id`, `type`, `wiki`, `title`, `user`, `bot`, `minor`, `server_name`, `event_timestamp`, `ingestion_timestamp`, `length_delta`, `processed_at`.

### Camada Gold

Três agregações analíticas com **Window Functions de 5 minutos** e **Watermarking de 10 minutos**, cada uma com checkpoint próprio. O `filter` de timestamps nulos é aplicado antes do `withWatermark` e o schema é inferido da Silver via leitura batch.

| Agregação | Caminho | Dimensão | Métricas |
|---|---|---|---|
| Impacto de Conteúdo | `gold/aggregations_parquet/window_impact` | `wiki` | `total_edits`, `total_content_impact` |
| Engajamento de Usuários | `gold/aggregations_parquet/user_activity` | `user`, `bot` | `total_events` |
| Perfil de Eventos | `gold/aggregations_parquet/event_types` | `wiki`, `type` | `total_events` |

> **Nota:** Com `trigger(availableNow=True)` em execuções finitas, o watermark pode não avançar o suficiente para finalizar todas as janelas antes do encerramento das queries. Esse é o comportamento esperado do Structured Streaming. Os resultados analíticos completos são exibidos no notebook via leitura batch da camada Silver, aplicando as mesmas agregações e janelas das queries Gold.

---

## Destaques técnicos

- **Schema explícito** declarado antes do `readStream` — evita falhas de inferência em streams contínuos
- **Checkpoints independentes por query** — garante retomada incremental sem reprocessamento indevido
- **`filter` antes do `withWatermark`** — remove timestamps nulos antes do cálculo do watermark
- **`outputMode("append")` com Watermarking** — emite janelas apenas após serem finalizadas, sem reemitir resultados parciais
- **`trigger(availableNow=True)`** — execução finita com semântica completa de Structured Streaming
- **Batch fallback para visualização da Gold** — mesmas agregações e janelas executadas via leitura batch da Silver para exibir resultados em execuções finitas

---

## Estrutura do repositório

```
streaming/
├── notebook-streaming-.ipynb   # Pipeline completo executado no Databricks
└── README.md
```

---

## Como executar

O notebook foi desenvolvido e executado no **Databricks** com acesso a um Unity Catalog.

1. Importar `notebook-streaming-.ipynb` no Databricks Workspace
2. Anexar a um cluster com Spark 3.x+
3. Executar as células em ordem — o setup cria o schema, o volume e os diretórios automaticamente
4. A variável `RESET_PIPELINE_OUTPUTS = True` limpa execuções anteriores antes de iniciar

---

## Contexto acadêmico

Projeto desenvolvido para a disciplina **Stream Processing Pipelines** — FIAP (2026).  
Requisitos atendidos: pipeline de streaming, conversão de formato, validação (Bônus 1), agregação (Bônus 2)  

---

## Autoria
<table>
  <tr>
    <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/vivianecorrea" 
           width="100px;" 
           alt="Viviane Corrêa"/>
      <br/>
      <b>Viviane Corrêa</b>
      <br/>
      <a href="https://github.com/vivianecorrea">GitHub</a>
    </td>
    <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/tatiane-ss" 
           width="100px;" 
           alt="Tatiane Silva"/>
      <br/>
      <b>Tatiane Silva</b>
      <br/>
      <a href="https://github.com/tatiane-ss">GitHub</a>
    </td>
    <td align="center">
      <img style="border-radius: 50%;" 
           src="https://avatars.githubusercontent.com/ThatianeBotelho" 
           width="100px;" 
           alt="Thatiane Botelho"/>
      <br/>
      <b>Thatiane Botelho</b>
      <br/>
      <a href="https://github.com/ThatianeBotelho">GitHub</a>
    </td>
  </tr>
</table>
