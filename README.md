# fiap-spp-wikimidia-analytics-pipeline

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
Gold — Parquet agregado 
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

## O que o pipeline faz

### Ingestão
Consome o endpoint [`https://stream.wikimedia.org/v2/stream/recentchange`](https://stream.wikimedia.org/v2/stream/recentchange) via SSE, gravando os eventos em JSON Lines na camada Raw do Unity Catalog Volume.

### Camada Bronze
Lê os arquivos JSON em modo streaming (`readStream`) com schema explícito e grava em Parquet (`writeStream`) com checkpoint incremental.

### Camada Silver
Aplica regras de qualidade sobre o stream Bronze:
- Filtra registros com campos obrigatórios ausentes (`id`, `type`, `wiki`, `title`)
- Restringe eventos aos tipos `edit`, `new`, `log` e `categorize`
- Converte timestamp Unix para `TimestampType`
- Calcula `length_delta` (diferença de tamanho da página)

### Camada Gold
Três agregações analíticas com **Window Functions de 5 minutos** e **Watermarking de 10 minutos**, cada uma com checkpoint próprio:

| Agregação | Dimensão | Métricas |
|---|---|---|
| Impacto de Conteúdo | `wiki` | `total_edits`, `total_content_impact` |
| Engajamento de Usuários | `user`, `bot` | `total_events` |
| Perfil de Eventos | `wiki`, `type` | `total_events` |

---

## Destaques técnicos

- **Schema explícito** declarado antes do `readStream` — evita falhas de inferência em streams contínuos
- **Checkpoints independentes por query** — garante retomada incremental sem reprocessamento indevido
- **`filter` antes do `withWatermark`** — remove timestamps nulos antes do cálculo do watermark
- **`outputMode("append")` com Watermarking** — emite janelas apenas após serem finalizadas, sem reemitir resultados parciais
- **`trigger(availableNow=True)`** — execução finita com semântica completa de Structured Streaming

---

## Estrutura do repositório

```
streaming/
├── notebook-streaming.ipynb   # Pipeline completo executado no Databricks
└── README.md
```

---

## Como executar

O notebook foi desenvolvido e executado no **Databricks** com acesso a um Unity Catalog.

1. Importar `notebook-streaming.ipynb` no Databricks Workspace
2. Anexar a um cluster com Spark 3.x+
3. Executar as células em ordem — o setup cria o schema, o volume e os diretórios automaticamente
4. A variável `RESET_PIPELINE_OUTPUTS = True` limpa execuções anteriores antes de iniciar

---

## Contexto acadêmico

Projeto desenvolvido para a disciplina **Stream Processing Pipelines** — FIAP (2026).  
Requisitos atendidos: pipeline de streaming, conversão de formato, validação (Bônus 1), agregação (Bônus 2) e Window Function com Watermarking (Bônus 3).

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