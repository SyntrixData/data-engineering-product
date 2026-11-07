"""Category 23: Generative AI Data Engineering.

Purpose:
    Ingest, parse, chunk, and embed enterprise documents on Azure Databricks so
    they can power retrieval-augmented generation (RAG) and AI agents.

Activities covered:
    1. Document Ingestion Pipelines        9.  Incremental Document Sync
    2. PDF/Word/Text Extraction           10. Multimodal Data Ingestion
    3. Document Parsing and Normalization 11. Knowledge Base Integration
    4. RAG Data Preparation               12. AI Data Access Controls
    5. Text Chunking and Metadata         13. Retrieval Dataset Evaluation
    6. Embedding Generation Pipelines     14. KB Freshness Monitoring
    7. Vector Database Ingestion          15. Source Document Lineage
    8. Vector Index Updates               16. AI Agent Pipeline Support
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, StringType, StructType, StructField

try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

CONFIG = {
    "adls_account": os.environ.get("ADLS_ACCOUNT", "<adls-account-name>"),
    "container": os.environ.get("ADLS_CONTAINER", "lakehouse"),
    "keyvault_scope": os.environ.get("KEYVAULT_SCOPE", "kv-data-product"),
    "openai_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT", "https://<openai>.openai.azure.com"),
    "openai_deployment": os.environ.get("AZURE_OPENAI_EMBED_DEPLOYMENT", "text-embedding-3-small"),
    "openai_api_version": os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
    "vector_index_url": os.environ.get("VECTOR_INDEX_URL", "<vector-db-endpoint>"),
    "docs_source": os.environ.get("DOCS_SOURCE_PATH", "abfss://docs@<adls>.dfs.core.windows.net/incoming"),
    "process_date": os.environ.get("PROCESS_DATE", datetime.utcnow().strftime("%Y-%m-%d")),
    "chunk_size": int(os.environ.get("CHUNK_SIZE", "1000")),
    "chunk_overlap": int(os.environ.get("CHUNK_OVERLAP", "150")),
}

try:
    dbutils.widgets.text("process_date", CONFIG["process_date"])
    dbutils.widgets.text("collection", "enterprise_kb")
    CONFIG["process_date"] = dbutils.widgets.get("process_date")
except Exception:
    pass

def get_spark() -> SparkSession:
    """Return an existing Spark session or build a local one with Delta configs."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (SparkSession.builder.appName("genai-data-engineering")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate())

spark = get_spark()
BASE = f"abfss://{CONFIG['container']}@{CONFIG['adls_account']}.dfs.core.windows.net"

def _genai_path(layer: str, name: str) -> str:
    return f"{BASE}/genai/{layer}/{name}"

def _get_secret(key: str, env_fallback: str) -> str:
    """Fetch a secret from Databricks secret scope, falling back to env vars."""
    try:
        return dbutils.secrets.get(scope=CONFIG["keyvault_scope"], key=key)
    except Exception:
        return os.environ.get(env_fallback, "")

def generate_embeddings(texts: List[str], batch_size: int = 16) -> List[List[float]]:
    """Call the Azure OpenAI embeddings REST API in batches (secret from Key Vault)."""
    api_key = _get_secret("azure-openai-key", "AZURE_OPENAI_KEY")
    url = (f"{CONFIG['openai_endpoint']}/openai/deployments/{CONFIG['openai_deployment']}"
           f"/embeddings?api-version={CONFIG['openai_api_version']}")
    headers = {"api-key": api_key, "Content-Type": "application/json"}
    vectors: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        resp = requests.post(url, headers=headers, json={"input": texts[i:i + batch_size]},
                             timeout=120)
        resp.raise_for_status()
        vectors.extend([d["embedding"] for d in resp.json()["data"]])
    return vectors

def ingest_documents(source_path: str, batch_name: str) -> DataFrame:
    """Ingest raw document binaries with Auto Loader (cloudFiles) into Delta."""
    query = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile")
             .option("cloudFiles.includeExistingFiles", "true").load(source_path)
             .writeStream.format("delta").outputMode("append")
             .option("checkpointLocation", _genai_path("checkpoints", batch_name))
             .start(_genai_path("raw", batch_name)))
    query.awaitTermination(timeout=300)
    return spark.read.format("delta").load(_genai_path("raw", batch_name))

def extract_document_text(df: DataFrame) -> DataFrame:
    """Extract text from PDF/Word/TXT binaries via a filename-dispatched UDF."""
    def _extract(path: str, content: bytes) -> Optional[str]:
        try:
            import io
            lower = path.lower()
            if lower.endswith(".txt"):
                return content.decode("utf-8", errors="ignore")
            if lower.endswith(".pdf"):
                from pypdf import PdfReader
                return "\n".join(p.extract_text() or ""
                                 for p in PdfReader(io.BytesIO(content)).pages)
            if lower.endswith((".docx", ".doc")):
                import docx
                return "\n".join(p.text for p in docx.Document(io.BytesIO(content)).paragraphs)
            return None
        except Exception:
            return None

    return df.withColumn("raw_text", F.udf(_extract, StringType())(F.col("path"), F.col("content")))

def parse_and_normalize(df: DataFrame) -> DataFrame:
    """Normalize extracted text: strip control chars, collapse whitespace."""
    clean = F.trim(F.regexp_replace(F.regexp_replace(F.col("raw_text"), r"[\x00-\x1F]", " "),
                                    r"\s+", " "))
    return df.withColumn("clean_text", clean).filter(F.length("clean_text") > 50)

def prepare_rag_dataset(df: DataFrame, collection: str) -> DataFrame:
    """Shape parsed docs into a RAG-ready table with stable document ids."""
    df = df.withColumn("doc_id", F.sha2(F.col("path"), 256)) \
           .withColumn("prepared_at", F.current_timestamp())
    out = _genai_path("rag", collection)
    df.select("doc_id", "path", "clean_text", "prepared_at") \
      .write.format("delta").mode("append").save(out)
    return df

def chunk_and_enrich(df: DataFrame, chunk_size: Optional[int] = None,
                     overlap: Optional[int] = None) -> DataFrame:
    """Sliding-window chunking with positional and source metadata."""
    size, ov = chunk_size or CONFIG["chunk_size"], overlap or CONFIG["chunk_overlap"]
    step = max(size - ov, 1)

    def _chunks(text: str) -> List[Dict]:
        return [{"chunk_index": str(i), "chunk_text": text[s:s + size],
                 "char_start": str(s), "char_end": str(min(s + size, len(text)))}
                for i, s in enumerate(range(0, max(len(text), 1), step))]

    schema = ArrayType(StructType([StructField("chunk_index", StringType(), True),
                                   StructField("chunk_text", StringType(), True),
                                   StructField("char_start", StringType(), True),
                                   StructField("char_end", StringType(), True)]))
    exploded = df.withColumn("chunks", F.udf(_chunks, schema)(F.col("clean_text"))) \
                 .select("doc_id", "path", F.explode("chunks").alias("c"))
    return exploded.select(
        F.sha2(F.concat("doc_id", F.col("c.chunk_index")), 256).alias("chunk_id"),
        "doc_id", "path", F.col("c.chunk_index").alias("chunk_index"),
        F.col("c.chunk_text").alias("chunk_text"), F.col("c.char_start").alias("char_start"),
        F.col("c.char_end").alias("char_end"),
        F.length(F.col("c.chunk_text")).alias("char_count"))

def embed_chunks(df: DataFrame, collection: str) -> DataFrame:
    """Attach embedding vectors to chunks via a driver-side batched API call."""
    rows = df.select("chunk_id", "chunk_text").collect()
    vectors = generate_embeddings([r["chunk_text"] for r in rows])
    vec_df = spark.createDataFrame(
        [(r["chunk_id"], json.dumps(v)) for r, v in zip(rows, vectors)],
        ["chunk_id", "embedding_json"])
    joined = df.join(vec_df, on="chunk_id", how="inner")
    joined.write.format("delta").mode("append").save(_genai_path("embedded", collection))
    return joined

def ingest_to_vector_db(df: DataFrame, collection: str) -> int:
    """Upsert embedded chunks into the vector DB via its REST upsert endpoint."""
    api_key = _get_secret("vector-db-key", "VECTOR_DB_KEY")
    rows = df.select("chunk_id", "chunk_text", "embedding_json", "doc_id", "path").collect()
    payload = {"collection": collection, "points": [
        {"id": r["chunk_id"], "vector": json.loads(r["embedding_json"]),
         "payload": {"text": r["chunk_text"], "doc_id": r["doc_id"], "path": r["path"]}}
        for r in rows]}
    resp = requests.post(f"{CONFIG['vector_index_url']}/upsert",
                         headers={"api-key": api_key}, json=payload, timeout=180)
    resp.raise_for_status()
    return len(rows)

def update_vector_index(collection: str, chunk_ids: List[str]) -> dict:
    """Request a vector-index refresh/optimize for the given chunk ids."""
    api_key = _get_secret("vector-db-key", "VECTOR_DB_KEY")
    resp = requests.post(f"{CONFIG['vector_index_url']}/index/refresh",
                         headers={"api-key": api_key},
                         json={"collection": collection, "ids": chunk_ids}, timeout=120)
    resp.raise_for_status()
    return resp.json()

def sync_documents_incremental(source_path: str, collection: str) -> DataFrame:
    """MERGE newly extracted docs into the RAG table keyed on doc hash."""
    new_docs = prepare_rag_dataset(
        parse_and_normalize(extract_document_text(
            spark.read.format("binaryFile").load(source_path))), collection) \
        .select("doc_id", "path", "clean_text", "prepared_at")
    if DeltaTable is not None:
        (DeltaTable.forPath(spark, _genai_path("rag", collection)).alias("t")
         .merge(new_docs.alias("s"), "t.doc_id = s.doc_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return spark.read.format("delta").load(_genai_path("rag", collection))

def ingest_multimodal(source_path: str, collection: str) -> DataFrame:
    """Ingest images/audio alongside text with modality metadata for encoders."""
    df = (spark.read.format("binaryFile").load(source_path)
          .withColumn("modality",
                      F.when(F.col("path").rlike(r"\.(png|jpg|jpeg)$"), "image")
                       .when(F.col("path").rlike(r"\.(mp3|wav)$"), "audio")
                       .otherwise("text")))
    df.write.format("delta").mode("append").save(_genai_path("multimodal", collection))
    return df

def integrate_knowledge_base(sources: Dict[str, str], collection: str) -> DataFrame:
    """Union multiple enterprise sources into one knowledge-base table."""
    frames = [spark.read.format("delta").load(p).withColumn("kb_source", F.lit(n))
              for n, p in sources.items()]
    kb = frames[0]
    for f in frames[1:]:
        kb = kb.unionByName(f, allowMissingColumns=True)
    kb.write.format("delta").mode("overwrite").save(_genai_path("kb", collection))
    return kb

def apply_access_controls(df: DataFrame, classification_col: str = "classification") -> DataFrame:
    """Tag rows with access policy and redact restricted content before indexing."""
    return (df.withColumn("access_policy",
                          F.when(F.col(classification_col) == "restricted", "deny")
                           .otherwise("allow"))
              .withColumn("chunk_text",
                          F.when(F.col("access_policy") == "deny", F.lit("[REDACTED]"))
                           .otherwise(F.col("chunk_text"))))

def evaluate_retrieval(eval_pairs: DataFrame, collection: str, top_k: int = 5) -> DataFrame:
    """Score retrieval recall@k for (question, expected_chunk_id) eval pairs."""
    api_key = _get_secret("vector-db-key", "VECTOR_DB_KEY")
    results = []
    for r in eval_pairs.collect():
        vec = generate_embeddings([r["question"]])[0]
        resp = requests.post(f"{CONFIG['vector_index_url']}/search",
                             headers={"api-key": api_key},
                             json={"collection": collection, "vector": vec, "top_k": top_k},
                             timeout=60)
        resp.raise_for_status()
        hits = [h["id"] for h in resp.json().get("hits", [])]
        results.append((r["question"], r["expected_chunk_id"],
                        r["expected_chunk_id"] in hits, len(hits)))
    out = spark.createDataFrame(results, ["question", "expected_chunk_id", "recalled", "n_hits"])
    out.write.format("delta").mode("overwrite").save(_genai_path("eval", collection))
    return out

def monitor_kb_freshness(collection: str, sla_hours: int = 24) -> DataFrame:
    """Flag KB documents whose newest ingestion exceeds the freshness SLA."""
    df = spark.read.format("delta").load(_genai_path("rag", collection))
    cutoff = F.date_sub(F.current_timestamp(), sla_hours // 24)
    return (df.groupBy("path").agg(F.max("prepared_at").alias("last_ingested"))
              .withColumn("stale", F.col("last_ingested") < cutoff).filter(F.col("stale")))

def record_source_lineage(collection: str) -> DataFrame:
    """Persist doc -> chunk -> embedding lineage for audit and reprocessing."""
    lineage = (spark.read.format("delta").load(_genai_path("embedded", collection))
               .select("doc_id", "path", "chunk_id", "chunk_index", "char_start", "char_end")
               .withColumn("lineage_ts", F.current_timestamp()))
    lineage.write.format("delta").mode("overwrite").save(_genai_path("lineage", collection))
    return lineage

def publish_agent_tool_view(collection: str, tool_name: str) -> str:
    """Expose the embedded KB as a queryable temp view for AI agent tool calls."""
    (spark.read.format("delta").load(_genai_path("embedded", collection))
     .createOrReplaceTempView(f"agent_{tool_name}"))
    return f"agent_{tool_name}"

def main() -> None:
    collection = "enterprise_kb"
    raw = ingest_documents(CONFIG["docs_source"], f"batch_{CONFIG['process_date']}")
    rag = prepare_rag_dataset(parse_and_normalize(extract_document_text(raw)), collection)
    chunks = apply_access_controls(chunk_and_enrich(rag))
    embedded = embed_chunks(chunks, collection)
    n = ingest_to_vector_db(embedded, collection)
    record_source_lineage(collection)
    stale = monitor_kb_freshness(collection)
    print(f"GenAI pipeline complete: embedded={embedded.count()} upserted={n} "
          f"stale_docs={stale.count()}")

if __name__ == "__main__":
    main()
