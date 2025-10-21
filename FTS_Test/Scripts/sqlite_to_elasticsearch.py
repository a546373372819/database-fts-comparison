import sqlite3
from elasticsearch import Elasticsearch, helpers

# 1. Connect to SQLite
sqlite_path = r"dms_model_medium.db3"
conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# 2. Connect to Elasticsearch (no auth)
es = Elasticsearch("http://localhost:9200")

# 3. Create index with basic text mapping
index_name = "idobj_text"
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

es.indices.create(
    index=index_name,
    body={
        "mappings": {
            "properties": {
                "gid": {"type": "long"},
                "type_id": {"type": "integer"},
                "idobj_name": {"type": "text"},
                "idobj_alias": {"type": "text"},
                "idobj_customid": {"type": "text"}
            }
        }
    }
)

# 4. Read joined data from SQLite
query = """
SELECT ITI.gid, ITI.type_id, ITF.idobj_name, ITF.idobj_alias, ITF.idobj_customid
FROM idobj_text_fts ITF
LEFT JOIN idobj_text_indexed ITI ON ITF.rowid = ITI.rowid
"""
cursor.execute(query)

# 5. Stream rows into Elasticsearch in batches
def generate_actions():
    for row in cursor.fetchall():
        yield {
            "_index": index_name,
            "_source": {
                "gid": row[0],
                "type_id": row[1],
                "idobj_name": row[2],
                "idobj_alias": row[3],
                "idobj_customid": row[4]
            }
        }

helpers.bulk(es, generate_actions())
print("✅ Migration complete")

conn.close()
