import json


def block_from_row(row):
    return {"index": row["idx"], "timestamp": row["timestamp"],
            "transactions": json.loads(row["transactions"]),
            "previous_hash": row["previous_hash"], "nonce": row["nonce"],
            "hash": row["hash"]}


def chain_status(c, validator):
    height=c.execute("SELECT COALESCE(MAX(idx),0) n FROM blocks").fetchone()["n"]
    tx_count=0
    for row in c.execute("SELECT transactions FROM blocks"):
        tx_count += len(json.loads(row["transactions"]))
    return {"height":height,"blocks":height+1,"transactions":tx_count,"valid":bool(validator(c))}


def transactions(c, limit=200):
    rows=c.execute("SELECT * FROM blocks ORDER BY idx ASC").fetchall(); out=[]
    for row in rows:
        block=block_from_row(row)
        for tx in block["transactions"]:
            out.append({"block":block["index"],"block_hash":block["hash"],**tx})
    return out[-max(1,min(int(limit),1000)):]
