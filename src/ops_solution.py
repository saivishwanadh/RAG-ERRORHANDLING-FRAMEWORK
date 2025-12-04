from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
import json
from typing import Dict, Any, Optional
import os
from embeddingmodel import EmbeddingGenerator
from vectordb import QdrantStore
import logging
#from sympy import re, true
import re as r
from structuraldb import DB
from dotenv import load_dotenv, dotenv_values 

from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware



env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

app = FastAPI()
API_KEY = os.getenv("HUGGINGFACE_APIKEY")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
def extract_solution(llm_solution_string: str, solution_id: str) -> str:
    try:
        # Convert JSON string to dict
        data = json.loads(llm_solution_string)

        # Build key dynamically → solution1 / solution2 / solution3
        key = f"solution{solution_id}"

        # Return only instructions text (or empty string if not found)
        return data.get(key, {}).get("instructions", "")
    except Exception:
        return ""

def clean_error_description(text: str) -> dict:
    """Normalize/clean incoming description """
    if not text:
        return {"cleanText": ""}

    s = text
    s = s.replace("\\n", " ").replace("\n", " ")
    s = s.replace('\\"', "").replace("&quot;", "")
    s = r.sub(r"\s+", " ", s)
    s = r.sub(r"[\(\)\[\]\{\}]", "", s)
    s = r.sub(r"[\'\":;^|\-,]", "", s)
    s = s.strip().strip('"').strip("'")
    return {"cleanText": s}




@app.get("/exposeFeedBackUI-for-solutionSelection", response_class=HTMLResponse)
async def load_solution(request: Request):
    token = request.query_params.get("token")
    solution_id = request.query_params.get("SOLUTION_ID")
    if not token or not solution_id:
        return HTMLResponse("Missing token or SOLUTION_ID", status_code=400)
    db = DB()
    sql = """
        SELECT * FROM errorsolutiontable WHERE sessionid=%s
        """
    params = (
        token,    # session_id from query param
    )

    rows = db.execute(sql, params, fetch=True)
    db.close()
    payload ={
        "serviceName": rows[0].get("application_name"),
        "environment": "Non Prod (This value need to change )" ,
        "timestamp": rows[0].get("error_timestamp"),
        "errorType": rows[0].get("error_code"),
        "errorMessage": rows[0].get("error_description"),
        "errorId": rows[0].get("id"),
        "sessionId": rows[0].get("sessionid"),
        "solution": rows[0].get("llm_solution"),
    }
    print(payload.get("solution"))
    selected_solution = extract_solution(payload.get("solution"), solution_id)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(BASE_DIR, "UI", "llm-suggested-submit-ui.html")
    print(template_path)
    print(os.path.exists(template_path))
    with open(template_path, "r", encoding="utf-8") as f:
        html_template = f.read()
    html = (
        html_template
        .replace("{{ERROR_ID}}", str(payload.get("errorId")))
        .replace("{{ERROR_TYPE}}", payload.get("errorType"))
        .replace("{{SOLUTION_TEXT}}", selected_solution)
        .replace("{{SOLUTION_ID}}", solution_id)
    )
    return HTMLResponse(html, media_type="text/html")
    
    
    
@app.get("/exposeFeedBackUI-for-CustomSolution", response_class=HTMLResponse)
async def custom_solution(request: Request):
    token = request.query_params.get("token")
    db = DB()
    sql = """
        SELECT * FROM errorsolutiontable WHERE sessionid=%s
        """
    params = (
        token,    # session_id from query param
    )

    rows = db.execute(sql, params, fetch=True)
    payload ={
        "serviceName": rows[0].get("application_name"),
        "environment": "Non Prod (This value need to change )" ,
        "timestamp": rows[0].get("error_timestamp"),
        "errorType": rows[0].get("error_code"),
        "errorMessage": rows[0].get("error_description"),
        "errorId": rows[0].get("id"),
        "sessionId": rows[0].get("sessionid"),
        "solution": rows[0].get("llm_solution"),
    }
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_path = os.path.join(BASE_DIR, "UI", "custom-solution-submit-ui.html")
    print(template_path)
    print(os.path.exists(template_path))
    with open(template_path, "r", encoding="utf-8") as f:
        html_template = f.read()
    html = (
        html_template
        .replace("{{ERROR_ID}}", str(payload.get("errorId")))
        .replace("{{ERROR_TYPE}}", payload.get("errorType"))
    )
    return HTMLResponse(html, media_type="text/html")
    



@app.post("/submitopssolution")
async def update_vector(request: Request):
    payload = await request.json()
    record_id = payload.get("errorId")
    custom_solution = payload.get("customSolution")
    selected_solution_id = payload.get("solutionId")
    solution_timestamp = payload.get("solutionTimestamp")
    
    db = DB()

    # check if session is active
    rows = db.execute(
        "SELECT sessionid_status FROM errorsolutiontable WHERE id=%s",
        (record_id,), fetch=True
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Record not found")
    
    if rows[0]["sessionid_status"] != "active":
        return JSONResponse(
            content={"error": "Session Expired", "status": "inactive", "message": "Your session has expired"},
            status_code=404
        )
    if custom_solution is None:
        row = db.execute(
            "SELECT error_code,error_description,llm_solution FROM errorsolutiontable WHERE id=%s",
            (record_id,), fetch=True
        )[0]

        llm_solution_dict = json.loads(row["llm_solution"])   # <-- convert string to dict
        solution_key = f"solution{selected_solution_id}"
        custom_solution = llm_solution_dict.get(solution_key, {}).get("instructions")
        
    db.execute(
        """UPDATE errorsolutiontable
           SET ops_solution=%s, ops_solution_timestamp=%s, sessionid_status=%s
           WHERE id=%s""",
        (custom_solution, solution_timestamp, "inactive", record_id,)
    )
    # fetch record again for embedding creation
    record = db.execute(
        "SELECT application_name,error_code,error_description,ops_solution FROM errorsolutiontable WHERE id=%s",
        (record_id,), fetch=True
    )[0]
    cleanErrorDescription = clean_error_description(record["error_description"])
    embed_payload_input = {
        "embed_input": "Error:" + str(record['error_code']) + " " + "Description:" + str(cleanErrorDescription.get("cleanText", ""))
    }
    embed_gen = EmbeddingGenerator(api_key=API_KEY)
    raw_embedding_input = embed_gen.get_embedding(embed_payload_input["embed_input"])
    store = QdrantStore()
    store.upsert_vector(
        collection="error_solutions",
        vector_id=int(record_id),
        vector=raw_embedding_input,
        payload={
            "error_code": record.get("error_code", ""),
            "error_description": record.get("error_description", ""),
            "solution": custom_solution
        }
    )
    
    return {"message": "Vector DB updated successfully", "status": "SUCCESS"}

    
