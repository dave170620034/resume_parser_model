import streamlit as st
import pandas as pd
import re
import pdfplumber
import docx
import json
import io
import mysql.connector
import snowflake.connector

# ---------------------- CONFIGS ----------------------
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Cybrom#1123",
    "database": "candidate_info",
    "port": 3306
}

SNOWFLAKE_CONFIG = {
    "user": "DEEPAK1706",
    "password": "Dave00170608@#@#",
    "account": "DIIUKVE-QP45921",
    "warehouse": "SNOWFLAKE_LEARNING_WH",
    "database": "CANDIDATE_INFO",
    "schema": "PUBLIC"      
}
# -----------------------------------------------------

# ---------------------- DB CONNECTIONS ----------------
def connect_mysql():
    return mysql.connector.connect(
        host=MYSQL_CONFIG["host"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        port=MYSQL_CONFIG["port"],
        autocommit=True            
    )

def connect_snowflake():
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
# -----------------------------------------------------


# ---------------------- UPSERT FUNCTIONS ----------------
def save_to_mysql(data_rows):
    try:
        conn = connect_mysql()
        cursor = conn.cursor()

        sql = """
        INSERT INTO candidate_details (file_name, name, email, phone, skills, education)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            email = VALUES(email),
            phone = VALUES(phone),
            skills = VALUES(skills),
            education = VALUES(education)
        """

        for r in data_rows:
            cursor.execute(sql, (
                r["file"], r["name"], r["email"], r["phone"], r["skills"], r["education"]
            ))

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        st.error(f"MySQL Upsert Error: {e}")
        return False


def save_to_snowflake(data_rows):
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()

        for r in data_rows:

            # Escape single quotes to avoid SQL issues
            def esc(x): return str(x).replace("'", "''")

            sql = f"""
            MERGE INTO {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.CANDIDATE_DETAILS AS tgt
            USING (SELECT '{esc(r['file'])}' AS FILE_NAME,
                         '{esc(r['name'])}' AS NAME,
                         '{esc(r['email'])}' AS EMAIL,
                         '{esc(r['phone'])}' AS PHONE,
                         '{esc(r['skills'])}' AS SKILLS,
                         '{esc(r['education'])}' AS EDUCATION
                  ) AS src
            ON tgt.FILE_NAME = src.FILE_NAME
            WHEN MATCHED THEN UPDATE SET
                NAME = src.NAME,
                EMAIL = src.EMAIL,
                PHONE = src.PHONE,
                SKILLS = src.SKILLS,
                EDUCATION = src.EDUCATION
            WHEN NOT MATCHED THEN INSERT (FILE_NAME, NAME, EMAIL, PHONE, SKILLS, EDUCATION)
            VALUES (src.FILE_NAME, src.NAME, src.EMAIL, src.PHONE, src.SKILLS, src.EDUCATION);
            """

            cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        st.error(f"Snowflake Upsert Error: {e}")
        return False

# -------------------------------------------------------------


# ---------------------- EXTRACTION HELPERS ---------------------

def extract_text_from_pdf(file_bytes):
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            return "\n".join([p.extract_text() or "" for p in pdf.pages])
    except:
        return ""


def extract_text_from_docx(file_bytes):
    try:
        doc = docx.Document(io.BytesIO(file_bytes))
        return "\n".join([p.text for p in doc.paragraphs])
    except:
        return ""


def extract_text_from_json(file_bytes):
    try:
        return json.dumps(json.loads(file_bytes.decode('utf-8')), indent=2)
    except:
        return ""


def extract_text_from_csv(file_bytes):
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
        return " ".join(df.astype(str).values.flatten())
    except:
        return ""


def extract_text(uploaded_file):
    name = uploaded_file.name.lower()
    content = uploaded_file.read()

    if name.endswith(".pdf"):
        return extract_text_from_pdf(content)
    if name.endswith(".docx") or name.endswith(".doc"):
        return extract_text_from_docx(content)
    if name.endswith(".json"):
        return extract_text_from_json(content)
    if name.endswith(".csv"):
        return extract_text_from_csv(content)

    try:
        return content.decode("utf-8")
    except:
        return content.decode("latin1", errors="ignore")


# ---------------------- ENTITY EXTRACTORS ----------------------

def extract_email(text):
    found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return found[0] if found else ""


def extract_phone(text):
    found = re.findall(r"\+?\d[\d\s\-\(\)]{8,15}", text)
    if found:
        return re.sub(r"[\s\-\(\)]", "", found[0])
    return ""


def extract_name(text):
    m = re.search(r"NAME[:\-]?\s*([A-Za-z][A-Za-z0-9 ]+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    for line in text.split("\n")[:12]:
        if line.lower().startswith("name:"):
            return line.split(":", 1)[1].strip()

    for line in text.split("\n")[:12]:
        if re.match(r"^[A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z]+.*$", line.strip()):
            return line.strip()

    return ""


def extract_skills(text):
    keywords = [
        "python", "java", "sql", "excel", "power bi", "tableau", "aws",
        "machine learning", "deep learning", "nlp", "pytorch",
        "tensorflow", "docker", "kubernetes"
    ]
    t = text.lower()
    return ", ".join(sorted({k for k in keywords if k in t}))


def extract_education(text):
    keys = [
        "b.tech", "m.tech", "bachelor", "master",
        "b.sc", "m.sc", "phd", "computer science", "engineering"
    ]
    lines = text.lower().split("\n")
    found = [line.strip() for line in lines for k in keys if k in line]
    return ", ".join(sorted(set(found)))

# -------------------------------------------------------------


# ---------------------- STREAMLIT UI --------------------------
st.set_page_config(page_title="Resume NER + MySQL + Snowflake", layout="wide")
st.title("Resume NER → MySQL → Snowflake → Auto Refresh")

uploaded_files = st.file_uploader(
    "Upload resume files", type=["pdf", "docx", "json", "csv", "txt"], accept_multiple_files=True
)

# --------- Extract Button ---------
if st.button("Extract Details") and uploaded_files:
    rows = []
    for f in uploaded_files:
        text = extract_text(f)
        rows.append({
            "file": f.name,
            "name": extract_name(text),
            "email": extract_email(text),
            "phone": extract_phone(text),
            "skills": extract_skills(text),
            "education": extract_education(text)
        })

    st.session_state["rows"] = rows
    st.success("Extraction Completed!")


# -------- Show Extracted Data -------
if "rows" in st.session_state:
    df = pd.DataFrame(st.session_state["rows"])
    st.subheader("Extracted Data")
    st.dataframe(df, width="stretch")


# -------- Save to Database ----------
if st.button("Save to Database"):
    if "rows" not in st.session_state:
        st.error("No data to save!")
    else:
        rows = st.session_state["rows"]

        st.info("Saving to MySQL...")
        ok_mysql = save_to_mysql(rows)

        st.info("Saving to Snowflake...")
        ok_snow = save_to_snowflake(rows)

        if ok_mysql:
            st.success("MySQL upsert successful.")

            #  AUTO SHOW UPDATED TABLE — MySQL
            import pandas as pd
            conn = connect_mysql()
            df_mysql = pd.read_sql("SELECT * FROM candidate_details", conn)
            st.subheader(" Live MySQL Table Data")
            st.dataframe(df_mysql, width="stretch")

        if ok_snow:
            st.success("Snowflake upsert successful.")

            #  AUTO SHOW UPDATED TABLE — SNOWFLAKE
            conn = connect_snowflake()
            query = "SELECT * FROM CANDIDATE_DETAILS"
            df_snow = pd.read_sql(query, conn)
            st.subheader(" Live Snowflake Table Data")
            st.dataframe(df_snow, width="stretch")

        if ok_mysql and ok_snow:
            st.success("Data saved successfully to both databases.")
