# app.py
# A Flask API to receive a Base64-encoded VALEO PDF, extract its specific
# data, and store it in a PostgreSQL database.

import os
import io
import re
import fitz  # PyMuPDF
import psycopg2
import logging
import base64
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- CONFIGURATION ---
# Load environment variables from a .env file for security
load_dotenv()

# Basic Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Connection String from .env file
# Example: "postgresql://user:password@host:port/database"
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY")  
# --- FLASK APP INITIALIZATION ---
app = Flask(__name__)

# --- DATABASE HELPER ---
def init_db():
    """
    Initializes the database and creates the EDIGlobal table if it doesn't exist.
    This function is safe to run even if the table already exists.
    """
    # This check is added to ensure the script doesn't fail if DATABASE_URL is not set
    if not DATABASE_URL:
        logging.warning("DATABASE_URL not set. Skipping database initialization.")
        return
        
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        # Create the table based on the provided schema IF IT DOES NOT EXIST
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public."EDIGlobal"
            (
                "Site" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "ClientCode" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "ClientMaterialNo" character varying(50) COLLATE pg_catalog."default",
                "AVOMaterialNo" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "DateFrom" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "DateUntil" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "Quantity" integer NOT NULL,
                "ForecastDate" character varying(50) COLLATE pg_catalog."default" NOT NULL,
                "LastDeliveryDate" character varying(50) COLLATE pg_catalog."default",
                "LastDeliveredQuantity" integer,
                "CumulatedQuantity" integer NOT NULL,
                "EDIStatus" character varying(50) COLLATE pg_catalog."default",
                CONSTRAINT "EDIGlobal_pkey" PRIMARY KEY ("ClientCode", "AVOMaterialNo", "DateFrom", "DateUntil", "ForecastDate", "CumulatedQuantity")
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Database initialized successfully. 'EDIGlobal' table is ready.")
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to the database: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during DB initialization: {e}")


# --- CORE LOGIC FUNCTIONS ---

def process_valeo_pdf(pdf_bytes: io.BytesIO, customer_code: str, customer_name: str, site="Germany"):
    """
    Processes VALEO PDF format from a byte stream for EDIGlobal insertion.
    (This function contains your custom processing logic and remains unchanged).
    """
    full_text = ""
    try:
        # 1. Extract all text from the PDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for page in doc:
                full_text += page.get_text()
    except Exception as e:
        logging.error(f"Failed to parse PDF file with PyMuPDF: {e}")
        raise ValueError(f"Failed to parse PDF file: {e}")

    # 2. Extract data based on VALEO format
    forecast_match = re.search(r"Druckdatum:(?:\s|\n|\r)*.*?(\d{2}\.\d{2}\.\d{4})", full_text)
    forecast_date = forecast_match.group(1) if forecast_match else None
    if not forecast_date:
        raise ValueError("ForecastDate (Druckdatum) could not be extracted.")

    material_map = {
        "1023093": "190313", "1023645": "191663", "1026188": "187144",
        "1026258": "194470", "1026540": "202066", "1026629": "214188"
    }
    reverse_map = {v: k for k, v in material_map.items()}

    sections = re.split(r"Sachnummer:\s+", full_text)[1:]
    results = []

    for section in sections:
        last_deliv_match = re.search(r"Lieferscheinnr.*?Datum:\s*(\d{2}\.\d{2}\.\d{4}).*?Menge:\s*(\d+)", section, re.DOTALL)
        last_delivery_date = last_deliv_match.group(1) if last_deliv_match else None
        last_delivered_qty = int(last_deliv_match.group(2)) if last_deliv_match else None

        material_match = re.match(r"0*(\d+)", section)
        raw_client_code = material_match.group(1) if material_match else None
        
        customer_code_val = "100442"
        avo_code = raw_client_code or "UNKNOWN"

        if raw_client_code:
            if raw_client_code in material_map:
                avo_code = raw_client_code
                customer_code_val = material_map[raw_client_code]
            elif raw_client_code in reverse_map:
                customer_code_val = raw_client_code
                avo_code = reverse_map[raw_client_code]

        delivery_lines = re.findall(
            r"((?:\d{2}\.\d{2}\.\d{4})|(?:20\d{2}\s*w\d{2}\s*-\s*20\d{2}\s*w\d{2}))\s+(\d+)\s+(\d+)",
            section
        )

        for date_or_range, qty_str, cum_qty_str in delivery_lines:
            if "w" in date_or_range:
                from_to_match = re.match(r"(20\d{2}\s*w\d{2})\s*-\s*(20\d{2}\s*w\d{2})", date_or_range)
                date_from = from_to_match.group(1) if from_to_match else date_or_range
                date_until = from_to_match.group(2) if from_to_match else date_or_range
            else:
                date_from = date_until = date_or_range

            results.append({
                "Site": site, "ClientCode": "100442", "ClientMaterialNo": customer_code_val,
                "AVOMaterialNo": avo_code, "DateFrom": date_from, "DateUntil": date_until,
                "Quantity": int(qty_str), "ForecastDate": forecast_date,
                "LastDeliveryDate": last_delivery_date, "LastDeliveredQuantity": last_delivered_qty,
                "CumulatedQuantity": int(cum_qty_str), "EDIStatus": "Forecast"
            })

    if not results:
        raise ValueError("No valid delivery data found in the VALEO PDF.")
    
    logging.info(f"Successfully extracted {len(results)} records from the PDF.")
    return results

def save_to_postgres(records: list):
    """Saves a list of extracted data records to the PostgreSQL database."""
    if not records:
        logging.warning("No records provided to save.")
        return False
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        sql = """
            INSERT INTO public."EDIGlobal" (
                "Site", "ClientCode", "ClientMaterialNo", "AVOMaterialNo", "DateFrom", 
                "DateUntil", "Quantity", "ForecastDate", "LastDeliveryDate", 
                "LastDeliveredQuantity", "CumulatedQuantity", "EDIStatus"
            ) VALUES (
                %(Site)s, %(ClientCode)s, %(ClientMaterialNo)s, %(AVOMaterialNo)s, %(DateFrom)s,
                %(DateUntil)s, %(Quantity)s, %(ForecastDate)s, %(LastDeliveryDate)s,
                %(LastDeliveredQuantity)s, %(CumulatedQuantity)s, %(EDIStatus)s
            )
            ON CONFLICT ("ClientCode", "AVOMaterialNo", "DateFrom", "DateUntil", "ForecastDate", "CumulatedQuantity")
            DO UPDATE SET
                "Site" = EXCLUDED."Site",
                "ClientMaterialNo" = EXCLUDED."ClientMaterialNo",
                "Quantity" = EXCLUDED."Quantity",
                "LastDeliveryDate" = EXCLUDED."LastDeliveryDate",
                "LastDeliveredQuantity" = EXCLUDED."LastDeliveredQuantity",
                "EDIStatus" = EXCLUDED."EDIStatus";
        """
        
        cur.executemany(sql, records)
        
        conn.commit()
        logging.info(f"Successfully saved/updated {len(records)} records to PostgreSQL.")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Database insertion failed: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False


# --- API ENDPOINT ---
@app.route("/process-valeo-pdf", methods=['POST'])
def process_pdf_endpoint():
    """
    API endpoint to process a VALEO PDF sent as a Base64 string.
    Extracts customer_code from the filename (e.g., CXXXXX_...).
    Expects JSON: {"file_name": "...", "file_content_base64": "..."}
    """
    # --- API KEY CHECK (ADD THIS BLOCK) ---
    client_key = request.headers.get('x-api-key')
    if not client_key or client_key != API_KEY:
        abort(401, description="Invalid or missing API key.")
    # 1. Get data from the request
    data = request.get_json()
    required_keys = ['file_name', 'file_content_base64']
    if not data or not all(k in data for k in required_keys):
        missing_keys = [k for k in required_keys if k not in data]
        return jsonify({"error": f"Missing keys in request body: {', '.join(missing_keys)}"}), 400
    
    file_name = data['file_name']
    
    # 2. Extract customer_code from filename and set customer_name
    customer_code_match = re.search(r"(C\d{5})", file_name)
    if not customer_code_match:
        return jsonify({"error": f"Could not extract customer code (format CXXXXX) from filename: {file_name}"}), 400
    
    customer_code = customer_code_match.group(1)
    customer_name = "Valeo" # Hardcoded as requested
    
    # 3. Decode the Base64 file content
    try:
        pdf_decoded_bytes = base64.b64decode(data['file_content_base64'])
        pdf_bytes_io = io.BytesIO(pdf_decoded_bytes)
    except (TypeError, base64.binascii.Error) as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": "Invalid Base64 content."}), 400

    # 4. Extract data from the PDF
    try:
        # Pass the dynamically found code and hardcoded name
        extracted_records = process_valeo_pdf(pdf_bytes_io, customer_code, customer_name)
    except ValueError as e:
        return jsonify({"error": f"Failed to extract data from PDF. Reason: {e}"}), 422
    except Exception as e:
        logging.error(f"An unexpected error occurred during PDF processing for {file_name}: {e}")
        return jsonify({"error": "An unexpected error occurred during PDF processing."}), 500

    # 5. Save data to PostgreSQL
    success = save_to_postgres(extracted_records)
    if not success:
        return jsonify({"error": "Failed to save data to the database."}), 500

    # 6. Return success response
    return jsonify({
        "message": "Successfully processed and stored PDF data.",
        "file_processed": file_name,
        "customer_code_found": customer_code,
        "records_processed": len(extracted_records)
    }), 201


# --- MAIN EXECUTION ---
if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5001, debug=True)