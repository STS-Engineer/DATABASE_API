import base64
import csv
import io
import logging
import traceback
import fitz  # PyMuPDF
from flask import Flask, request, jsonify
import psycopg2
import pdfplumber
import PyPDF2
from psycopg2 import errors
from datetime import datetime ,date, timedelta
import re
from collections import defaultdict , Counter
import os
from psycopg2.extras import execute_values
from PyPDF2 import PdfReader
import json, gzip
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import requests
from flask_mail import Mail, Message
import openai
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import pytz

app = Flask(__name__)

# --- Flask-Mail Configuration (Outlook SMTP) ---


# ... existing DB config ...

# ---------- logging ----------
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

app.config['MAIL_SERVER'] = 'avocarbon-com.mail.protection.outlook.com'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False 
app.config['MAIL_DEFAULT_SENDER'] = 'administration.STS@avocarbon.com'

# Initialize Mail
mail = Mail(app)
# Use your actual DATABASE_URL as an environment variable for better security!
DATABASE_URL = "postgresql://adminavo:%24%23fKcdXPg4%40ue8AW@avo-adb-001.postgres.database.azure.com:5432/EDI%20IA"

def get_pg_connection():
    return psycopg2.connect(DATABASE_URL)


def decode_base64_csv(b64_string):
    try:
        csv_bytes = base64.b64decode(b64_string)
        csv_text = csv_bytes.decode('utf-8')
        return csv_text
    except Exception as e:
        logging.error(f"Base64 decoding error: {e}")
        raise


_BOM = "\ufeff"

def _normalize_colname(name):
    if isinstance(name, str):
        return name.replace(_BOM, "").strip()
    return name

def _find_col_index(header, wanted_names):
    """
    Returns the original index in `header` for any of the wanted_names,
    after normalizing BOM/whitespace.
    """
    norm_map = { _normalize_colname(col): i for i, col in enumerate(header) }
    for w in wanted_names:
        w_norm = _normalize_colname(w)
        if w_norm in norm_map:
            return norm_map[w_norm]
    return None

def detect_company_and_prepare(rows):
    logging.warning(f"DEBUG: starting detect_company_and_prepare; rows_count={0 if rows is None else len(rows)}")

    if not rows:
        logging.warning("DEBUG: rows empty or None -> returning (None, [])")
        return None, []

    header = rows[0]
    logging.warning(f"DEBUG: header_len={len(header)}; header_preview={header}")

    # --- Valeo via 'Org_Name_Customer'
    org_col = _find_col_index(header, ["Org_Name_Customer"])
    if org_col is not None:
        logging.warning(f"DEBUG: 'Org_Name_Customer' index={org_col}")
        matches = 0
        sample_vals = []
        for r in rows[1:]:
            v = r[org_col].strip() if len(r) > org_col and isinstance(r[org_col], str) else ""
            if len(sample_vals) < 8:
                sample_vals.append(v)
            if v == 'Valeo':
                matches += 1
        logging.warning(f"DEBUG: Valeo check -> matches={matches}; sample_vals={sample_vals}")
        if matches:
            logging.warning("DEBUG: Detected company='Valeo'")
            return 'Valeo', header
    else:
        logging.warning("DEBUG: header missing 'Org_Name_Customer'")

    # --- Inteva via 'Site/Building' in ['ESS2', 'GAD1']
    site_col = _find_col_index(header, ["Site/Building"])
    if site_col is not None:
        logging.warning(f"DEBUG: 'Site/Building' index={site_col}")
        targets = {'ESS2', 'GAD1'}
        matches = 0
        sample_vals = []
        for r in rows[1:]:
            v = r[site_col].strip() if len(r) > site_col and isinstance(r[site_col], str) else ""
            if len(sample_vals) < 8:
                sample_vals.append(v)
            if v in targets:
                matches += 1
        logging.warning(f"DEBUG: Inteva check targets={list(targets)} -> matches={matches}; sample_vals={sample_vals}")
        if matches:
            logging.warning("DEBUG: Detected company='Inteva'")
            return 'Inteva', header
    else:
        logging.warning("DEBUG: header missing 'Site/Building'")

    # --- Nidec via 'Plant' or '\ufeffPlant' in ['BI01', 'ZI01', 'SPER']
    plant_col = _find_col_index(header, ["Plant", "\ufeffPlant"])
    if plant_col is not None:
        logging.warning(f"DEBUG: 'Plant' index={plant_col} (handles BOM)")
        targets = {'BI01', 'ZI01', 'SPER'}
        matches = 0
        sample_vals = []
        for r in rows[1:]:
            v = r[plant_col].strip() if len(r) > plant_col and isinstance(r[plant_col], str) else ""
            if len(sample_vals) < 8:
                sample_vals.append(v)
            if v in targets:
                matches += 1
        logging.warning(f"DEBUG: Nidec check targets={list(targets)} -> matches={matches}; sample_vals={sample_vals}")
        if matches:
            logging.warning("DEBUG: Detected company='Nidec'")
            return 'Nidec', header
    else:
        logging.warning("DEBUG: header missing 'Plant' (including BOM variant)")

    logging.warning("DEBUG: No company detected; returning (None, header)")
    return None, header




def to_a_week(date_str):
    """Convertit une date en chaîne 'YYYY-WXX' (semaine ISO), sans ajustement."""
    dt = parse_date_flexible(date_str)
    if not dt:
        return ""
    week = dt.isocalendar()[1]
    year = dt.isocalendar()[0]
    return f"{year}-W{week:02d}"




def to_forecast_week(raw):
    """Convertit une date (format libre) ou CW en semaine ISO 'YYYY-WXX'"""
    raw = raw.strip()
    if not raw or raw.upper() == "BACKORDER":
        return raw  # conserve BACKORDER tel quel
    # Cas CW 37/2025
    m = re.match(r"CW\s*(\d{1,2})/(\d{4})", raw, re.IGNORECASE)
    if m:
        week = int(m.group(1))
        year = int(m.group(2))
        return f"{year}-W{week:02d}"

    # Sinon, essaye de parser une vraie date
    dt = parse_date_flexible(raw)
    if dt:
        week = dt.isocalendar()[1]
        year = dt.isocalendar()[0]
        return f"{year}-W{week:02d}"
    
    logging.warning(f"[Valeo] Format de date inconnu: '{raw}'")
    return ""



def process_valeo_rows(rows, header):
    plant_to_client = {
        "SK01": "C00250",
        "W113": "C00303",
        "FUEN": "C00125",
        "BN01": "C00132",
    }

    # Client material → AVOMaterialNo mapping (Valeo-specific)
    material_to_avo = {
        "473801": "V473801",
        "469917D": "V469.917D",      # as you specified
        "471346D": "V471.346D",
        "471553D": "V471.553D",
        "W000023134D": "VW000023134",
    }

    processed = []
    idx = {col: header.index(col) for col in header}

    for row in rows[1:]:
        try:
            # Skip header/garbage line if Customer_No is not numeric
            if 'Customer_No' in idx and not row[idx['Customer_No']].isnumeric():
                continue

            plant = row[idx['Plant_No']].strip()
            client_code = plant_to_client.get(plant, None)
            if not client_code:
                # Unknown plant → skip line
                continue

            delivery_date = row[idx['Delivery_Date']].strip()
            date_str = row[idx['Date']].strip()

            # Parse date and compute forecast week
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                date_obj = datetime.strptime(date_str, "%d.%m.%Y")

            week_num = date_obj.isocalendar()[1]
            # If after Tuesday, push to next week
            if date_obj.weekday() > 1:
                week_num += 1
            forecast_date = f"{date_obj.year}-W{week_num:02d}"

            # --- Material + AVO mapping logic ---
            material_code = row[idx['Material_No_Customer']].strip().upper()

            # 1) Try explicit mapping first
            mapped_avo = material_to_avo.get(material_code)

            if mapped_avo:
                AVOmaterial_code = mapped_avo
            else:
                # 2) Fallback to old logic (V + material + POL/SLP)
                AVOmaterial_code = (
                    material_code if material_code.startswith("V")
                    else "V" + material_code
                )
                if client_code == "C00250":
                    AVOmaterial_code += "POL"
                elif client_code == "C00303":
                    AVOmaterial_code += "SLP"

            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": to_forecast_week(delivery_date),
                "DateUntil": delivery_date,
                "Quantity": int(row[idx['Despatch_Qty']].strip() or 0),
                "ForecastDate": to_forecast_week(date_str),
                "LastDeliveryDate": to_forecast_week(row[idx['Last_Delivery_Note_Date']].strip()),
                "LastDeliveredQuantity": int(row[idx['Last_Delivery_Quantity']].strip() or 0),
                "CumulatedQuantity": int(row[idx['Cum_Quantity']].strip() or 0),
                "EDIStatus": {
                    "p": "Forecast",
                    "P": "Forecast",
                    "f": "Firm",
                    "F": "Firm"
                }.get(row[idx['Commitment_Level']].strip(), row[idx['Commitment_Level']].strip()),
                "ProductName": row[idx['Description']].strip(),
                "LastDeliveryNo": row[idx['Last_Delivery_Note']].strip()
            })

        except Exception as e:
            logging.error(f"Valeo row processing error: {e}")

    logging.warning(f"DEBUG: Valeo processed {len(processed)} records from {len(rows)-1} data rows")
    return processed





def extract_material_code(val):
    match = re.match(r"(\d+)", val)
    if match:
        return f"{match.group(1)}"
    return ""





def parse_date_flexible(date_str):
    """Try multiple date formats and return a datetime object or None."""
    formats = [
        "%Y-%m-%d",   # e.g. 2025-07-03
        "%d.%m.%Y",   # e.g. 03.07.2025
        "%d.%m.%y",
        "%d/%m/%Y",   # e.g. 03/07/2025
        "%d/%m/%y",   # e.g. 03/07/25 ← important!
        "%Y/%m/%d",   # e.g. 2025/07/03
        "%d-%m-%Y",   # e.g. 03-07-2025
        "%d-%m-%y",   # e.g. 03-07-25
        "%m/%d/%Y",   # e.g. 07/03/2025 (US-style)
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except (ValueError, TypeError):
            continue
    return None



def process_inteva_rows(rows, header):
    site_to_client = {
        "ESS2": "C00410",
        "GAD1": "C00241",
    }
    processed = []
    header = [h.strip() for h in header if h.strip() != ""]
    idx = {col: header.index(col) for col in header}

    # Flexible lookup for the material code/part column
    material_code_col = None
    for key in ['Material Code or Part-Revision', 'Material Code', 'Part-Revision']:
        if key in idx:
            material_code_col = key
            break

    # Find correct 'Site/Building' header name
    site_building_col = None
    for candidate in ['Site/Building', 'Site/Building,']:
        if candidate in idx:
            site_building_col = candidate
            break
    if not site_building_col:
        logging.error("Site/Building column not found in header.")
        return processed
    
    forecast_date = ""
    if len(rows) > 1:
        first_due_date_val = rows[1][idx['Due Date']].strip()
        dt = parse_date_flexible(first_due_date_val)
        if dt:
            week_num = dt.isocalendar()[1]
            if dt.weekday() > 1:  # Friday or later
                week_num += 1
            forecast_date = f"{dt.year}-W{week_num:02d}"

    product_running = defaultdict(lambda: None)

    
    for row in rows[1:]:
        try:
            # Defensive: If row is shorter than header, pad with empty strings
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            site = row[idx[site_building_col]].strip()
            client_code = site_to_client.get(site, None)
            if not client_code or material_code_col is None:
                continue
            part_val = row[idx[material_code_col]].strip()
            material_code = extract_material_code(part_val)
            qty = parse_euro_number(row[idx['Quantity']].strip())
            total_received = parse_euro_number(row[idx['Total Received']].strip())
            balance = parse_euro_number(row[idx['Balance']].strip())
            last_cum = product_running[material_code]
            if last_cum is None:
                cumulated = total_received + balance
            else:
                cumulated = last_cum + balance
            product_running[material_code] = cumulated

            # Parse Last Receipt Date robustly
            last_receipt_date_raw = row[idx['Last Receipt Date']].strip()
            last_receipt_date_dt = parse_date_flexible(last_receipt_date_raw)
            last_receipt_date = last_receipt_date_dt.strftime("%Y-%m-%d") if last_receipt_date_dt else ""
            AVOmaterial_code = "V" + material_code
            if client_code == "C00241" :
                AVOmaterial_code =  AVOmaterial_code + "GAD"
            else : 
                AVOmaterial_code =  AVOmaterial_code
        
            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": to_a_week(row[idx['Due Date']].strip()),
                "DateUntil": row[idx['Due Date']].strip(),
                "Quantity": qty,
                "ForecastDate": forecast_date,
                "LastDeliveryDate": to_a_week(last_receipt_date),
                "LastDeliveredQuantity": parse_euro_number(row[idx['Last Receipt Quantity']].strip()),
                "CumulatedQuantity": cumulated,
                "EDIStatus": {
                    "On Order": "Firm",
                    "Forecast": "Forecast",
                    "ON ORDER": "Firm",
                    "FORECAST": "Forecast"
                }.get(row[idx['Release Status']].strip(), row[idx['Release Status']].strip()),
                "ProductName": row[idx['Description']].strip(),
                "LastDeliveryNo": None

            })
        except Exception as e:
            logging.error(f"Inteva row processing error: {e}")
    logging.warning(f"DEBUG: Inteva processed {len(processed)} records from {len(rows)-1} data rows")
    return processed


def _normalize_colname(name: str) -> str:
    # strip UTF-8/UTF-16 BOM if present and trim whitespace
    if isinstance(name, str):
        return name.replace(_BOM, "").strip()
    return name

def _build_index(header):
    """
    Build an index dict using *normalized* header names.
    Example: {'Plant': 3, 'CallOffDate': 4, ...}
    """
    norm_header = [_normalize_colname(c) for c in header]
    idx = {col: i for i, col in enumerate(norm_header)}
    # debug visibility
    if header and header[0] != norm_header[0]:
        logging.warning(f"DEBUG: BOM detected in first header cell; raw='{header[0]}', normalized='{norm_header[0]}'")
    logging.warning(f"DEBUG: header normalized => {norm_header}")
    return idx

def process_nidec_rows(rows, header):
    # New mapping: Old Part N° (current AVOMaterialNo) -> New Part N°
    NEW_AVO_MAPPING = {
        # Nidec ESP
        "VA18116507G": "V504.519SP",
        "VA14116701A": "V504.243SP",
        "VA14116698P": "V504.510SP",
        "VA13116595N": "V502.730SP",
        # Nidec POL
        "V504.519": "V504.519PL",
        "V504.243": "V504.243PL",
        "V504.510": "V504.510PL",  # corrected (was "504.510 PL")
    }

    def apply_new_mapping(avo_code: str) -> str:
        mapped = NEW_AVO_MAPPING.get(avo_code.strip(), avo_code)
        if mapped != avo_code:
            logging.warning(f"DEBUG: AVOMaterialNo mapped '{avo_code}' -> '{mapped}'")
        return mapped

    plant_to_client = {
        "ZI01": "C00126",
        "SPER": "C00050",
        "BI01": "C00113",
    }

    if not rows:
        logging.warning("DEBUG: rows empty or None -> returning []")
        return []

    # --- BOM-aware header index
    idx = _build_index(header)

    # Required fields (normalized)
    required_fields = [
        'Plant', 'CallOffDate', 'Material', 'DateFrom', 'DateUntil', 'DespatchQty',
        'LastDeliveryDate', 'LastDeliveryQuantity', 'CumQuantity', 'Status', 'LastDeliveryNo'
    ]
    missing = [f for f in required_fields if f not in idx]
    if missing:
        logging.error(f"Column(s) missing in Nidec CSV (normalized): {missing}")
        return []

    processed = []

    # quick shape sanity
    for i, row in enumerate(rows[1:], 1):
        if len(row) != len(header):
            logging.warning(f"Row length mismatch at row {i}: {len(row)} fields vs header {len(header)}. Row: {row}")

    for i, row in enumerate(rows[1:]):
        try:
            if len(row) < len(header):
                row += [""] * (len(header) - len(row))

            plant_raw = row[idx['Plant']]
            plant = plant_raw.strip() if isinstance(plant_raw, str) else ""
            if plant not in plant_to_client:
                logging.warning(f"SKIP row {i+1}: Plant '{plant}' not recognized in plant_to_client")
                continue

            client_code = plant_to_client.get(plant)
            if not client_code:
                logging.warning(f"SKIP row {i+1}: No client_code for plant '{plant}'")
                continue

            call_off_date_str = (row[idx['CallOffDate']].strip()
                                 if isinstance(row[idx['CallOffDate']], str) else "")
            logging.warning(f"DEBUG: Row {i+1}: CallOffDate = '{call_off_date_str}'")
            if not call_off_date_str:
                logging.warning(f"SKIP row {i+1}: Empty CallOffDate")
                continue

            try:
                date_obj = datetime.strptime(call_off_date_str, "%Y-%m-%d")
            except ValueError:
                try:
                    date_obj = datetime.strptime(call_off_date_str, "%d.%m.%Y")
                except ValueError:
                    logging.warning(f"SKIP row {i+1}: Unparsable CallOffDate '{call_off_date_str}'")
                    continue

            # ISO week; shift if day > Tuesday (like your original logic)
            week_num = date_obj.isocalendar()[1]
            if date_obj.weekday() > 1:
                week_num += 1
            forecast_date = f"{date_obj.year}-W{week_num:02d}"

            material_val = row[idx['Material']]
            material_code = material_val.strip() if isinstance(material_val, str) else ""
            # --- make AVOMaterialNo
            def make_avo_material_code(code):
                code = code.strip()
                if code == "502-730-99-99":
                    return "VA13116595N"
                # If contains any letter, just add V in front
                if re.search(r'[a-zA-Z]', code):
                    return "V" + code
                # Else, expect format like 503-996-99-99, take first two parts
                parts = code.split('-')
                if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                    return f"V{parts[0]}.{parts[1]}"
                # Fallback: just add V
                return "V" + code

            avo_material_code = make_avo_material_code(material_code)
            avo_material_code_mapped = apply_new_mapping(avo_material_code)

            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": avo_material_code_mapped,
                "DateFrom": to_forecast_week((row[idx['DateFrom']].strip()
                                              if isinstance(row[idx['DateFrom']], str) else "")),
                "DateUntil": (row[idx['DateUntil']].strip()
                              if isinstance(row[idx['DateUntil']], str) else ""),
                "Quantity": parse_euro_number((row[idx['DespatchQty']].strip()
                                               if isinstance(row[idx['DespatchQty']], str) else "")),
                "ForecastDate": forecast_date,
                "LastDeliveryDate": to_forecast_week((row[idx['LastDeliveryDate']].strip()
                                                      if isinstance(row[idx['LastDeliveryDate']], str) else "")),
                "LastDeliveredQuantity": parse_euro_number((row[idx['LastDeliveryQuantity']].strip()
                                                            if isinstance(row[idx['LastDeliveryQuantity']], str) else "")),
                "CumulatedQuantity": parse_euro_number((row[idx['CumQuantity']].strip()
                                                        if isinstance(row[idx['CumQuantity']], str) else "")),
                "EDIStatus": (row[idx['Status']].strip()
                              if isinstance(row[idx['Status']], str) else ""),
                "ProductName": None,
                "LastDeliveryNo": (row[idx['LastDeliveryNo']].strip()
                                   if isinstance(row[idx['LastDeliveryNo']], str) else "")
            })
        except Exception as e:
            logging.error(f"Nidec row processing error at row {i+1}: {e}")
            logging.error(f"Nidec row processing error: {e}")

    logging.warning(f"DEBUG: Nidec processed {len(processed)} records from {len(rows)-1} data rows")
    return processed


def parse_euro_number(val):
    """Convert European-style number strings to float or int safely."""
    if val is None or str(val).strip() == '':
        return 0
    # Remove thousands separator, fix decimal comma
    sanitized = str(val).replace('.', '').replace(',', '.')
    try:
        # If it's decimal, return float, else int
        return float(sanitized) if '.' in sanitized else int(sanitized)
    except ValueError:
        return 0



def save_to_postgres_with_conflict_reporting(extracted_records):
    conn = None
    success_count = 0
    error_details = []
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            for record in extracted_records:
                try:
                    cur.execute("""
                        INSERT INTO public."EDIGlobal" (
                            "Site", "ClientCode", "ClientMaterialNo", "AVOMaterialNo", "DateFrom",
                            "DateUntil", "Quantity", "ForecastDate", "LastDeliveryDate",
                            "LastDeliveredQuantity", "CumulatedQuantity", "EDIStatus", "ProductName","LastDeliveryNo"
                        ) VALUES (
                            %(Site)s, %(ClientCode)s, %(ClientMaterialNo)s, %(AVOMaterialNo)s, %(DateFrom)s,
                            %(DateUntil)s, %(Quantity)s, %(ForecastDate)s, %(LastDeliveryDate)s,
                            %(LastDeliveredQuantity)s, %(CumulatedQuantity)s, %(EDIStatus)s, %(ProductName)s, %(LastDeliveryNo)s
                        )
                    """, record)
                    success_count += 1
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    error_details.append({
                        "record": record,
                        "error": "Duplicate primary key: record already exists."
                    })
                except psycopg2.DataError as de:
                    conn.rollback()
                    error_details.append({
                        "record": record,
                        "error": f"Data error: {str(de)}"
                    })
                except psycopg2.Error as pe:
                    conn.rollback()
                    error_details.append({
                        "record": record,
                        "error": f"Database error: {str(pe)}"
                    })
            conn.commit()
        return success_count, error_details
    except Exception as e:
        logging.error(f"Database error: {e}")
        return 0, [{"error": str(e)}]
    finally:
        if conn:
            conn.close()



def clean_super_weird_csv(text):
    # Remove all double quotes from the entire file
    cleaned_text = text.replace('"', '')

    # Replace commas with semicolons (assuming original delimiter is comma)
    cleaned_text = cleaned_text.replace(',', ';')

    # Optionally, strip any trailing semicolons from each line
    cleaned_lines = []
    for line in cleaned_text.splitlines():
        if line.endswith(';'):
            line = line[:-1]
        cleaned_lines.append(line)
    return '\n'.join(cleaned_lines)



def read_clean_csv(csv_text):
    # Remove double quotes (if you want, or comment out if not needed)
    cleaned_text = csv_text.replace('"', '')

    # Detect delimiter (comma or semicolon)
    try:
        dialect = csv.Sniffer().sniff(cleaned_text, delimiters=",;")
        logging.warning(f"Detected delimiter: '{dialect.delimiter}'")
    except Exception:
        dialect = csv.excel  # fallback to default (comma)
        logging.warning("Could not detect delimiter. Defaulting to ','.")

    csv_io = io.StringIO(cleaned_text)
    rows = list(csv.reader(csv_io, dialect))

    # Clean all cells (strip spaces)
    rows = [[cell.strip() for cell in row] for row in rows]

    # Debug: log the first 3 rows
    logging.warning(f"DEBUG: Rows after cleaning: {len(rows)} rows")
    logging.warning(f"DEBUG: Header: {rows[0] }")
    for i, row in enumerate(rows[1:4]):
        logging.warning(f"DEBUG: Row {i+1}: {row}")

    return rows


def decode_and_clean_csv(b64_string):
    try:
        csv_bytes = base64.b64decode(b64_string)
        try:
            csv_text = csv_bytes.decode('utf-8')
        except UnicodeDecodeError:
            csv_text = csv_bytes.decode('latin1')
        # Only remove quotes
        csv_text = csv_text.replace('"', '')
        # Detect delimiter from the first line
        first_line = csv_text.splitlines()[0]
        comma_count = first_line.count(',')
        semi_count = first_line.count(';')
        # If comma delimiter, convert to semicolon
        if comma_count > semi_count:
            logging.warning("Detected comma delimiter, converting to semicolon.")
            csv_text = csv_text.replace(',', ';')
        else:
            logging.warning("Detected semicolon delimiter, leaving as is.")
        return csv_text
    except Exception as e:
        logging.error(f"Base64 or decode error: {e}")
        raise





def detect_pdf_format(file_bytes):
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        # Combine all text for easy searching
        all_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
        lines = all_text.splitlines()
        
        # 1. Nidec: First 10 lines, look for "NIDEC"
        for line in lines[:10]:
            if "NIDEC" in line.upper():
                return "nidec"
        
        # 2. Pierburg: Look for "Organization: PIERB"
        if any("Organization: PIERB" in line for line in lines):
            return "pierburg"
        
        # 3. Valeo France (Nevers) or Brasil (Campinas)
        # Search all lines for "DELIVERY IN:"
        for idx, line in enumerate(lines):
            if "DELIVERY IN:" in line:
                # Try to get the next two lines for multi-line addresses
                combined = line
                if idx+1 < len(lines): combined += " " + lines[idx+1]
                if idx+2 < len(lines): combined += " " + lines[idx+2]
                
                # Normalize spaces for matching
                combined = re.sub(r'\s+', ' ', combined)
                
                # Check for Valeo Nevers
                if re.search(r"DELIVERY IN:\s*VALEO CIE Nevers", combined, re.IGNORECASE):
                    return "valeo_nevers"
                # Check for Valeo Campinas
                elif re.search(r"DELIVERY IN:\s*VWS Campinas", combined, re.IGNORECASE):
                    return "valeo_campinas"
        
        # Unknown format
        return None


def extract_value_after_label(lines, label, default=None):
    for i, line in enumerate(lines):
        if label in line:
            # Get text after the label
            rest = line.split(label, 1)[-1].strip()
            if rest:
                return rest
            # Else maybe it's on next line
            if i + 1 < len(lines):
                return lines[i+1].strip()
    return default



def process_pierburg_pdf(file_bytes, file_name):
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        all_text = "\n".join(page.extract_text() or '' for page in pdf.pages)
        lines = all_text.splitlines()

        # Material (Customer): 501312040 Material description (Customer):
        m = re.search(r"Material \(Customer\):\s*(\S+)", all_text)
        material_customer = m.group(1) if m else ""

        # Material description (Customer): (optional)
        m = re.search(r"Material description \(Customer\):\s*([^\n]*)", all_text)
        product_name = "BRUSHCARD BG42"

        forecast_date = ""
        for line in lines:
            if "Delivery Instruction Number:" in line and "Date:" in line:
                logging.warning(f"[PIERBURG] Matched line: {repr(line)}")
                parts = line.split("Date:")
                logging.warning(f"[PIERBURG] Parts after split: {parts}")
                if len(parts) == 2:
                    date_str = parts[1].strip().split()[0]  # e.g. "03/07/25"
                    logging.warning(f"[PIERBURG] Extracted date string: {date_str}")
                    date_obj = parse_date_flexible(date_str)
                    logging.warning(f"[PIERBURG] Parsed datetime object: {date_obj}")
                    if date_obj:
                        week_num = date_obj.isocalendar()[1]
                        weekday = date_obj.weekday()
                        logging.warning(f"[PIERBURG] Week num: {week_num}, Weekday: {weekday}")
                        if weekday > 1:  # Not Monday or Tuesday
                            week_num += 1
                            logging.warning("[PIERBURG] Added +1 to week number because weekday > 1")
                        forecast_date = f"{date_obj.year}-W{week_num:02d}"
                        logging.warning(f"[PIERBURG] Final forecast_date: {forecast_date}")
                    else:
                        logging.error(f"[PIERBURG] Could not parse date from string: {date_str}")
                else:
                    logging.error(f"[PIERBURG] Split on 'Date:' did not yield 2 parts: {parts}")
                break

        if not forecast_date:
            logging.error("[PIERBURG] forecast_date could not be extracted from the document.")



        # Deliverynote Number: C00285/20/06/25 Date: 25/06/25 Quantity: 1.080
        m = re.search(r"Deliverynote Number:\s*(\S+)", all_text)
        last_delivery_no = m.group(1) if m else ""

        m = re.search(r"Deliverynote Number:.*?Date:\s*([\d/\.]+)", all_text)
        last_delivery_date = m.group(1) if m else ""

        m = re.search(r"Deliverynote Number:.*?Quantity:\s*([\d.,]+)", all_text)
        last_delivered_qty = parse_euro_number(m.group(1)) if m else 0

        # Schedule line: 09/07/25 2.160 527.349 Fix
        schedule_regex = re.compile(
            r'(\d{2}/\d{2}/\d{2})\s+([\d.,]+)\s+([\d.,]+)\s+(\w+)', re.IGNORECASE
        )

        results = []
        for line in lines:
            m = schedule_regex.match(line)
            if not m:
                continue
            delivery_date, dispatch_qty, cum_quantity, diff_commit = m.groups()
            if diff_commit == "Fix":
                diff_commit="Firm"
            results.append({
                "Site": "Tunisia",
                "ClientCode": "C00285",
                "ClientMaterialNo": material_customer,
                "AVOMaterialNo": f"V{material_customer}",
                "DateFrom": to_a_week(delivery_date),
                "DateUntil": delivery_date,
                "Quantity": parse_euro_number(dispatch_qty),
                "ForecastDate": forecast_date,
                "LastDeliveryNo": last_delivery_no,
                "LastDeliveryDate": to_a_week(last_delivery_date),
                "LastDeliveredQuantity": last_delivered_qty,
                "CumulatedQuantity": parse_euro_number(cum_quantity),
                "EDIStatus": diff_commit,
                "ProductName": product_name,
            })

        return results



def pars_euro_number(val):
    """Supports both Euro and US style, but prioritize your PDF style."""
    if val is None or str(val).strip() == '':
        return 0
    s = str(val).replace(' ', '')
    # Euro style: 1.234,56
    if '.' in s and ',' in s and s.rfind(',') > s.rfind('.'):
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace(',', '')
    try:
        return float(s) if '.' in s else int(s)
    except ValueError:
        return 0




def process_nidec_pdf(file_bytes, file_name):
    results = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        all_text = "\n".join(page.extract_text() or '' for page in pdf.pages)
        lines = all_text.splitlines()
        logging.warning(f"[NIDEC] Number of extracted lines: {len(lines)}")
        for i, line in enumerate(lines):
            logging.warning(f"[NIDEC][LINE {i}] {repr(line)}")

    # --- Extract PO Date and PO Type ---
    po_date, po_type = "", ""
    for line in lines:
        m = re.search(r'PO\s*Date\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})', line) or re.search(r'PODate\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})', line)
        if m:
            po_date = m.group(1)
            logging.warning(f"[NIDEC] Extracted po_date: {po_date}")
        m = re.search(r'PO\s*Type\s*:\s*([A-Za-z ]+)', line) or re.search(r'POType\s*:\s*([A-Za-z ]+)', line)
        if m:
            po_type = re.split(r'[\s\-]', m.group(1).strip())[0]
            logging.warning(f"[NIDEC] Extracted po_type: {po_type}")

    # --- Compute ForecastDate ---
    forecast_date = ""
    date_obj = parse_date_flexible(po_date)
    logging.warning(f"[NIDEC] Parsed po_date: {date_obj}")
    if date_obj:
        week_num = date_obj.isocalendar()[1]
        weekday = date_obj.weekday()
        logging.warning(f"[NIDEC] Week num: {week_num}, Weekday: {weekday}")
        if weekday > 1:
            week_num += 1
            logging.warning("[NIDEC] Added +1 to week number because weekday > 1")
        forecast_date = f"{date_obj.year}-W{week_num:02d}"
        logging.warning(f"[NIDEC] Final forecast_date: {forecast_date}")
    else:
        logging.error("[NIDEC] Could not parse po_date for week calculation.")

    # --- Join item lines ---
    matched_lines = 0
    i = 0
    while i < len(lines) - 1:
        line1 = lines[i]
        line2 = lines[i + 1]
        # Looks for a line that starts with a number and has qty+date at end, and the next line is part of drawing/desc
        m = re.match(
            r'^\s*(\d+)\s+(\d+)\s+([A-Z0-9]+)\s+([A-Za-z0-9-]+)\s+\d+\s+([\d,]+)\s+[A-Z]+\s+[\d.]+[\s\d.,]+([\d/]{10})',
            line1
        )
        if m:
            matched_lines += 1
            item_code = m.group(2)
            drawing_no_1 = m.group(3)
            drawing_no_2 = ''  # Not used, but you can parse if present
            product_name = m.group(4)
            quantity = pars_euro_number(m.group(5))
            req_date = m.group(6)
            # Get drawing/desc from next line
            desc_match = re.match(r'^\d+\s+(.*)', line2)
            extra_desc = desc_match.group(1) if desc_match else ''
            avo_material_no = drawing_no_1 + (drawing_no_2 if drawing_no_2 else '')
            # (Optionally) use extra_desc in your ProductName if you need
            results.append({
                "Site": "Tunisia",
                "ClientCode": "C00260",
                "ClientMaterialNo": item_code,
                "AVOMaterialNo": avo_material_no,
                "DateFrom": to_a_week(req_date),
                "DateUntil": req_date,
                "Quantity": quantity,
                "ForecastDate": forecast_date,
                "LastDeliveryDate": None,
                "LastDeliveredQuantity": None,
                "CumulatedQuantity": None,
                "EDIStatus": po_type,
                "ProductName": product_name + " " + extra_desc,
                "LastDeliveryNo": None
            })
            logging.warning(f"[NIDEC] Matched item at lines {i}/{i+1}: {item_code}, {avo_material_no}, {quantity}, {req_date}, {product_name + ' ' + extra_desc}")
            i += 2  # skip next line too
        else:
            i += 1
    logging.warning(f"[NIDEC] Total matched data lines: {matched_lines}")
    logging.warning(f"[NIDEC] Total results: {len(results)}")
    return results




def parse_week_number(date_str):
    if not date_str:
        return ""
    
    formats = ["%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            week_num = dt.isocalendar()[1]
            year = dt.isocalendar()[0]
            return f"{year}-W{week_num:02d}"
        except ValueError:
            continue

    logging.warning(f"[parse_week_number] Invalid date: {date_str}")
    return ""




def convert_mmddyyyy_to_week(date_str):
    """
    Convert a MM/DD/YYYY date string to ISO week format YYYY-WXX.
    Returns empty string on failure.
    """
    try:
        logging.warning(f"[convert_mmddyyyy_to_week] Raw input: {date_str}")
        month, day, year = map(int, date_str.strip().split("/"))
        dt = date(year, month, day)
        iso_year, iso_week, _ = dt.isocalendar()
        week_str = f"{iso_year}-W{iso_week:02d}"
        logging.warning(f"[convert_mmddyyyy_to_week] Parsed: {dt} → Week: {week_str}")
        return week_str
    except Exception as e:
        logging.error(f"[convert_mmddyyyy_to_week] Error: {e}")
        return ""




def parse_any_date(raw_date):
    """Force le format MM/DD/YYYY uniquement"""
    try:
        return datetime.strptime(raw_date.strip(), "%m/%d/%Y").date()
    except:
        return None




def process_valeo_campinas_pdf(pdf_bytes, file_name):
    data = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_pages_text = [page.get_text() for page in doc]
    full_text = "\n".join(all_pages_text)
    logging.info(f"[VALEO] Extracted {len(full_text)} characters of text.")

    # Extract ForecastDate ONCE from the whole document
    m_date = re.search(r"DATE[: ]+(\d{2}/\d{2}/\d{4})", full_text)
    forecast_week = ""
    if m_date:
        raw_forecast_date = m_date.group(1)
        logging.warning(f"[VALEO] Raw Forecast Date from document: {raw_forecast_date}")
        forecast_week = convert_mmddyyyy_to_week(raw_forecast_date)
        logging.warning(f"[VALEO] Converted Forecast week: {forecast_week}")
    else:
        logging.warning(f"[VALEO] No forecast date found in the entire document.")

    # Split into sections by SCHEDULING AGREEMENT
    sched_split = list(re.finditer(r"SCHEDULING AGREEMENT \d+", full_text))
    agreement_numbers = [re.search(r"(\d+)", m.group()).group(1) for m in sched_split]

    # Find the start page for each agreement
    agreement_start_pages = []
    for agreement_no in agreement_numbers:
        for i, page_text in enumerate(all_pages_text):
            if f"SCHEDULING AGREEMENT {agreement_no}" in page_text:
                agreement_start_pages.append(i)
                break
    agreement_start_pages.append(len(all_pages_text))  # Add end boundary

    for idx, agreement_no in enumerate(agreement_numbers):
        section_start_page = agreement_start_pages[idx]
        section_end_page = agreement_start_pages[idx + 1]
        section_pages = all_pages_text[section_start_page:section_end_page]

        current_material = None
        current_product = None
        avo_material = None
        raw_last_date = ""
        last_delivery_no = None
        last_delivered_qty = None

        for page_no, page_text in enumerate(section_pages):
            # Update material/product if found on this page
            m_prod = re.search(
                r"Material\s+([A-Z0-9]+)\s+([^\n]+?)\s+Unit\s+of\s+Measure", page_text)
            if m_prod:
                current_material = m_prod.group(1)
                current_product = m_prod.group(2).strip()
                avo_material = "V" + current_material

            # Last Delivery block
            m_last = re.search(
                r"LAST DELIVERY.*?DEL DATE\s+DOCUMENT\s+QUANTITY\s*\n?(\d{2}/\d{2}/\d{4})\s+(\S+)\s+([\d,\.]+)",
                page_text, re.DOTALL)
            if m_last:
                raw_last_date = m_last.group(1)
                logging.warning(f"[VALEO][{agreement_no}][PAGE {page_no+1}] Raw LastDeliveryDate: {raw_last_date}")
                last_delivery_no = m_last.group(2)
                last_delivered_qty = pars_euro_number(m_last.group(3))

            # Forecast rows
            forecast_rows = re.findall(
                r"(FORECAST|PAST DUE)\s+(\d{2}/\d{2}/\d{4})?\s+([\d,.]+)\s+([\d,.]+)", page_text)

            for idx_row, (status, date_str, qty, cumm) in enumerate(forecast_rows):
                if not date_str:
                    date_str = ""
                    logging.warning(f"[VALEO][{agreement_no}][PAGE {page_no+1}][ROW {idx_row}] Inserting row with missing date: {status}, {qty}, {cumm}")
                if avo_material not in ("V1001MR035","VW000056480","VW000024158") and not avo_material.endswith("BRA"):
                    avo_material += "BRA"

                row = {
                    "ClientCode": "C00072",
                    "SchedulingAgreement": agreement_no,
                    "ForecastDate": forecast_week,
                    "ClientMaterialNo": current_material,
                    "AVOMaterialNo": avo_material,
                    "ProductName": current_product,
                    "EDIStatus": status,
                    "DateFrom": convert_mmddyyyy_to_week(date_str),
                    "DateUntil": date_str,
                    "Quantity": float(qty.replace(',', '').replace('.', '')),
                    "CumulatedQuantity": float(cumm.replace(',', '').replace('.', '')),
                    "LastDeliveryDate": convert_mmddyyyy_to_week(raw_last_date),
                    "LastDeliveryNo": last_delivery_no,
                    "LastDeliveredQuantity": last_delivered_qty,
                    "Site": "Tunisia"
                }
                data.append(row)

    # Deduplicate rows
    unique_rows = {}
    for row in data:
        pk = (
            row['Site'], row['ClientCode'], row['AVOMaterialNo'],
            row['DateFrom'], row['Quantity'], row['ForecastDate']
        )
        if pk not in unique_rows:
            unique_rows[pk] = row
    data = list(unique_rows.values())

    logging.info(f"[VALEO] Extraction complete. Total records: {len(data)}")
    return data


def to_adjusted_iso_week(date_str):
    """Convert dd.mm.yyyy to yyyy-Wxx, add +1 if Wednesday or later."""
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        weekday = dt.weekday()  # Monday=0, Tuesday=1, ..., Sunday=6
        iso_year, iso_week, _ = dt.isocalendar()
        if weekday >= 2:  # Wednesday (2) or later
            dt_next = dt + datetime.timedelta(days=(7 - weekday))
            iso_year, iso_week, _ = dt_next.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    except Exception as e:
        logging.warning(f"Couldn't convert ForecastDate '{date_str}' to week: {e}")
        return ""


def to_week(date_str: str) -> str:
    date_str = date_str.strip()

    # Case 1: Format is WW.YYYY → like 38.2025
    if re.match(r"^\d{2}\.\d{4}$", date_str):
        week, year = date_str.split(".")
        try:
            week = int(week)
            year = int(year)
            # Get Monday of that ISO week
            date_obj = datetime.strptime(f"{year}-W{week}-1", "%Y-W%W-%w")
            return f"{year}-W{week:02d}"
        except Exception as e:
            logging.warning(f"[to_a_week] Failed to parse WW.YYYY: {date_str} – {e}")
            return date_str

    # Case 2: Format is DD.MM.YYYY
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", date_str):
        try:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            week = date_obj.isocalendar()[1]
            if date_obj.weekday() > 1:  # Add +1 to match your logic
                week += 1
            return f"{date_obj.year}-W{week:02d}"
        except Exception as e:
            logging.warning(f"[to_a_week] Failed to parse DD.MM.YYYY: {date_str} – {e}")
            return date_str

    # Unknown format, return as-is
    logging.warning(f"[to_a_week] Unknown format: {date_str}")
    return date_str


def process_valeo_nevers_pdf(pdf_bytes, file_name):

    data = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_pages_text = [page.get_text() for page in doc]
    full_text = "\n".join(all_pages_text)
    logging.info(f"[VALEO NEVERS] Extracted {len(full_text)} characters of text.")

    # Extract ForecastDate
    m_date = re.search(r"Created on (\d{2}\.\d{2}\.\d{4})", full_text)
    raw_forecast_date = m_date.group(1) if m_date else ""
    forecast_date = to_adjusted_iso_week(raw_forecast_date) if raw_forecast_date else ""
    logging.info(f"[VALEO NEVERS] ForecastDate raw: {raw_forecast_date}, week format: {forecast_date}")
    # Extract agreements
    sched_split = list(re.finditer(r"SCHEDULING AGREEMENT \d+", full_text))
    agreement_numbers = [re.search(r"(\d+)", m.group()).group(1) for m in sched_split]
    logging.info(f"[VALEO NEVERS] Found {len(agreement_numbers)} scheduling agreement sections: {agreement_numbers}")

    # Start pages for each agreement
    agreement_start_pages = []
    for agreement_no in agreement_numbers:
        for i, page_text in enumerate(all_pages_text):
            if f"SCHEDULING AGREEMENT {agreement_no}" in page_text:
                agreement_start_pages.append(i)
                break
    agreement_start_pages.append(len(all_pages_text))
    logging.info(f"[VALEO NEVERS] Agreement start pages: {agreement_start_pages}")

    for idx, agreement_no in enumerate(agreement_numbers):
        section_start_page = agreement_start_pages[idx]
        section_end_page = agreement_start_pages[idx + 1]
        section_pages = all_pages_text[section_start_page:section_end_page]
        logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Processing section from page {section_start_page} to {section_end_page}")

        current_material = ""
        avo_material = f"V{current_material}"
        current_product = None

        last_delivery_date = ""
        last_delivery_qty = None
        last_delivery_doc = ""

        # Try to extract product name and material from the block (first page in section)
        m_prod = re.search(
            r"Material\s+([A-Z0-9]+)\s+([^\n]+?)\s+Unit\s+of\s+Measure", section_pages[0])
        if m_prod:
            current_material = m_prod.group(1)
            current_product = m_prod.group(2).strip()
            avo_material = f"V{current_material}"
            logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Material: {current_material}, Product: {current_product}")

        # LAST DELIVERY block (all pages in section)
        last_delivery_search = re.search(
            r"LAST DELIVERY(.*?)SCHEDULING AGREEMENT", section_pages[0], re.DOTALL)
        if last_delivery_search:
            block_text = last_delivery_search.group(1)
            logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] LAST DELIVERY block raw text:\n{block_text}")
            lines = block_text.splitlines()
            logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Last delivery block found with {len(lines)} lines.")
            for i in range(len(lines)-2):
                date_line = lines[i].strip()
                qty_line = lines[i+1].strip()
                doc_line = lines[i+2].strip()

                # Look for a date line
                m_date = re.match(r"(\d{2}\.\d{2}\.\d{4})$", date_line)
                m_qty = re.match(r"^([\d\.,]+)$", qty_line)
                m_doc = re.match(r"^(\d+)$", doc_line)
                logging.debug(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Check triple: {date_line!r}, {qty_line!r}, {doc_line!r}")

                if m_date and m_qty and m_doc:
                    last_delivery_date = m_date.group(1)
                    try:
                        last_delivery_qty = int(float(m_qty.group(1).replace('.', '').replace(',', '.')))
                    except Exception as e:
                        logging.warning(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Error parsing last delivery qty: {e}")
                        last_delivery_qty = None
                    last_delivery_doc = m_doc.group(1)
                    logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] LAST DELIVERY triple matched: {last_delivery_date}, {last_delivery_qty}, {last_delivery_doc}")
                    break
        else:
            logging.warning(f"[VALEO NEVERS][AGREEMENT {agreement_no}] LAST DELIVERY block not found in section page.")
        if not last_delivery_search:
            logging.error(f"[VALEO NEVERS][AGREEMENT {agreement_no}] No LAST DELIVERY block found! Here is section page 0 text:\n{section_pages[0]}")

        # Block for planning (between UoM and CUMM.RECEIVED)
        planning_block = re.search(r"Unit of Measure[^\n]*\n(?P<planning>.+?)CUMM\. RECEIVED", "\n".join(section_pages), re.DOTALL)
        if not planning_block:
            logging.warning(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Planning block not found.")
            continue

        planning_lines = [l.strip() for l in planning_block.group('planning').splitlines() if l.strip()]
        logging.info(f"[VALEO NEVERS][AGREEMENT {agreement_no}] Found {len(planning_lines)} planning lines.")

        edi_map = {
            "PAST DUE": "Past Due",
            "FIRM AUTHORIZED SHIPPMENTS": "Firm",
            "PLANNED SHIPPMENTS": "Firm",
            "FORECAST": "Forecast",
        }

        last_status = None
        for idx_row, line in enumerate(planning_lines):
            logging.debug(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Line: {line}")

            # Status lines
            status_match = re.match(r"^(?:\*?)(PAST DUE|FIRM AUTHORIZED SHIPPMENTS|PLANNED SHIPPMENTS|FORECAST)$", line)
            # Data lines: date/qty/cumulated or just qty/cumulated (for PAST DUE, maybe no date)
            data_match = re.match(
                r"^([0-9]{2}\.[0-9]{2}\.[0-9]{4}D|[0-9]{2}\.[0-9]{4}W|[0-9]{2}\.[0-9]{2}\.[0-9]{4})?\s*([\d\.,]+)\s+([\d\.,]+)$", 
                line
            )

            if status_match:
                last_status = status_match.group(1)
                logging.debug(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Found status: {last_status}")
                continue
            elif data_match and last_status:
                # Assign last status as EDI status
                date_from = data_match.group(1) or ""
                qty = data_match.group(2)
                cum = data_match.group(3)
                edi_status = last_status
                last_status = None  # Reset until next status line

                edi_map = {
                    "PAST DUE": "Past Due",
                    "FIRM AUTHORIZED SHIPPMENTS": "Firm",
                    "PLANNED SHIPPMENTS": "Firm",
                    "FORECAST": "Forecast",
                }
                edi_status = edi_map.get(edi_status, edi_status)
                date_from = date_from.replace("D", "").replace("W", "") if date_from else ""

                try:
                    quantity = int(float(qty.replace(".", "").replace(",", ".")))
                except Exception as e:
                    logging.warning(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Error parsing quantity: {qty} - {e}")
                    quantity = 0
                try:
                    cumulated = int(float(cum.replace(".", "").replace(",", ".")))
                except Exception as e:
                    logging.warning(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Error parsing cumulated: {cum} - {e}")
                    cumulated = 0

                row = {
                    "Site": "Tunisia",
                    "ClientCode": "C00409",
                    "SchedulingAgreement": agreement_no,
                    "ForecastDate": forecast_date,
                    "ClientMaterialNo": current_material,
                    "AVOMaterialNo": avo_material,
                    "ProductName": current_product,
                    "EDIStatus": edi_status,
                    "DateFrom": to_week(date_from),
                    "DateUntil": date_from,
                    "Quantity": quantity,
                    "CumulatedQuantity": cumulated,
                    "LastDeliveryDate": to_week(last_delivery_date),
                    "LastDeliveredQuantity": last_delivery_qty,
                    "LastDeliveryNo": last_delivery_doc,
                }
                data.append(row)
                logging.info(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Row: {row}")
            else:
                logging.warning(f"[VALEO NEVERS][{agreement_no}][ROW {idx_row}] Line did not match any expected pattern: {line}")


    # Deduplicate rows by primary key if you need, similar to Campinas
    unique_rows = {}
    for row in data:
        pk = (
            row['Site'], row['ClientCode'], row['AVOMaterialNo'],
            row['DateFrom'], row['Quantity'], row['ForecastDate']
        )
        if pk not in unique_rows:
            unique_rows[pk] = row
    data = list(unique_rows.values())

    logging.info(f"[VALEO NEVERS] Extraction complete. Total records: {len(data)}")
    return data



SUFFIX_TOKENS = {"PL", "SP"}

def _safestr(v):
    import math
    if v is None: return ""
    if isinstance(v, float) and math.isnan(v): return ""
    return str(v).strip()

def _normalize_avo_ref(s: object, following_hint: object = None) -> str:
    base = _safestr(s)
    if not base: return ""
    parts = base.split()
    code = parts[0]
    suffix = None
    if len(parts) >= 2 and parts[1].upper() in SUFFIX_TOKENS:
        suffix = parts[1].upper()
    if not suffix and following_hint is not None:
        nxt = _safestr(following_hint)
        if nxt:
            tok = nxt.split()[0].upper()
            if tok in SUFFIX_TOKENS:
                suffix = tok
    return code + (suffix or "")

def _looks_like_pdf(b: bytes) -> bool:
    return b.lstrip()[:5] == b"%PDF-"

def _contains_facture(pdf_bytes: bytes, pages_to_check: int = 2) -> bool:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i in range(min(pages_to_check, len(pdf.pages))):
                txt = pdf.pages[i].extract_text() or ""
                if re.search(r"\bfacture\b", txt, re.IGNORECASE):
                    return True
    except Exception:
        pass
    return False

def parse_delivery_pdf_bytes(pdf_bytes: bytes, *, default_site: str = "Tunisia") -> pd.DataFrame:
    import io, re, pdfplumber, pandas as pd
    from dateutil.parser import parse as dtparse

    delivery_no = None
    doc_date_iso = None
    detected_site = None
    rows = []

    # helper: add a record
    def _add(ref_val: str, qty_val: int):
        rows.append({
            "Date": doc_date_iso,
            "DeliveryNo": str(delivery_no) if delivery_no else "UNKNOWN",
            "AVOMaterialNo": ref_val,
            "Quantity": int(qty_val),
            "Site": detected_site or default_site,
            "Status": "Dispatched",
        })

    # patterns
    header_no_pat   = re.compile(r"FACTURE\s*n[°o]\s*([A-Za-z0-9\-_/]+)", re.IGNORECASE)
    header_date_pat = re.compile(r"\bDate\s+(\d{1,2}/\d{1,2}/\d{4})\b", re.IGNORECASE)
    site_pat        = re.compile(r"\bAVOCARBON[^\n]*", re.IGNORECASE)
    total_row_pat   = re.compile(r"^\s*TOTAL\b", re.IGNORECASE)

    # fallback line parser:
    # ex line: "85030010 OUI V502.730 SP PPC 11TA ... 960 1,9672 0,3262 ..."
    # capture ref (V502.730) and the quantity (960) that appears BEFORE 2–4 decimal numbers
    line_pat = re.compile(
    r"^\s*\d{8}\s+(?:OUI|NON)\s+([A-Z0-9][A-Z0-9.\-]+)(?:\s+(PL|SP))?\s+.+?\s+(\d{1,9})\s+(?:\d+[.,]\d+\s+){2,4}\S+",
    re.IGNORECASE
)


    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        # -------- header (page 1, with fallbacks) --------
        if pdf.pages:
            p0_text = pdf.pages[0].extract_text() or ""
            m_no = header_no_pat.search(p0_text)
            if m_no:
                delivery_no = m_no.group(1).strip()
            m_date = header_date_pat.search(p0_text) or re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", p0_text)
            if m_date:
                doc_date_iso = dtparse(m_date.group(1), dayfirst=True).date().isoformat()
            m_site = site_pat.search(p0_text)
            if m_site:
                detected_site = "Tunisia"
        # if date still missing, try PDF metadata
        if not doc_date_iso:
            try:
                meta = pdf.metadata or {}
                meta_date = meta.get("CreationDate") or meta.get("ModDate")
                if meta_date:
                    doc_date_iso = dtparse(meta_date).date().isoformat()
            except Exception:
                pass

        # -------- attempt 1: table extraction --------
        tbl_settings = dict(
            vertical_strategy="lines",
            horizontal_strategy="lines",
            intersect_tolerance=5,
            snap_tolerance=3,
            join_tolerance=3,
            text_x_tolerance=2,
            text_y_tolerance=3,
            keep_blank_chars=False,
            edge_min_length=3,
        )
        header_ref = re.compile(r"\bREFERENCE\b|\bREFERENCE\s+ARTICLE\b|\bREF\b", re.IGNORECASE)
        header_qty = re.compile(r"\bQUANTITE\b|\bQTE\b|\bQTY\b", re.IGNORECASE)
        material_pat = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-_/]*[A-Za-z0-9]$")

        found = 0
        for page in pdf.pages:
            tables = []
            try:
                t = page.extract_table(tbl_settings)
                if t: tables.append(t)
            except Exception:
                pass
            try:
                ts = page.extract_tables(tbl_settings) or []
                tables.extend(ts)
            except Exception:
                pass

            for tbl in tables:
                if not tbl or len(tbl) < 2:
                    continue
                ref_idx = qty_idx = None
                # find headers in first rows
                for r in tbl[:3]:
                    if not r: continue
                    for i, c in enumerate(r):
                        cell = (c or "").strip()
                        if ref_idx is None and header_ref.search(cell or ""):
                            ref_idx = i
                        if qty_idx is None and header_qty.search(cell or ""):
                            qty_idx = i
                    if ref_idx is not None and qty_idx is not None:
                        break
                # fallback heuristic on row 0
                if ref_idx is None or qty_idx is None:
                    r0 = tbl[0]
                    for i, c in enumerate(r0):
                        low = (c or "").lower()
                        if ref_idx is None and "ref" in low and "prix" not in low:
                            ref_idx = i
                        if qty_idx is None and any(k in low for k in ("quant", "qty", "qte")):
                            qty_idx = i
                if ref_idx is None or qty_idx is None:
                    continue

                data_rows = tbl[1:]
                for r in data_rows:
                    if not r: continue
                    if total_row_pat.search(" ".join([(c or "").strip() for c in r if c])):
                        continue
                    raw_ref = (r[ref_idx] or "").strip() if ref_idx < len(r) else ""
                    following = (r[ref_idx + 1] if (ref_idx + 1) < len(r) else "")
                    ref_val = _normalize_avo_ref(raw_ref, following)
                    qty_val = (r[qty_idx] or "").strip() if qty_idx < len(r) else ""
                    if not ref_val or not qty_val: 
                        continue
                    if not material_pat.match(ref_val): 
                        continue
                    qtxt = qty_val.replace("\u00A0", "").replace(" ", "").replace(",", "")
                    if not re.match(r"^-?\d+(?:\.\d+)?$", qtxt):
                        continue
                    _add(ref_val, int(float(qtxt)))
                    found += 1

        # -------- attempt 2: text-line regex fallback --------
        if found == 0:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    if total_row_pat.search(line):
                        continue
                    m = line_pat.search(line)
                    if not m:
                        continue
                    ref_core = m.group(1).strip()
                    ref_sfx = (m.group(2) or "").strip().upper()
                    ref_val = ref_core + (ref_sfx if ref_sfx in SUFFIX_TOKENS else "")
                    qty_val = int(m.group(3))
                    _add(ref_val, qty_val)


    df = pd.DataFrame(rows, columns=["Date","DeliveryNo","AVOMaterialNo","Quantity","Site","Status"])
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def _clean_qty(v):
    """
    Convert various user-entered quantity formats to a safe int.
    Handles: strings with spaces/commas, floats, NaN, None.
    """
    if v is None:
        return 0
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        # If it's NaN, treat as 0
        return 0 if (v != v) else int(round(v))
    s = str(v).strip()
    if s == "" or s.lower() == "nan" or s.lower() == "none":
        return 0
    # remove common thousand separators and non-digit except minus sign
    s = s.replace(",", "").replace(" ", "").replace("\u00A0", "")  # remove normal & non-breaking spaces
    m = re.match(r"^-?\d+(\.\d+)?$", s)
    if m:
        return int(float(s))
    # last resort: strip everything not a digit or minus
    s2 = re.sub(r"[^\d-]", "", s)
    return int(s2) if s2 not in ("", "-",) else 0

def _norm_status(s):
    if s is None:
        return ""
    s = str(s).strip()
    if s.lower() == "sent":
        return "Dispatched"
    # unify common variants
    if s.lower().replace(" ", "") in ("intransit","in-transit"):
        return "InTransit"
    if s.lower() == "dispatched":
        return "Dispatched"
    if s.lower() == "delivered":
        return "Delivered"
    return s  # fallback unchanged

def process_delivery_invoice_pdf(pdf_bytes: bytes, *, default_site: str = "Tunisia") -> list[dict]:
    """
    Parse a delivery invoice PDF (recognized via 'FACTURE') and return
    records ready for DB: Site, AVOMaterialNo, DeliveryNo, Quantity, Date, Status.
    - Merges PL/SP suffixes into AVOMaterialNo (e.g., 'V504.243 PL' -> 'V504.243PL')
    - Pre-aggregates duplicates (sum Quantity) by (Site, AVOMaterialNo, DeliveryNo, Date, Status)
    """
    df = parse_delivery_pdf_bytes(pdf_bytes, default_site=default_site[:20])  # parser sets Status='Dispatched'
    # Defensive normalization
    for col in ["Site","AVOMaterialNo","DeliveryNo","Date","Status"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_safestr)

    # Merge PL/SP into AVOMaterialNo (idempotent)
    df["AVOMaterialNo"] = df.apply(lambda r: _normalize_avo_ref(r.get("AVOMaterialNo"), None), axis=1)

    # Clean qty and status
    if "Quantity" not in df.columns:
        df["Quantity"] = 0
    df["Quantity"] = df["Quantity"].apply(_clean_qty).astype(int)
    df["Status"] = df["Status"].apply(_norm_status)

    # Pre-aggregate duplicates
    key_cols = ["Site","AVOMaterialNo","DeliveryNo","Date","Status"]
    if not df.empty:
        df = (df.groupby(key_cols, as_index=False)["Quantity"].sum())
        df = df[df["Quantity"] != 0].reset_index(drop=True)

    return df.to_dict(orient="records")



def insert_deliverydetails(df):
    """
    psycopg2 version — uses get_pg_connection() and merges/sums duplicates.
    Expected df columns: Site, AVOMaterialNo, DeliveryNo, Date, Status, Quantity
    """
    import psycopg2

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:

                def _sum_key(site, avo_mat, delivery_no, date, status):
                    cur.execute("""
                        SELECT COALESCE(SUM("Quantity"), 0)
                        FROM public."DeliveryDetails"
                        WHERE "Site"=%s AND "AVOMaterialNo"=%s AND "DeliveryNo"=%s AND "Date"=%s AND "Status"=%s
                    """, (site, avo_mat, delivery_no, date, status))
                    return int(cur.fetchone()[0] or 0)

                def _delete_key(site, avo_mat, delivery_no, date, status):
                    cur.execute("""
                        DELETE FROM public."DeliveryDetails"
                        WHERE "Site"=%s AND "AVOMaterialNo"=%s AND "DeliveryNo"=%s AND "Date"=%s AND "Status"=%s
                    """, (site, avo_mat, delivery_no, date, status))

                def _insert_row(site, avo_mat, delivery_no, date, status, qty):
                    cur.execute("""
                        INSERT INTO public."DeliveryDetails"
                            ("Site","AVOMaterialNo","DeliveryNo","Quantity","Date","Status")
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (site, avo_mat, delivery_no, int(qty), date, status))

                def _merge_sum_key(site, avo_mat, delivery_no, date, status, delta_qty):
                    existing = _sum_key(site, avo_mat, delivery_no, date, status)
                    total = existing + int(delta_qty)
                    _delete_key(site, avo_mat, delivery_no, date, status)
                    if total > 0:
                        _insert_row(site, avo_mat, delivery_no, date, status, total)

                def _sum_intransit(site, avo_mat):
                    cur.execute("""
                        SELECT COALESCE(SUM("Quantity"), 0)
                        FROM public."DeliveryDetails"
                        WHERE "Site"=%s AND "AVOMaterialNo"=%s AND "Status"='InTransit'
                    """, (site, avo_mat))
                    return int(cur.fetchone()[0] or 0)

                def _delete_all_intransit(site, avo_mat):
                    cur.execute("""
                        DELETE FROM public."DeliveryDetails"
                        WHERE "Site"=%s AND "AVOMaterialNo"=%s AND "Status"='InTransit'
                    """, (site, avo_mat))

                def _rewrite_intransit(site, avo_mat, delivery_no, date, qty):
                    _delete_all_intransit(site, avo_mat)
                    if int(qty) > 0:
                        _insert_row(site, avo_mat, delivery_no, date, "InTransit", int(qty))

                # iterate normalized df
                for _, row in df.iterrows():
                    site        = _safestr(row.get("Site"))[:20]
                    avo_mat     = _safestr(row.get("AVOMaterialNo"))[:30]
                    delivery_no = _safestr(row.get("DeliveryNo"))[:30]
                    date        = _safestr(row.get("Date"))[:20]
                    qty         = _clean_qty(row.get("Quantity"))
                    status      = _norm_status(row.get("Status"))

                    if not (site and avo_mat and delivery_no and date and status):
                        continue

                    if status == "Dispatched":
                        curr_it = _sum_intransit(site, avo_mat)
                        _rewrite_intransit(site, avo_mat, delivery_no, date, curr_it + qty)
                        _merge_sum_key(site, avo_mat, delivery_no, date, "Dispatched", qty)

                    elif status == "Delivered":
                        curr_it = _sum_intransit(site, avo_mat)
                        new_it = max(0, curr_it - qty)
                        _rewrite_intransit(site, avo_mat, delivery_no, date, new_it)
                        _merge_sum_key(site, avo_mat, delivery_no, date, "Delivered", qty)

                    elif status == "InTransit":
                        curr_it = _sum_intransit(site, avo_mat)
                        _rewrite_intransit(site, avo_mat, delivery_no, date, curr_it + qty)

                    else:
                        _merge_sum_key(site, avo_mat, delivery_no, date, status, qty)

    finally:
        try:
            conn.close()
        except Exception:
            pass




OCR_SERVICE_URL = "https://ocr-files-cdh9dbaqf2cufdgs.francecentral-01.azurewebsites.net/process-base64"



# ---------------------------------------------------------
# >>> NEW HELPER FUNCTIONS FOR OCR & PARSING <<<
# ---------------------------------------------------------

month_map_pl = {
    'sty': '01', 'lut': '02', 'mar': '03', 'kwi': '04', 'maj': '05', 'cze': '06',
    'lip': '07', 'sie': '08', 'wrz': '09', 'paź': '10', 'paz': '10', 'lis': '11', 'gru': '12'
}

def parse_polish_date_ocr(date_str):
    """Converts '2.lip.2025' to '2025-07-02'"""
    try:
        clean_str = date_str.strip(' .')
        parts = clean_str.split('.')
        if len(parts) != 3:
            return date_str 
        day, month_txt, year = parts
        month_digit = month_map_pl.get(month_txt.lower(), '01')
        return f"{year}-{month_digit}-{day.zfill(2)}"
    except Exception:
        return date_str

def extract_delivery_data_from_text(ocr_text):
    """
    Parses the raw text from OCR based on the specific Delivery Note logic.
    Adds 'POL' suffix to AVOMaterialNo if 'Poland' is detected in text.
    """
    # Normalize text
    text = " ".join(ocr_text.split())

    # --- 1. Context Check: Detect Poland ---
    is_poland = "poland" in text.lower()

    # --- 2. DeliveryNo ---
    delivery_no = "UNKNOWN"
    match_no = re.search(r'Delivery Note\s+(.*?)\s+Loading place', text, re.IGNORECASE)
    if match_no:
        delivery_no = match_no.group(1).strip()

    # --- 3. DeliveryDate ---
    delivery_date = datetime.now().strftime("%Y-%m-%d") 
    match_date = re.search(r'Delivery Date\s+([\d]{1,2}\.[a-zA-Z]{3}\.[\d]{4})', text, re.IGNORECASE)
    if match_date:
        raw_date = match_date.group(1).strip()
        delivery_date = parse_polish_date_ocr(raw_date)

    # --- 4. Quantities ---
    quantities = []
    match_qty = re.search(r'Qty pcs\s+(.*?)\s+Remarks', text, re.IGNORECASE)
    if match_qty:
        qty_string = match_qty.group(1)
        quantities = [int(q) for q in qty_string.split() if q.isdigit()]

    # --- 5. AVO Materials ---
    avo_materials = []
    match_31 = re.search(r'\s31\s+(.*)', text)
    if match_31:
        after_31_text = match_31.group(1)
        tokens = after_31_text.split()
        # Capture tokens starting with V
        potential_avos = [t for t in tokens if t.startswith('V')]
        limit = len(quantities)
        avo_materials = potential_avos[:limit]

    # --- 6. Construct Rows for DataFrame ---
    results = []
    for i in range(len(quantities)):
        mat_no = avo_materials[i] if i < len(avo_materials) else "UNKNOWN"
        
        # >>> NEW LOGIC: Append POL if applicable <<<
        if is_poland and mat_no != "UNKNOWN":
            mat_no = f"{mat_no}PL"

        results.append({
            "Site": "Tunisia",
            "AVOMaterialNo": mat_no,
            "DeliveryNo": delivery_no,
            "Date": delivery_date,
            "Status": "Delivered",
            "Quantity": quantities[i]
        })
        
    return results



def perform_ocr_on_base64(file_base64, filename):
    """
    Sends the Base64 string to the local OCR service and returns the raw text.
    """
    payload = {
        "file_name": filename,
        "file_content_base64": file_base64,
        "max_pages": 5 
    }
    try:
        response = requests.post(OCR_SERVICE_URL, json=payload, timeout=60)
        if response.status_code == 200:
            return response.json().get("text", "")
        else:
            logging.error(f"OCR Service failed: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Failed to connect to OCR service: {e}")
        return None





@app.route("/process-TunisiaSite", methods=['POST'])
def process_file_endpoint():
    data = request.get_json()
    required_keys = ['file_name', 'file_content_base64']
    if not data or not all(k in data for k in required_keys):
        missing_keys = [k for k in required_keys if k not in data]
        return jsonify({"error": f"Missing keys in request body: {', '.join(missing_keys)}"}), 400

    file_name = data['file_name']
    file_content_base64 = data['file_content_base64']
    file_type = data.get('file_type', None)
    is_pdf = (file_type == "pdf") or file_name.lower().endswith('.pdf')

    try:
        file_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

    extracted_records = []
    company = None
    header = None

    # ---------- PDF path ----------
    if is_pdf and _looks_like_pdf(file_bytes):
        
        # Check if it is scanned
        scan_resp, scan_status = is_scanned_pdf(file_bytes, file_name)

        # >>> MODIFIED LOGIC: If scanned, try OCR instead of erroring <<<
        if scan_resp is not None:
            logging.info(f"File {file_name} detected as scanned. Attempting OCR...")
            
            # 1. Call OCR Service
            ocr_text = perform_ocr_on_base64(file_content_base64, file_name)
            
            if ocr_text:
                # 2. Parse the OCR text using your specific logic
                try:
                    parsed_data = extract_delivery_data_from_text(ocr_text)
                    
                    if parsed_data:
                        # 3. Save to DeliveryDetails
                        df_ocr = pd.DataFrame(parsed_data)
                        insert_deliverydetails(df_ocr)
                        
                        return jsonify({
                            "message": "Scanned PDF processed via OCR.",
                            "file_processed": file_name,
                            "company_detected": "Valeo/Nidec (OCR)",
                            "records_processed": len(parsed_data),
                            "records_inserted": len(parsed_data),
                            "records_failed": 0,
                            "errors": []
                        }), 200
                    else:
                        return jsonify({"error": "OCR succeeded but no delivery data extracted matches criteria."}), 422
                        
                except Exception as e:
                    logging.error(f"Error parsing OCR text: {e}")
                    return jsonify({"error": f"Error parsing OCR result: {e}"}), 500
            else:
                 # OCR Failed: Return the original 'is_scanned' error response
                 logging.warning("OCR service returned no text or failed.")
                 return scan_resp, scan_status

        # Not scanned: try FACTURE first (Delivery invoice)
        try:
            if _contains_facture(file_bytes):
                # Site is Tunisia for this endpoint; keep <= 20 chars for schema
                extracted_records = process_delivery_invoice_pdf(file_bytes, default_site="Tunisia")
                company = "Delivery Invoice (FACTURE)"
            else:
                # Fallback to your vendor-specific detectors
                pdf_format = detect_pdf_format(file_bytes)
                if not pdf_format:
                    return jsonify({"error": "Unknown or unsupported PDF format."}), 400

                if pdf_format == "valeo_campinas":
                    extracted_records = process_valeo_campinas_pdf(file_bytes, file_name)
                    company = "Valeo VWS Campinas"

                elif pdf_format == "valeo_nevers":
                    extracted_records = process_valeo_nevers_pdf(file_bytes, file_name)
                    company = "Valeo CIE Nevers"

                elif pdf_format == "pierburg":
                    extracted_records = process_pierburg_pdf(file_bytes, file_name)
                    company = "Pierburg"

                elif pdf_format == "nidec":
                    extracted_records = process_nidec_pdf(file_bytes, file_name)
                    company = "Nidec"

                else:
                    return jsonify({"error": "Unrecognized PDF format."}), 400

        except Exception as e:
            logging.exception("PDF processing error")
            return jsonify({"error": f"PDF processing error: {e}"}), 400

    # ---------- CSV path ----------
    else:
        try:
            logging.warning(f"DEBUG: first 100 b64 chars: {file_content_base64[:100]}")
            csv_text = decode_and_clean_csv(file_content_base64)
            csv_io = io.StringIO(csv_text)
            rows = list(csv.reader(csv_io, delimiter=';'))
            header = [col.strip() for col in rows[0]]
            rows_cleaned = []
            for row in rows:
                cleaned_row = [cell.strip() for cell in row]
                while cleaned_row and cleaned_row[-1] == '':
                    cleaned_row.pop()
                rows_cleaned.append(cleaned_row)
            rows = rows_cleaned
            rows[0] = header
        except Exception as e:
            logging.error(f"Failed to decode and clean Base64 string for file {file_name}: {e}")
            return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

        company, header = detect_company_and_prepare(rows)
        if not company:
            return jsonify({"error": "Unknown or unsupported CSV format."}), 400
        if company == "Valeo":
            extracted_records = process_valeo_rows(rows, header)
        elif company == "Inteva":
            extracted_records = process_inteva_rows(rows, header)
        elif company == "Nidec":
            extracted_records = process_nidec_rows(rows, header)
        else:
            return jsonify({"error": "Unrecognized company type."}), 400

    # ---------- Save to DB ----------
    try:
        # anything that contains "FACTURE" goes to DeliveryDetails
        if company and "FACTURE" in str(company):
            df = pd.DataFrame(extracted_records)
            if df.empty:
                return jsonify({"error": "No delivery lines parsed"}), 422

            # ensure required columns exist
            for col in ["Site","AVOMaterialNo","DeliveryNo","Date","Status","Quantity"]:
                if col not in df.columns:
                    df[col] = ""

            # normalize and respect varchar lengths
            df["Site"] = df["Site"].map(lambda s: _safestr(s)[:20])
            df["AVOMaterialNo"] = df.apply(lambda r: _normalize_avo_ref(r.get("AVOMaterialNo"), None), axis=1)
            df["AVOMaterialNo"] = df["AVOMaterialNo"].map(lambda s: _safestr(s)[:30])
            df["DeliveryNo"] = df["DeliveryNo"].map(lambda s: _safestr(s)[:30])
            df["Date"] = df["Date"].map(_safestr)
            df["Status"] = df["Status"].map(_norm_status)
            df["Quantity"] = df["Quantity"].apply(_clean_qty).astype(int)

            # pre-aggregate duplicates
            key_cols = ["Site","AVOMaterialNo","DeliveryNo","Date","Status"]
            source_lines = len(df)
            df = df.groupby(key_cols, as_index=False)["Quantity"].sum()
            df = df[df["Quantity"] != 0].reset_index(drop=True)

            # insert into DeliveryDetails (psycopg2 version)
            insert_deliverydetails(df)

            return jsonify({
                "message": "Delivery invoice processed.",
                "file_processed": file_name,
                "company_detected": company,
                "records_processed": int(source_lines),
                "records_inserted": int(len(df)),
                "records_failed": 0,
                "errors": []
            }), 200

        # non-FACTURE flows (Vendor PDFs / CSVs) keep using the EDI saver
        success_count, error_details = save_to_postgres_with_conflict_reporting(extracted_records)
        logging.warning(
        f"DEBUG: DB result for {file_name}: inserted={success_count}, errors={len(error_details)}"
    )
        return jsonify({
            "message": "Processing completed.",
            "file_processed": file_name,
            "company_detected": company,
            "records_processed": len(extracted_records),
            "records_inserted": success_count,
            "records_failed": len(error_details),
            "errors": error_details
        }), (200 if success_count > 0 else 400)

    except Exception as e:
        logging.exception("Database error")
        return jsonify({"error": f"Database error: {e}"}), 400










def is_scanned_pdf(file_bytes: bytes, file_name: str) -> tuple:
    """
    Analyse un PDF pour détecter s'il est scanné (aucun texte extractible).
    Retourne un tuple (json_response, http_status) pour envoi direct.
    """
    try:
        with fitz.open(stream=file_bytes, filetype="pdf") as doc:
            for page in doc:
                if page.get_text("text").strip():
                    # PDF non scanné
                    return None, None  # On renvoie None pour signaler que ce n'est pas un scan

        # Si on est ici, c’est un PDF scanné
        file_ext = os.path.splitext(file_name)[1] or ".pdf"
        suggested_name = f"scanned_pdf_{os.path.splitext(file_name)[0]}{file_ext}".lower()

        payload = {
            "file_processed": file_name,
            "client_code": "SCANNED",
            "company_detected": "Unknown",
            "forecast_week": None,
            "records_detected": 0,
            "suggested_filename": suggested_name,
            "reason": "pdf_scanned_no_text",
            "file_type": "pdf",
            "is_scanned": True
        }
        return jsonify(payload), 200

    except Exception as e:
        logging.warning(f"is_scanned_pdf failed to process {file_name}: {e}")
        file_ext = os.path.splitext(file_name)[1] or ".pdf"
        payload = {
            "file_processed": file_name,
            "client_code": "UNRECOGNIZED",
            "company_detected": "Unknown",
            "forecast_week": None,
            "records_detected": 0,
            "suggested_filename": f"unknown_pdf_error{file_ext}",
            "reason": "pdf_read_error",
            "file_type": "pdf",
            "is_scanned": None
        }
        return jsonify(payload), 200






def build_unknown_response(
    file_name: str,
    file_ext: str,
    reason: str,
    file_type: str,
    is_scanned: bool = None,
) -> tuple:
    """
    Create a consistent JSON for unrecognized inputs that callers can route.
    Returns (json, 200).
    """
    safe_reason = reason.replace(" ", "_").lower()
    suggested_name = f"unknown_edi_{safe_reason}{file_ext}".lower()
    payload = {
        "file_processed": file_name,
        "client_code": "UNRECOGNIZED",
        "company_detected": "Unknown",
        "forecast_week": None,
        "records_detected": 0,
        "suggested_filename": suggested_name,
        "reason": reason,
        "file_type": file_type,
        "file_recognition": False  # Always false for unknown files
    }
    if is_scanned is not None:
        payload["is_scanned"] = is_scanned
    return jsonify(payload), 200


@app.route("/detect-client-info", methods=["POST"])
def detect_client_info():
    data = request.get_json()
    required_keys = ['file_name', 'file_content_base64']
    if not data or not all(k in data for k in required_keys):
        missing_keys = [k for k in required_keys if k not in data]
        return jsonify({"error": f"Missing keys in request body: {', '.join(missing_keys)}"}), 400

    file_name = data['file_name']
    file_content_base64 = data['file_content_base64']
    file_ext = os.path.splitext(file_name)[1] or ".dat"
    is_pdf = file_name.lower().endswith('.pdf')

    try:
        file_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

    extracted_records = []
    client_code = None
    company_name = None
    scanned_flag = None  # set only for PDFs

    # ---------- PDF HANDLING ----------
    if is_pdf:
        scan_resp, scan_status = is_scanned_pdf(file_bytes, file_name)
        if scan_resp is not None:
            return scan_resp, scan_status

        scanned_flag = False

        # ✅ NEW: detect delivery invoice by the word "FACTURE"
        try:
            if _contains_facture(file_bytes):
                company_name = "Delivery Invoice (FACTURE)"

                # Optional: parse to get DeliveryNo/Date for a nicer suggested filename
                try:
                    recs = process_delivery_invoice_pdf(file_bytes, default_site="Tunisia")
                except Exception:
                    recs = []

                dno = str((recs[0].get("DeliveryNo") if recs else None) or "unknown")
                ddate = str((recs[0].get("Date") if recs else None) or "unknown")
                suggested_name = f"delivery_invoice_{dno}_{ddate}{file_ext}".lower()

                # Early return: this is a delivery invoice, not EDI
                payload = {
                    "file_processed": file_name,
                    "client_code": "UNRECOGNIZED",     # invoices don’t carry EDI client code
                    "company_detected": company_name,
                    "forecast_week": None,
                    "records_detected": len(recs),
                    "suggested_filename": suggested_name,
                    "file_type": "pdf",
                    "file_recognition": True,
                    "document_kind": "delivery_invoice",
                    "is_scanned": scanned_flag
                }
                return jsonify(payload), 200
        except Exception as e:
            # If something goes wrong in the detection, fall through to the legacy flow
            app.logger.warning(f"FACTURE detection failed for {file_name}: {e}")

        # (legacy vendor PDF flow kept as-is)
        pdf_format = detect_pdf_format(file_bytes)
        if not pdf_format:
            return build_unknown_response(
                file_name=file_name,
                file_ext=file_ext,
                reason="unrecognized_pdf_format",
                file_type="pdf",
                is_scanned=scanned_flag,
            )

        try:
            if pdf_format == "valeo_campinas":
                extracted_records = process_valeo_campinas_pdf(file_bytes, file_name)
            elif pdf_format == "valeo_nevers":
                extracted_records = process_valeo_nevers_pdf(file_bytes, file_name)
            elif pdf_format == "pierburg":
                extracted_records = process_pierburg_pdf(file_bytes, file_name)
            elif pdf_format == "nidec":
                extracted_records = process_nidec_pdf(file_bytes, file_name)
            else:
                return build_unknown_response(
                    file_name=file_name,
                    file_ext=file_ext,
                    reason="unrecognized_pdf_format",
                    file_type="pdf",
                    is_scanned=scanned_flag,
                )
        except Exception as e:
            logging.warning(f"PDF parsing error for {file_name}: {e}")
            return build_unknown_response(
                file_name=file_name,
                file_ext=file_ext,
                reason="pdf_parse_error",
                file_type="pdf",
                is_scanned=scanned_flag,
            )


    # ---------- CSV HANDLING ----------
    else:
        try:
            csv_text = decode_and_clean_csv(file_content_base64)
            csv_io = io.StringIO(csv_text)
            rows = list(csv.reader(csv_io, delimiter=';'))

            if not rows or not rows[0]:
                return build_unknown_response(
                    file_name=file_name,
                    file_ext=file_ext,
                    reason="empty_or_invalid_csv",
                    file_type="csv",
                )

            header = [col.strip() for col in rows[0]]
            rows_cleaned = [[cell.strip() for cell in row] for row in rows]
            rows_cleaned[0] = header
            rows = rows_cleaned
        except Exception as e:
            return build_unknown_response(
                file_name=file_name,
                file_ext=file_ext,
                reason="csv_decoding_failed",
                file_type="csv",
            )

        company, header = detect_company_and_prepare(rows)
        if not company:
            return build_unknown_response(
                file_name=file_name,
                file_ext=file_ext,
                reason="unrecognized_csv_format",
                file_type="csv",
            )

        try:
            if company == "Valeo":
                extracted_records = process_valeo_rows(rows, header)
            elif company == "Inteva":
                extracted_records = process_inteva_rows(rows, header)
            elif company == "Nidec":
                extracted_records = process_nidec_rows(rows, header)
            else:
                return build_unknown_response(
                    file_name=file_name,
                    file_ext=file_ext,
                    reason="unrecognized_company_type",
                    file_type="csv",
                )
        except Exception as e:
            logging.warning(f"CSV parsing error for {file_name}: {e}")
            return build_unknown_response(
                file_name=file_name,
                file_ext=file_ext,
                reason="csv_parse_error",
                file_type="csv",
            )

    # ---------- Produce recognized response ----------
    if extracted_records:
        client_code = extracted_records[0].get("ClientCode", None)

    code_to_company = {
        "C00409": "Valeo Nevers",
        "C00072": "Valeo Brasil",
        "C00285": "Pierburg",
        "C00260": "Nidec Inde",
        "C00113": "Nidec DCK",
        "C00126": "Nidec Pologne",
        "C00050": "Nidec ESP",
        "C00241": "Inteva GAD",
        "C00410": "Inteva Esson",
        "C00250": "Valeo Poland",
        "C00303": "Valeo Mexique",
        "C00125": "Valeo Madrid",
        "C00132": "Valeo Betigheim"
    }
    company_name = code_to_company.get(client_code, "Unknown") if client_code else "Unknown"
    forecast_date = extracted_records[0].get("ForecastDate") if extracted_records else None
    suggested_name = f"{company_name.replace(' ', '_')}_edi_{(forecast_date or 'unknown_week')}{file_ext}".lower()

    payload = {
        "file_processed": file_name,
        "client_code": client_code if client_code else "UNRECOGNIZED",
        "company_detected": company_name,
        "forecast_week": forecast_date,
        "records_detected": len(extracted_records),
        "suggested_filename": suggested_name,
        "file_type": "pdf" if is_pdf else "csv",
        "file_recognition": True if extracted_records else False  # true only if recognized & has records
    }
    if scanned_flag is not None:
        payload["is_scanned"] = scanned_flag
    if not extracted_records:
        payload["reason"] = "recognized_but_no_records"

    return jsonify(payload), 200

######################################################################################################################################


def process_valeo_de_csv_rows(rows, header):
    processed = []

    plant_to_client = {
        "CZ22": "100442", 
        "FUEN": "100541", 
        "KJ01": "100506", 
        "CA02": "100573",
        "ET01": "100523"
    }

    valeo_de_product_map = {
        "190313": "1023093",
        "191663": "1023645",
        "187144": "1026188",
        "194470": "1026258",
        "202066": "1026540",
        "214188": "1026629",
        "471550":  "1026325",
        "478537":  "1026365",
        "470737":  "1026384"

    }

    idx = {col: header.index(col) for col in header}

    for row in rows[1:]:
        try:
            if 'Customer_No' in idx and not row[idx['Customer_No']].isnumeric():
                continue

            plant = row[idx['Plant_No']].strip()
            client_code = plant_to_client.get(plant)
            if not client_code:
                logging.warning(f"[Valeo DE CSV] Skipping row — Unknown plant: {plant}")
                continue

            material_code = row[idx['Material_No_Customer']].strip()
            AVOmaterial_code = valeo_de_product_map.get(material_code.lstrip("0"))

            if not AVOmaterial_code:
                logging.warning(f"[Valeo DE CSV] Skipping row — No AVO mapping for material: {material_code}")
                continue

            delivery_date = row[idx['Delivery_Date']].strip()
            date_str = row[idx['Date']].strip()

            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                date_obj = datetime.strptime(date_str, "%d.%m.%Y")

            week_num = date_obj.isocalendar()[1]
            if date_obj.weekday() > 1:
                week_num += 1
            forecast_date = f"{date_obj.year}-W{week_num:02d}"

            processed.append({
                "Site": "Germany",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": to_forecast_week(delivery_date),
                "DateUntil": delivery_date,
                "Quantity": int(row[idx['Despatch_Qty']].strip() or 0),
                "ForecastDate": to_forecast_week(date_str),
                "LastDeliveryDate": to_forecast_week(row[idx['Last_Delivery_Note_Date']].strip()),
                "LastDeliveredQuantity": int(row[idx['Last_Delivery_Quantity']].strip() or 0),
                "CumulatedQuantity": int(row[idx['Cum_Quantity']].strip() or 0),
                "EDIStatus": {
                    "p": "Forecast",
                    "P": "Forecast",
                    "f": "Firm",
                    "F": "Firm"
                }.get(row[idx['Commitment_Level']].strip(), row[idx['Commitment_Level']].strip()),
                "ProductName": row[idx['Description']].strip(),
                "LastDeliveryNo": row[idx['Last_Delivery_Note']].strip()
            })
        except Exception as e:
            logging.error(f"[Valeo DE CSV] Row processing error: {e}")

    logging.warning(f"[Valeo DE CSV] Processed {len(processed)} records from {len(rows)-1} rows")
    return processed


def process_nidec_de_csv_rows(rows, header):
    plant_to_client = {
        "ZI01": "100420",  # Nidec Poland - Germany site
    }

    material_map = {
        "471-695-99-99": "1022201",
        "503-660-99-99": "1027700"
    }

    processed = []
    idx = {col: header.index(col) for col in header}

    required_fields = ['Plant', 'CallOffDate', 'Material', 'DateFrom', 'DateUntil', 
                       'DespatchQty', 'LastDeliveryDate', 'LastDeliveryQuantity', 
                       'CumQuantity', 'Status', 'LastDeliveryNo']
    for f in required_fields:
        if f not in idx:
            logging.error(f"Column missing in Nidec DE CSV: {f}")
            return []

    for i, row in enumerate(rows[1:], 1):
        try:
            if len(row) < len(header):
                row += [""] * (len(header) - len(row))

            plant = row[idx['Plant']].strip()
            if plant not in plant_to_client:
                logging.warning(f"[Nidec DE CSV] SKIP row {i}: Unknown plant '{plant}'")
                continue

            client_code = plant_to_client[plant]
            call_off_date_str = row[idx['CallOffDate']].strip()

            if not call_off_date_str:
                logging.warning(f"[Nidec DE CSV] SKIP row {i}: Empty CallOffDate")
                continue

            try:
                date_obj = datetime.strptime(call_off_date_str, "%Y-%m-%d")
            except ValueError:
                try:
                    date_obj = datetime.strptime(call_off_date_str, "%d.%m.%Y")
                except ValueError:
                    logging.warning(f"[Nidec DE CSV] SKIP row {i}: Unparsable CallOffDate '{call_off_date_str}'")
                    continue

            week_num = date_obj.isocalendar()[1]
            if date_obj.weekday() > 1:
                week_num += 1
            forecast_date = f"{date_obj.year}-W{week_num:02d}"

            material_code = row[idx['Material']].strip()
            avo_material_code = material_map.get(material_code)

            if not avo_material_code:
                logging.warning(f"[Nidec DE CSV] SKIP row {i}: No AVO mapping for material '{material_code}'")
                continue

            processed.append({
                "Site": "Germany",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": avo_material_code,
                "DateFrom": to_forecast_week(row[idx['DateFrom']].strip()),
                "DateUntil": row[idx['DateUntil']].strip(),
                "Quantity": parse_euro_number(row[idx['DespatchQty']].strip()),
                "ForecastDate": forecast_date,
                "LastDeliveryDate": to_forecast_week(row[idx['LastDeliveryDate']].strip()),
                "LastDeliveredQuantity": parse_euro_number(row[idx['LastDeliveryQuantity']].strip()),
                "CumulatedQuantity": parse_euro_number(row[idx['CumQuantity']].strip()),
                "EDIStatus": row[idx['Status']].strip(),
                "ProductName": None,
                "LastDeliveryNo": row[idx['LastDeliveryNo']].strip()
            })

        except Exception as e:
            logging.error(f"[Nidec DE CSV] Row {i} processing error: {e}")

    logging.warning(f"[Nidec DE CSV] Processed {len(processed)} records from {len(rows)-1} rows")
    return processed


def process_bosch_pdf(file_bytes, file_name):
    text = parse_pdf(io.BytesIO(file_bytes))
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Standortcode --> ClientCode
    client_code = "UNKNOWN"
    m = re.search(r"Standortcode\s*\(Kunde\):\s*(\w+)", text)
    if m:
        code = m.group(1)
        site_code_to_client = {
            "2570": "100409",
            "5060": "100410",
            "908A": "100327",
            "526W": "100296"
        }
        client_code = site_code_to_client.get(code, "UNKNOWN")

    # Material (Client)
    material = re.search(r"Material:\s*([0-9A-Za-z\-]+)", text)
    material = material.group(1) if material else ""

    # ProductName
    product_name = re.search(r"Materialbeschreibung\s*\(Kunde\):\s*([^\n]+)", text)
    product_name = product_name.group(1).strip() if product_name else ""

    # LastDeliveryNo
    last_delivery_no = re.search(r"Lieferscheinnummer:?\s*(\S+)", text)
    last_delivery_no = last_delivery_no.group(1) if last_delivery_no else ""

    # LastDeliveryDate and Qty
    last_delivery_date = ""
    last_delivered_qty = 0
    m = re.search(r"Lieferscheinn(?:ummer|r).*?Datum:\s*(\d{2}\.\d{2}\.\d{2,4}).*?Menge:\s*([\d.]+)", text, re.DOTALL)
    if m:
        last_delivery_date = m.group(1)
        last_delivered_qty = int(m.group(2).replace(".", ""))

    # ForecastDate from akt. Lieferabrufnummer
    forecast_date = ""
    m = re.search(r"akt\. Lieferabrufnummer:.*?Datum:\s*(\d{2}\.\d{2}\.\d{2,4})", text)
    if m:
        forecast_date = m.group(1)

    # Material Mappings
    bosch_material_map = {
        "1027599": "1582875601",
        "1022031": "1582884102",
        "1026644": "1394320515",
        "1021731": "1394320230",
        "1394320230":  "1021731",
        "1394320228":  "1026021"

    }
    reverse_map = {v: k for k, v in bosch_material_map.items()}

    client_material_no = "UNKNOWN"
    avo_material_no = "UNKNOWN"
    if material in bosch_material_map:
        avo_material_no = material
        client_material_no = bosch_material_map[material]
    elif material in reverse_map:
        avo_material_no = reverse_map[material]
        client_material_no = material

    # Line Patterns
    pattern_with_times = re.compile(r"(\d{2}\.\d{2}\.\d{2})\s+(\d{2}:\d{2})\s+(\d{2}\.\d{2}\.\d{2})\s+(\d{2}:\d{2})\s+([\d.,]+)\s+([\d.,]+)\s+(\w+)")
    pattern_no_times = re.compile(r"(\d{2}\.\d{2}\.\d{2})\s+(\d{2}\.\d{2}\.\d{2})\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)?\s*(\w+)")
    pattern_short = re.compile(r"(\d{2}\.\d{2}\.\d{2})\s+([\d.,]+)\s+([\d.,]+)\s+(\w+)")

    def convert_euro(val):
        if not val: return 0
        val = val.replace(".", "").replace(",", ".")
        return int(float(val))

    edi_status_map = {
        "fix": "Firm",
        "fertigung": "Firm",
        "material": "Firm",
        "vorschau": "Forecast"
    }

    records = []

    for line in lines:
        match = (
            pattern_with_times.match(line) or
            pattern_no_times.match(line) or
            pattern_short.match(line)
        )
        if not match:
            continue

        if match.re is pattern_with_times:
            _, _, abholtermin, _, liefermenge, efz, status = match.groups()
        elif match.re is pattern_no_times:
            _, abholtermin, liefermenge, efz, _, status = match.groups()
        else:
            abholtermin, liefermenge, efz, status = match.groups()

        status = edi_status_map.get(status.strip().lower(), status.title())

        records.append({
            "Site": "Germany",
            "ClientCode": client_code,
            "ClientMaterialNo": client_material_no,
            "AVOMaterialNo": avo_material_no,
            "DateFrom": to_forecast_week(abholtermin),
            "DateUntil": abholtermin,
            "Quantity": convert_euro(liefermenge),
            "ForecastDate": to_forecast_week(forecast_date),
            "LastDeliveryNo": last_delivery_no,
            "LastDeliveryDate": to_forecast_week(last_delivery_date),
            "LastDeliveredQuantity": last_delivered_qty,
            "CumulatedQuantity": convert_euro(efz),
            "EDIStatus": status,
            "ProductName": product_name,
        })

    return records


def parse_pdf(file_bytes_io):
    """
    Parses a PDF from a BytesIO object using PyPDF2.
    """
    reader = PdfReader(file_bytes_io)
    text = ''
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text += page_text + '\n'
    return text


def process_nidec_de_pdf(file_bytes):
    import re, io
    from datetime import datetime

    text = parse_pdf(io.BytesIO(file_bytes))

    # --- helpers ---
    def int_from(s):
        return int(re.sub(r"[^\d]", "", s)) if s else 0

    def safe_to_forecast_week(date_str):
        if not date_str:
            return ""
        for fmt in ("%m/%d/%Y", "%d.%m.%Y", "%m.%d.%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                dt = None
        if not dt:
            return ""
        week = dt.isocalendar()[1]
        
        return f"{dt.year}-W{week:02d}"

    records = []

    # Normalize whitespace
    norm = re.sub(r"[ \t]+", " ", text)

    # --- Extract ForecastDate ONCE from document-level Release Date ---
    release_m = re.search(r"Release date\s*(\d{2}/\d{2}/\d{4})", norm, re.IGNORECASE)
    release_date = release_m.group(1) if release_m else ""
    forecast_week = safe_to_forecast_week(release_date)

    # --- Split into product blocks ---
    starts = [m.start() for m in re.finditer(r"NIDEC PART NUMBER:\s*\S+", norm)]
    if not starts:
        return records

    blocks = [norm[starts[i]: starts[i+1]] for i in range(len(starts)-1)]
    blocks.append(norm[starts[-1]:])

    for block in blocks:
        # Header fields
        part_m = re.search(r"NIDEC PART NUMBER:\s*([A-Za-z0-9\-]+)", block)
        desc_m = re.search(r"DESCRIPTION:\s*([^\n\r]+)", block)

        part = part_m.group(1) if part_m else "UNKNOWN"
        desc = (desc_m.group(1).strip() if desc_m else None)

        # Status = Forecast if FORECAST SCHEDULE is found
        is_forecast = "FORECAST SCHEDULE" in block.upper()
        edi_status = "Forecast" if is_forecast else "Firm"

        # Last delivery
        last_m = re.search(
            r"Last goods receipt\s*([\d\.,]+)\s*items.*?on\s*(\d{2}/\d{2}/\d{4}).*?delivery note no\.\s*([0-9A-Za-z]+)",
            block, flags=re.IGNORECASE | re.DOTALL
        )
        last_qty = int_from(last_m.group(1)) if last_m else 0
        last_date = last_m.group(2) if last_m else ""
        last_note = last_m.group(3) if last_m else ""

        # Shipment schedule section
        sched_m = re.search(
            r"(DATE\s+QUANTITY\s+CUMM\s+QTY\.[\s\S]*?)(?:CUMMS AUTHORIZATION|FORECAST SCHEDULE|LAST DELIVERIES|PAGE:|SCHEDULE AGREEMENT|TRANSIT INFORMATION)",
            block, flags=re.IGNORECASE
        )
        sched_text = sched_m.group(1) if sched_m else ""

        # Parse rows
        for line in sched_text.splitlines():
            line = line.strip()
            if not line:
                continue
            m = re.match(r"^(\d{2}/\d{2}/\d{4})\s+([\d,]+)\s+([\d,]+)$", line)
            if not m:
                continue
            ship_date, qty_s, cum_s = m.groups()
            qty = int_from(qty_s)
            cum = int_from(cum_s)

            records.append({
                "Site": "Germany",
                "ClientCode": "302194",                    # Nidec Germany (ZI01)
                "ClientMaterialNo": part,
                "AVOMaterialNo": part,
                "DateFrom": safe_to_forecast_week(ship_date),
                "DateUntil": ship_date,
                "Quantity": qty,
                "ForecastDate": forecast_week,             # Global value reused
                "LastDeliveryDate": safe_to_forecast_week(last_date) if last_date else "",
                "LastDeliveredQuantity": last_qty,
                "CumulatedQuantity": cum,
                "EDIStatus": edi_status,
                "ProductName": desc,
                "LastDeliveryNo": last_note
            })

    return records






@app.route("/process-GermanySite", methods=['POST'])
def process_file_endpoint_germany():
    data = request.get_json()
    required_keys = ['file_name', 'file_content_base64']
    if not data or not all(k in data for k in required_keys):
        missing_keys = [k for k in required_keys if k not in data]
        return jsonify({"error": f"Missing keys in request body: {', '.join(missing_keys)}"}), 400

    file_name = data['file_name']
    file_content_base64 = data['file_content_base64']
    file_type = data.get('file_type', None)
    is_pdf = file_type == "pdf" or file_name.lower().endswith('.pdf')

    try:
        file_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

    extracted_records = []
    company = None
    header = None

    if not is_pdf:
        try:
            logging.warning(f"DEBUG: first 100 b64 chars: {file_content_base64[:100]}")
            csv_text = decode_and_clean_csv(file_content_base64)
            csv_io = io.StringIO(csv_text)
            rows = list(csv.reader(csv_io, delimiter=';'))

            header = [col.strip() for col in rows[0]]
            rows_cleaned = []
            for row in rows:
                cleaned_row = [cell.strip() for cell in row]
                while cleaned_row and cleaned_row[-1] == '':
                    cleaned_row.pop()
                rows_cleaned.append(cleaned_row)
            rows = rows_cleaned
            rows[0] = header
        except Exception as e:
            logging.error(f"Failed to decode and clean Base64 string for file {file_name}: {e}")
            return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

        company, header = detect_company_and_prepare(rows)
        if not company:
            return jsonify({"error": "Unknown or unsupported CSV format."}), 400

        if company == "Valeo":
            extracted_records = process_valeo_de_csv_rows(rows, header)
        elif company == "Inteva":
            extracted_records = process_inteva_rows(rows, header)  # Optional: Replace if you want DE-specific Inteva logic
        elif company == "Nidec":
            extracted_records = process_nidec_de_csv_rows(rows, header)   # Optional: Replace if you want DE-specific Nidec logic
        else:
            return jsonify({"error": "Unrecognized company type."}), 400

    elif is_pdf:
        try:
            logging.warning(f"DEBUG: first 100 b64 chars: {file_content_base64[:100]}")
            file_bytes = base64.b64decode(file_content_base64)
            file = io.BytesIO(file_bytes)
            text = parse_pdf(file)

            if "Standortcode (Kunde):" in text:
                match = re.search(r"Standortcode\s*\(Kunde\):\s*(\w+)", text)
                if match:
                    bosch_code = match.group(1)
                    company="BOSCH"
                    bosch_code_to_client = {
                        "2570": "100409",
                        "5060": "100410",
                        "908A": "100327",
                        "526W": "100296"
                    }
                    client_code = bosch_code_to_client.get(bosch_code)
                    if not client_code:
                        return jsonify({"error": f"Unrecognized BOSCH Standortcode: {bosch_code}"}), 400

                    extracted_records = process_bosch_pdf(file_bytes, file_name)
                else:
                    return jsonify({"error": "Could not extract Standortcode (Kunde) from Bosch PDF."}), 400
            elif "NIDEC PART NUMBER" in text and "AVO CARBON GERMANY GMBH" in text:
                company = "Nidec USA"
                extracted_records = process_nidec_de_pdf(file_bytes)
            elif "DENSO MANUFACTURING ITALIA" in text and "MATERIAL RELEASE" in text:
                if "AVO CARBON GERMANY GMBH" in text:
                    company = "Denso"
                    extracted_records = process_denso_de_pdf(file_bytes)
                else:
                    return jsonify({"error": "Unrecognized Denso recipient in PDF."}), 400
            else:
                return jsonify({"error": "Unrecognized PDF format – Bosch header not found."}), 400

        except Exception as e:
            logging.exception("Error processing PDF file:")
            return jsonify({"error": f"Failed to process PDF. Detail: {str(e)}"}), 400
    success_count, error_details = save_to_postgres_with_conflict_reporting(extracted_records)
    return jsonify({
        "message": "Germany processing completed.",
        "file_processed": file_name,
        "company_detected": company,
        "records_processed": len(extracted_records),
        "records_inserted": success_count,
        "records_failed": len(error_details),
        "errors": error_details
    }), (200 if success_count > 0 else 400)





# ========================= EDI ANALYSIS (FULL) =========================

def _log(msg, *args):
    try:
        logger = app.logger  # Flask app logger if available in your project
    except Exception:
        logger = logging.getLogger("edi")
    logger.info(msg, *args)

# ========================= HELPERS =========================

WEEK_RE = re.compile(r"^\d{4}-W\d{2}$")  # e.g., 2025-W07

def norm_week_str(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    if len(s) >= 7 and s[4] == "-" and s[5] in ("W", "w"):
        year, wk = s[:4], s[6:]
        if wk.isdigit():
            return f"{year}-W{wk.zfill(2)}"
    return None

def parse_year_week(s: str) -> Optional[Tuple[int, int]]:
    s = norm_week_str(s)
    if not s:
        return None
    try:
        return int(s[:4]), int(s[-2:])
    except Exception:
        return None

def week_order_key(s: str):
    t = parse_year_week(s)
    return t if t else (0, 0)

def week_diff(a: str, b: str) -> Optional[int]:
    ta = parse_year_week(a); tb = parse_year_week(b)
    if not ta or not tb:
        return None
    (ya, wa), (yb, wb) = ta, tb
    return (ya - yb) * 52 + (wa - wb)

def get_interval(week_diff_val: Optional[int]) -> str:
    if week_diff_val is None: return "BackLog"
    if week_diff_val <= 1:    return "W-1 to W"      # current/backlog only
    # if week_diff_val == 1:  return "W+1"
    if 2 <= week_diff_val <= 5:   return "W+2 to W+5"
    if 6 <= week_diff_val <= 14:  return "W+6 to W+14"
    if 15 <= week_diff_val <= 24: return "W+15 to W+24"
    if week_diff_val >= 25:       return "W+25 and more"
    return "Other"

def interval_week_diff(interval: str) -> Optional[int]:
    return {"W-1 to W": 1, "W+2 to W+5": 3, "W+6 to W+14": 8, "W+15 to W+24": 9, "W+25 and more": None}.get(interval)

def get_allowed_change(interval: str) -> int:
    return {
        "W-1 to W": 0, "W+1": 0, "W+2 to W+5": 5,
        "W+6 to W+14": 10, "W+15 to W+24": 15, "W+25 and more": 20
    }.get(interval, 0)

def group_and_sum(rows: List[dict], group_keys: List[str], sum_key: str) -> List[dict]:
    grouped = defaultdict(float)
    for row in rows:
        key = tuple(row.get(k) for k in group_keys)
        try:
            grouped[key] += float(row.get(sum_key, 0) or 0)
        except (ValueError, TypeError):
            pass
    out = []
    for key, total in grouped.items():
        base = dict(zip(group_keys, key))
        base[sum_key] = total
        out.append(base)
    return out

# ---------- Debug helpers ----------

def debug_dump_deliveries(tag: str, rows: List[dict], limit: int = 8):
    _log("[DELIV:%s] fetched rows: %d", tag, len(rows))
    statuses = Counter([str(r.get("Status","")).strip().lower() for r in rows])
    _log("[DELIV:%s] statuses: %s", tag, dict(statuses))
    sites = Counter([str(r.get("Site","")).strip() for r in rows])
    _log("[DELIV:%s] top sites: %s", tag, sites.most_common(5))
    # Products counted ONLY by AVOMaterialNo now that ClientMaterialNo is removed
    prods = Counter([str(r.get("AVOMaterialNo") or "") for r in rows])
    _log("[DELIV:%s] top products: %s", tag, prods.most_common(5))
    for r in rows[:limit]:
        _log("[DELIV:%s][SAMPLE] %s", tag, r)

def debug_dump_intransit_map_site_only(map_s: Dict[str, Dict[str, float]], limit_sites=5, limit_prods=8):
    _log("[ITR] site/product entries: %d", len(map_s))
    shown = 0
    for site, m in list(map_s.items())[:limit_sites]:
        items = sorted(m.items(), key=lambda kv: kv[1], reverse=True)[:limit_prods]
        _log("[ITR] site=%s top products: %s", site, items)
        shown += 1
        if shown >= limit_sites: break

def debug_log_coverage_row(phase: str, site: str, product: str, interval: str,
                           ref_week: str, required_w: float, in_transit: float, ok: bool):
    _log("[COV:%s] site=%s prod=%s interval=%s ref=%s required=%.2f in_transit=%.2f ok=%s",
         phase, site, product, interval, ref_week, required_w, in_transit, ok)

def debug_probe_deliverytable(product_codes: Optional[List[str]], sites: Optional[List[str]]):
    """Lightweight probes to show what's actually inside DeliveryDetails."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT UPPER(TRIM("Status")) AS s, COUNT(*) FROM "DeliveryDetails" GROUP BY 1 ORDER BY 2 DESC')
            rows = cur.fetchall()
            _log("[DELIV:PROBE] status counts: %s", rows)

            cur.execute('SELECT TRIM("Site") AS site, COUNT(*) FROM "DeliveryDetails" GROUP BY 1 ORDER BY 2 DESC LIMIT 20')
            rows = cur.fetchall()
            _log("[DELIV:PROBE] top sites: %s", rows)

            # Removed client-only probe; the column no longer exists.
            if product_codes:
                cur.execute('''
                    SELECT COUNT(*)
                    FROM "DeliveryDetails"
                    WHERE UPPER(COALESCE("AVOMaterialNo", '')) = ANY(%s)
                ''', ([p.upper() for p in product_codes],))
                cnt = cur.fetchone()[0]
                _log("[DELIV:PROBE] rows matching product_codes by AVOMaterialNo: %s", cnt)

            if sites:
                cur.execute('''
                    SELECT COUNT(*)
                    FROM "DeliveryDetails"
                    WHERE UPPER(TRIM("Site")) = ANY(%s)
                ''', ([s.upper().strip() for s in sites],))
                cnt = cur.fetchone()[0]
                _log("[DELIV:PROBE] rows matching sites: %s", cnt)
    except Exception as e:
        _log("[DELIV:PROBE][ERROR] %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ========================= DB FETCHERS =========================

def fetch_ediglobal(
    weeks: List[str],
    client_codes: Optional[List[str]],
    product_codes: Optional[List[str]],
    sites: Optional[List[str]],
) -> List[dict]:
    fields = '"Site","ClientCode","AVOMaterialNo","DateFrom","ForecastDate","Quantity"'
    sql = f'SELECT {fields} FROM "EDIGlobal" WHERE "ForecastDate" = ANY(%s)'
    params: List[Any] = [weeks]

    if client_codes:
        sql += ' AND "ClientCode" = ANY(%s)'
        params.append(client_codes)
    if product_codes:
        sql += ' AND "AVOMaterialNo" = ANY(%s)'
        params.append(product_codes)
    if sites:
        sql += ' AND "Site" = ANY(%s)'
        params.append(sites)

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

def fetch_deliverydetails_raw(
    product_codes: Optional[List[str]],
    sites: Optional[List[str]],
) -> List[dict]:
    """
    Fetch delivery rows broadly and normalize:
    - Accept common status variants (IN TRANSIT, INTRANSIT)
    - Match products by AVOMaterialNo (case-insensitive)
    - Match Site case-insensitively, trimmed
    """
    prods_upper = [p.upper().strip() for p in product_codes] if product_codes else None
    sites_upper = [s.upper().strip() for s in sites] if sites else None

    # NOTE: ClientMaterialNo removed from both SELECT and filters.
    fields = '"Site","AVOMaterialNo","Date","Status","Quantity"'
    sql = f'''
        SELECT {fields}
        FROM "DeliveryDetails"
        WHERE UPPER(TRIM("Status")) IN ('IN TRANSIT','INTRANSIT')
    '''
    params: List[Any] = []

    if prods_upper:
        sql += '''
          AND UPPER(COALESCE("AVOMaterialNo", '')) = ANY(%s)
        '''
        params.append(prods_upper)

    if sites_upper:
        sql += ' AND UPPER(TRIM("Site")) = ANY(%s)'
        params.append(sites_upper)

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            debug_dump_deliveries("RAW", rows)
            return rows
    finally:
        conn.close()

def fetch_deliverydetails(
    weeks: List[str],                 # kept for signature compatibility
    product_codes: Optional[List[str]],
    sites: Optional[List[str]],
) -> List[dict]:
    # Return ALL relevant delivery rows (no week filter)
    return fetch_deliverydetails_raw(product_codes, sites)

# ========================= DB FETCHERS =========================
def fetch_productdetails_map(
    product_codes: Optional[List[str]],
) -> Dict[str, Dict[str, Any]]:
    """
    Return a mapping: AVOMaterialNo -> {"Line": <str or None>, "WeeklyCapacity": <float or None>}
    Keys are added in both raw and UPPER() forms to ease lookups.
    """
    sql = 'SELECT "AVOMaterialNo","Line","WeeklyCapacity" FROM "ProductDetails"'
    params: List[Any] = []
    if product_codes:
        sql += ' WHERE "AVOMaterialNo" = ANY(%s)'
        params.append(product_codes)

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            out: Dict[str, Dict[str, Any]] = {}
            for avo, line, cap in rows:
                key_raw = str(avo or "").strip()
                key_up  = key_raw.upper()
                rec = {
                    "Line": line,
                    "WeeklyCapacity": float(cap) if cap is not None else None,
                }
                if key_raw:
                    out[key_raw] = rec
                    out[key_up]  = rec
            return out
    finally:
        conn.close()


# ========================= CORE ANALYSIS =========================

def run_edi_analysis(
    edi_rows: List[dict],
    delivery_rows: List[dict],
    product_info: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    # Normalize weeks + intervals on EDI
    for r in edi_rows:
        r["ForecastDate"] = norm_week_str(r.get("ForecastDate"))
        r["DateFrom"]     = norm_week_str(r.get("DateFrom"))

    all_ref_weeks = sorted({r["ForecastDate"] for r in edi_rows if r.get("ForecastDate")}, key=week_order_key)
    if len(all_ref_weeks) < 2:
        raise ValueError(f"Cumulative analysis requires at least 2 forecast weeks, found {len(all_ref_weeks)}.")

    rows_by_refweek: Dict[str, List[dict]] = defaultdict(list)
    for r in edi_rows:
        ref_w = r["ForecastDate"]
        row_w = r["DateFrom"]
        diff = week_diff(row_w, ref_w)
        r["Interval"] = get_interval(diff)
        rows_by_refweek[ref_w].append(r)

    # ---- Effective In-Transit: (Site) -> { Product -> qty } ----
    in_transit_map: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for d in delivery_rows:
        site = (d.get("Site") or "").strip()
        prod = (d.get("AVOMaterialNo") or "").strip()  # ClientMaterialNo removed
        if not site or not prod:
            continue
        try:
            qty = float(d.get("Quantity", 0) or 0)
        except (ValueError, TypeError):
            qty = 0.0
        status_norm = str(d.get("Status","")).strip().upper().replace(" ", "")
        if status_norm in {"INTRANSIT", "DISPATCHED"}:
            in_transit_map[site][prod] += qty
        elif status_norm == "DELIVERED":
            in_transit_map[site][prod] -= qty
    # clamp negatives
    for site, m in in_transit_map.items():
        for p in list(m.keys()):
            if m[p] < 0:
                m[p] = 0.0
    debug_dump_intransit_map_site_only(in_transit_map)

    detailed_report: List[dict] = []
    all_quantities_by_key: Dict[Tuple[str, str, str, str], Dict[str, float]] = defaultdict(dict)

    for i in range(len(all_ref_weeks) - 1):
        w1_ref, w2_ref = all_ref_weeks[i], all_ref_weeks[i + 1]
        
        w1_data = rows_by_refweek.get(w1_ref, [])
        w2_data = rows_by_refweek.get(w2_ref, [])

        group_keys = ["Site", "ClientCode", "AVOMaterialNo", "Interval"]
        grouped_w1 = group_and_sum(w1_data, group_keys, "Quantity")
        grouped_w2 = group_and_sum(w2_data, group_keys, "Quantity")

        w1_map = {(r["Site"], r["ClientCode"], r["AVOMaterialNo"], r["Interval"]): r.get("Quantity", 0.0) for r in grouped_w1}
        w2_map = {(r["Site"], r["ClientCode"], r["AVOMaterialNo"], r["Interval"]): r.get("Quantity", 0.0) for r in grouped_w2}

        all_keys = set(w1_map.keys()) | set(w2_map.keys())

        for key in all_keys:
            site, client, product, interval = key
            q1 = float(w1_map.get(key, 0.0))
            q2 = float(w2_map.get(key, 0.0))

            all_quantities_by_key[key][w1_ref] = q1
            all_quantities_by_key[key][w2_ref] = q2

            difference = q2 - q1
            variation_pct = round(100 * (difference / q1), 2) if q1 > 0 else 0.0
            allowed_change = get_allowed_change(interval)
            violation = abs(variation_pct) > allowed_change
            required_w = q2
            in_transit = in_transit_map.get(site, {}).get(str(product), 0.0)
            coverage_ok = in_transit >= required_w

            # ---- attach product meta (Line, WeeklyCapacity)
            prod_key = str(product or "").strip()
            info = product_info.get(prod_key) or product_info.get(prod_key.upper())
            line = info.get("Line") if info else None
            cap  = info.get("WeeklyCapacity") if info else None

            row = {
                "Week_Comparison": f"{w1_ref}_vs_{w2_ref}",
                "Site": site,
                "ClientCode": client,
                "AVOMaterialNo": product,
                "Interval": interval,
                "Interval_Week_Diff": interval_week_diff(interval),
                "Quantity_W1": q1,
                "Quantity_W2": q2,
                "Difference": difference,
                "Variation_Pct": f"{variation_pct}%",
                "Allowed_Change_%": allowed_change,
                "Violation": violation,
                "InTransit": in_transit,
                "Required_W": required_w,
                "Coverage_OK": coverage_ok,
                "Delivery_Issue": not coverage_ok,
                "Line": line,
                "WeeklyCapacity": cap,
            }

            if interval == "W-1 to W":
                required_w = q2
                in_transit = in_transit_map.get(site, {}).get(str(product), 0.0)
                coverage_ok = in_transit >= required_w
                row.update({
                    "InTransit": in_transit,
                    "Required_W": required_w,
                    "Coverage_OK": coverage_ok,
                    "Delivery_Issue": not coverage_ok,
                })
                debug_log_coverage_row("MULTI", site, str(product), interval, w2_ref, required_w, in_transit, coverage_ok)

            detailed_report.append(row)

    # Summary (unchanged)
    summary_per_group = []
    start_ref, end_ref = all_ref_weeks[0], all_ref_weeks[-1]
    for key, weekly_quantities in all_quantities_by_key.items():
        site, client, product, interval = key
        group_rows = [
            r for r in detailed_report
            if r["Site"] == site and r["ClientCode"] == client and r["AVOMaterialNo"] == product and r["Interval"] == interval
        ]
        total_diff = sum(r["Difference"] for r in group_rows)
        start_q = float(weekly_quantities.get(start_ref, 0.0))
        end_q   = float(weekly_quantities.get(end_ref, 0.0))
        total_pct_var = round(100 * (end_q - start_q) / start_q, 2) if start_q > 0 else 0.0
        summary_per_group.append({
            "Site": site,
            "ClientCode": client,
            "AVOMaterialNo": product,
            "Interval": interval,
            "Total_Cumulated_Quantity_Difference": total_diff,
            "Total_Cumulated_Percentage_Variation": f"{total_pct_var}%"
        })

    # Sheets (carry Line & WeeklyCapacity via detailed_report rows)
    green_sheet, red_sheet = [], []
    for r in detailed_report:
        # NEW LOGIC: If there is a Violation, it is ALWAYS Red.
        # W-1 to W has a 0% tolerance, so any change will trigger a Violation.
        is_green = (r.get("Violation") is False)
        if is_green:
            green_sheet.append(r)
        else:
            if r["Interval"] == "W-1 to W" and r.get("Delivery_Issue") is True:
                red_sheet.append(r)
            elif r.get("Violation") is True:
                red_sheet.append(r)

    return {"summary_per_group": summary_per_group, "green_sheet": green_sheet, "red_sheet": red_sheet}


# ---------- single-week analysis (coverage-only) ----------

def analyze_single_week(
    edi_rows: List[dict],
    delivery_rows: List[dict],
    product_info: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    for r in edi_rows:
        r["ForecastDate"] = norm_week_str(r.get("ForecastDate"))
        r["DateFrom"]     = norm_week_str(r.get("DateFrom"))
    ref_weeks = sorted({r["ForecastDate"] for r in edi_rows if r.get("ForecastDate")}, key=week_order_key)
    if not ref_weeks:
        return {"summary_per_group": [], "green_sheet": [], "red_sheet": [], "analysis_mode": "single_week"}

    w_ref = ref_weeks[0]
    for r in edi_rows:
        r["Interval"] = get_interval(week_diff(r["DateFrom"], r["ForecastDate"]))

    # ---- Effective In-Transit: (Site) -> { Product -> qty } ----
    in_transit_map: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for d in delivery_rows:
        site = (d.get("Site") or "").strip()
        prod = (d.get("AVOMaterialNo") or "").strip()  # ClientMaterialNo removed
        if not site or not prod:
            continue
        try:
            qty = float(d.get("Quantity", 0) or 0)
        except (ValueError, TypeError):
            qty = 0.0
        status_norm = str(d.get("Status","")).strip().upper().replace(" ", "")
        if status_norm in {"INTRANSIT", "DISPATCHED"}:
            in_transit_map[site][prod] += qty
        elif status_norm == "DELIVERED":
            in_transit_map[site][prod] -= qty
    for site, m in in_transit_map.items():
        for p in list(m.keys()):
            if m[p] < 0:
                m[p] = 0.0
    debug_dump_intransit_map_site_only(in_transit_map)

    group_keys = ["Site","ClientCode","AVOMaterialNo","Interval"]
    grouped = group_and_sum([r for r in edi_rows if r["ForecastDate"] == w_ref], group_keys, "Quantity")

    detailed_report = []
    for g in grouped:
        site, client, product, interval = g["Site"], g["ClientCode"], g["AVOMaterialNo"], g["Interval"]
        q = float(g["Quantity"] or 0)

        # ---- attach product meta (Line, WeeklyCapacity)
        prod_key = str(product or "").strip()
        info = product_info.get(prod_key) or product_info.get(prod_key.upper())
        line = info.get("Line") if info else None
        cap  = info.get("WeeklyCapacity") if info else None

        row = {
            "Week_Comparison": f"{w_ref}_single",
            "Interval_Week_Diff": interval_week_diff(interval),
            "Site": site, "ClientCode": client, "AVOMaterialNo": product, "Interval": interval,
            "Quantity_W1": q, "Quantity_W2": q, "Difference": 0.0, "Variation_Pct": "0.0%",
            "Allowed_Change_%": get_allowed_change(interval), "Violation": False,
            "InTransit": "", "Required_W": "", "Coverage_OK": "", "Delivery_Issue": "",
            "Line": line, "WeeklyCapacity": cap,
        }
        if interval == "W-1 to W":
            required_w = q
            in_transit = in_transit_map.get(site, {}).get(str(product), 0.0)
            coverage_ok = in_transit >= required_w
            row.update({"InTransit": in_transit, "Required_W": required_w, "Coverage_OK": coverage_ok, "Delivery_Issue": not coverage_ok})
            debug_log_coverage_row("SINGLE", site, str(product), interval, w_ref, required_w, in_transit, coverage_ok)

        detailed_report.append(row)

    # summary: zeros (single-week) — unchanged
    summary_per_group = []
    for g in grouped:
        summary_per_group.append({
            "Site": g["Site"], "ClientCode": g["ClientCode"], "AVOMaterialNo": g["AVOMaterialNo"], "Interval": g["Interval"],
            "Total_Cumulated_Quantity_Difference": 0.0, "Total_Cumulated_Percentage_Variation": "0.0%"
        })

    # sheets (Line & WeeklyCapacity ride along in detailed rows)
    green_sheet, red_sheet = [], []
    for r in detailed_report:
        is_green = (r.get("Violation") is False)
        if is_green:
            green_sheet.append(r)
        else:
            if r["Interval"] == "W-1 to W" and r.get("Delivery_Issue") is True:
                red_sheet.append(r)
            elif r.get("Violation") is True:
                red_sheet.append(r)

    return {"summary_per_group": summary_per_group, "green_sheet": green_sheet, "red_sheet": red_sheet, "analysis_mode": "single_week"}


# ========================= ROUTE =========================

@app.route("/edi-analysis", methods=["POST"])
def edi_analysis_run():
    try:
        body = _extract_body()

        # Accept legacy keys too
        weeks = _coerce_list(body.get("forecastWeeks") or body.get("ediWeekNumbers"))
        client_codes = _coerce_list(body.get("clientCodes") or body.get("ClientCode"))
        product_codes = _coerce_list(body.get("productCodes") or body.get("ProductCode") or body.get("AVOMaterialNo"))
        sites = _coerce_list(body.get("sites") or body.get("Site"))

        if not weeks:
            return jsonify({"status":"error","message":"forecastWeeks is required (array)."}), 400
        # format check
        bad = [w for w in weeks if not WEEK_RE.match(str(w))]
        if bad:
            return jsonify({"status":"error","message":f"Weeks must be 'YYYY-WXX'. Bad: {bad}"}), 400

        _log("[ROUTE] weeks=%s client_codes=%s product_codes=%s sites=%s",
             weeks, client_codes, product_codes, sites)

        # Fetch DB rows
        edi_rows = fetch_ediglobal(weeks, client_codes, product_codes, sites)
        debug_probe_deliverytable(product_codes, sites)
        delivery_rows = fetch_deliverydetails(weeks, product_codes, sites)

        # Product meta (Line, WeeklyCapacity) for relevant products
        prods_for_meta = product_codes or sorted({
            str(r.get("AVOMaterialNo") or "").strip()
            for r in edi_rows if r.get("AVOMaterialNo")
        })
        product_info = fetch_productdetails_map(prods_for_meta)

        _log("[ROUTE] fetched edi_rows=%d delivery_rows=%d product_meta=%d",
             len(edi_rows), len(delivery_rows), len(product_info))

        # Single-week coverage mode OR multi-week full analysis
        if len(weeks) == 1:
            result = analyze_single_week(edi_rows, delivery_rows, product_info)
        else:
            result = run_edi_analysis(edi_rows, delivery_rows, product_info)

        return jsonify({"status":"ok","data":result}), 200

    except Exception as e:
        return jsonify({"status":"error","message":str(e)}), 500


# ---------- robust payload parsing ----------

def _coerce_list(v):
    """Accept list, single string, CSV string, numbers; return list[str] or None."""
    if v is None:
        return None
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, (int, float)):
        return [str(v)]
    if isinstance(v, str):
        s = v.strip()
        # try JSON array string
        if s.startswith("[") and s.endswith("]"):
            try:
                arr = json.loads(s)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        # CSV fallback
        return [p.strip() for p in s.split(",") if p.strip()]
    return None

def _extract_body():
    """Parse JSON (incl. gzipped), else form/multipart, else empty dict."""
    raw = request.get_data(cache=False, as_text=False)
    if raw:
        enc = (request.headers.get("Content-Encoding") or "").lower()
        if "gzip" in enc:
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            pass

    body = request.get_json(silent=True)
    if isinstance(body, dict):
        return body

    if request.form:
        d = {k: request.form.getlist(k) for k in request.form.keys()}
        return {k: (v if len(v) > 1 else v[0]) for k, v in d.items()}

    if request.args:
        d = {k: request.args.getlist(k) for k in request.args.keys()}
        return {k: (v if len(v) > 1 else v[0]) for k, v in d.items()}

    return {}






# ======================================================== ROUTE ==========================================================================


app.logger.setLevel(logging.INFO)


@app.route("/product-capacity-stock", methods=["POST"])
def product_capacity_stock():
    """
    Body (list or CSV string accepted):
    {
      "AVOMaterialNo": ["VA13116595N","V1001MR035"]
      // or
      "avo_material_nos": ["VA13116595N","V1001MR035"]
    }
    Returns WeeklyCapacity + Line from ProductDetails, and summed Quantity from ProductStock.
    (Fetched independently — no JOIN.)
    """
    try:
        body = _extract_body()
        app.logger.info("POST /product-capacity-stock payload: %s", body)

        avo_list = _coerce_list(body.get("AVOMaterialNo") or body.get("avo_material_nos"))
        app.logger.info("Parsed AVOMaterialNo: %s", avo_list)

        if not avo_list:
            app.logger.warning("Missing AVOMaterialNo.")
            return jsonify({"ok": False, "error": "AVOMaterialNo is required."}), 400

        # --- Query ProductDetails (WeeklyCapacity + Line) — NO JOIN ---
        sql_pd = '''
            SELECT
                "AVOMaterialNo",
                MAX("WeeklyCapacity") AS "WeeklyCapacity",
                MAX("Line") AS "Line"
            FROM public."ProductDetails"
            WHERE "AVOMaterialNo" = ANY(%s)
            GROUP BY "AVOMaterialNo";
        '''

        # --- Query ProductStock (TotalQuantity) — NO JOIN ---
        sql_ps = '''
            SELECT
                "ProductCode" AS "AVOMaterialNo",
                COALESCE(SUM("Quantity"), 0) AS "TotalQuantity"
            FROM public."ProductStock"
            WHERE "ProductCode" = ANY(%s)
            GROUP BY "ProductCode";
        '''

        app.logger.debug("SQL ProductDetails:\n%s", sql_pd)
        app.logger.debug("SQL ProductStock:\n%s", sql_ps)

        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # ProductDetails
                cur.execute(sql_pd, [avo_list])
                pd_rows = cur.fetchall()
                app.logger.info("ProductDetails rows: %d", len(pd_rows))
                # map: AVO -> {"WeeklyCapacity": x, "Line": y}
                pd_map = {
                    r["AVOMaterialNo"]: {
                        "WeeklyCapacity": r["WeeklyCapacity"],
                        "Line": r.get("Line")
                    }
                    for r in pd_rows
                }

                # ProductStock
                cur.execute(sql_ps, [avo_list])
                ps_rows = cur.fetchall()
                app.logger.info("ProductStock rows: %d", len(ps_rows))
                ps_map = {r["AVOMaterialNo"]: r["TotalQuantity"] for r in ps_rows}
        finally:
            conn.close()

        # Merge results in Python (preserve request order)
        items = []
        for avo in avo_list:
            pd_info = pd_map.get(avo, {})
            weekly = pd_info.get("WeeklyCapacity")
            line   = pd_info.get("Line")
            totalq = ps_map.get(avo, 0)

            items.append({
                "AVOMaterialNo": avo,
                "Line": line,  # <= added
                "WeeklyCapacity": int(weekly) if weekly is not None else None,
                "TotalQuantity": int(totalq or 0),
            })

        return jsonify({"ok": True, "count": len(items), "items": items}), 200

    except Exception as e:
        app.logger.error("Error in /product-capacity-stock: %s\n%s", str(e), traceback.format_exc())
        return jsonify({"ok": False, "error": str(e)}), 500


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------
# >>> DECISION MATRIX & REPORTING LOGIC (DETERMINISTIC + AI WORDING ONLY) <<<
# ---------------------------------------------------------
# What this does:
# - Python computes an exact Case_ID per red row (deterministic decision).
# - A fixed DECISION_MAP defines "Who_Pays / What_To_Do / Next_Action" per case.
# - ChatGPT is used ONLY to rewrite the deterministic fields into a clean client-facing Decision_Detail.
# - Decisions are applied ONLY to red_sheet. Green stays raw (no decisions).
# - No decision is added for rows that don't need it (Case_ID is None).
#
# SECURITY:
# - Uses OPENAI_API_KEY from environment variables only (no hardcoded keys).




# ========================= PRIMITIVES / AI ROW HELPERS =========================

def build_ai_row(i, row):
    """Doc/PPT-aligned primitives. Optional; used if you want richer case logic."""
    G = (row.get("Difference", 0) or 0)
    absG = abs(G)

    # Stock
    available_stock = row.get("Available_Stock", 0) or 0
    safety_stock = row.get("Safety_Stock", 0) or 0
    safety_protect = bool(row.get("Safety_Protect", True))
    fg_free = max(available_stock - (safety_stock if safety_protect else 0), 0)
    stock_cover = fg_free >= absG

    # Capacity
    line_capacity = row.get("Line_Capacity", row.get("WeeklyCapacity", 0)) or 0
    planned_qty = row.get("Planned_Qty_on_Line", 0) or 0
    normal_free_cap = max(line_capacity - planned_qty, 0)
    cap_norm_cover = normal_free_cap >= absG

    ot_ok = bool(row.get("OT_OK", False))
    ot_capacity = row.get("OT_Capacity", 0) or 0
    ot_cap_cover = ot_ok and (ot_capacity >= absG)

    alt_site_ok = bool(row.get("Alt_Site_OK", False))
    alt_cap = row.get("AltCap", 0) or 0
    subc_ok = bool(row.get("SubC_OK", False))
    subc_cap = row.get("SubCCap", 0) or 0
    alt_or_subc_cover = (alt_site_ok and alt_cap >= absG) or (subc_ok and subc_cap >= absG)

    # Material & logistics
    material_ok = bool(row.get("Material_OK", False))
    logi_ok = bool(row.get("Logi_OK", False))
    air_ok = bool(row.get("Air_OK", False))

    # Commercial
    out_of_protocol = bool(row.get("OutOfProtocol", row.get("Violation", False)))

    interval = row.get("Interval")
    critical = (interval == "W-1 to W") and (not stock_cover)

    return {
        "id": i,
        "Reference": row.get("AVOMaterialNo"),
        "Interval": interval,
        "G": G,
        "AbsG": absG,
        "INC": G > 0,
        "DEC": G < 0,
        "OutOfProtocol": out_of_protocol,
        "FG_Free": fg_free,
        "StockCover": stock_cover,
        "Normal_Free_Cap": normal_free_cap,
        "CapNormCover": cap_norm_cover,
        "OTCapCover": ot_cap_cover,
        "AltOrSubCCover": alt_or_subc_cover,
        "Material_OK": material_ok,
        "Logi_OK": logi_ok,
        "Air_OK": air_ok,
        "CRITICAL": critical,
        "WIP_Risk": bool(row.get("WIP_Risk", False)),
        "Swap_OK": bool(row.get("Swap_OK", False)),
        "PO_Cancelable": bool(row.get("PO_Cancelable", False)),
        "RS_OK": bool(row.get("RS_OK", False)),
        "Storage_OK": bool(row.get("Storage_OK", False)),
        "Realloc_OK": bool(row.get("Realloc_OK", False)),
    }

def choose_decrease_mitigation(row):
    """
    Pick EXACTLY ONE mitigation lever for DEC cases, in priority order.
    Adjust the priority order to match your policy.
    """
    if bool(row.get("PO_Cancelable", False)):
        return ("CancelPO", "Cancel open purchase orders where feasible to stop additional exposure.")
    if bool(row.get("Realloc_OK", False)):
        return ("Realloc", "Reallocate surplus to other demand where feasible to reduce liability.")
    if bool(row.get("Storage_OK", False)):
        return ("Storage", "Store surplus under agreed storage terms to contain liability.")
    if bool(row.get("RS_OK", False)):
        return ("Replan", "Replan production to avoid additional WIP build-up.")
    # If nothing is feasible, the ONE action is escalation (not a menu)
    return ("Escalate", "No mitigation lever is feasible—escalate for a containment plan and freeze further build.")



# ========================= CASE ENGINE (DETERMINISTIC) =========================

def compute_case_id(r):
    """
    Deterministic classification for Red Sheet rows.
    Returns Case_ID or None (no decision needed).
    """
    G = (r.get("Difference", 0) or 0)
    if G == 0:
        return None

    INC = G > 0
    DEC = G < 0

    oop = bool(r.get("OutOfProtocol", r.get("Violation", False)))
    interval = r.get("Interval")

    stock_cover = bool(r.get("StockCover", r.get("Coverage_OK", False)))
    cap_norm_cover = bool(r.get("CapNormCover", False))
    ot_cover = bool(r.get("OTCapCover", False))
    alt_subc_cover = bool(r.get("AltOrSubCCover", False))

    material_ok = bool(r.get("Material_OK", False))
    logi_ok = bool(r.get("Logi_OK", False))
    air_ok = bool(r.get("Air_OK", False))

    wip_risk = bool(r.get("WIP_Risk", False))
    swap_ok = bool(r.get("Swap_OK", False))

    # CRITICAL increase: W-1 to W + no stock
    if INC and interval == "W-1 to W" and not stock_cover:
        return "INC_CRITICAL_AIR" if air_ok else "INC_CRITICAL_EXPEDITE"

    if INC:
        if stock_cover:
            return "INC_OOP_STOCK" if oop else "INC_IP_STOCK"
        if cap_norm_cover and material_ok and logi_ok:
            return "INC_OOP_NORMCAP" if oop else "INC_IP_NORMCAP"
        if ot_cover and material_ok and (logi_ok or air_ok):
            return "INC_OOP_OT" if oop else "INC_IP_OT"
        if alt_subc_cover and material_ok and (logi_ok or air_ok):
            return "INC_OOP_ALT_SUBC" if oop else "INC_IP_ALT_SUBC"
        if swap_ok:
            return "INC_SWAP_PARTIAL"
        return "INC_MANUAL"

    if DEC:
        if oop:
            return "DEC_OOP"
        if wip_risk:
            return "DEC_WIP_RISK"
        return "DEC_IP"

    return None

DECISION_MAP = {
    # -------- INCREASE: Inside protocol (Supplier pays) --------
    "INC_IP_STOCK": {
        "Who_Pays": "Supplier",
        "Lever": "Stock",
        "What_To_Do": "Ship the additional quantity from available finished-goods stock (do not consume safety stock).",
        "Next_Action": "Planner confirms FG availability; Warehouse executes shipment; Customer Service confirms ship date."
    },
    "INC_IP_NORMCAP": {
        "Who_Pays": "Supplier",
        "Lever": "NormalCap",
        "What_To_Do": "Produce the additional quantity within normal capacity (standard hours).",
        "Next_Action": "Planner adds to schedule; Procurement confirms material release; Logistics books standard transport."
    },
    "INC_IP_OT": {
        "Who_Pays": "Supplier",
        "Lever": "OT",
        "What_To_Do": "Cover the increase using overtime capacity (supplier absorbs incremental cost).",
        "Next_Action": "Ops approves OT plan; Planner schedules OT lot; Logistics confirms transport mode."
    },
    "INC_IP_ALT_SUBC": {
        "Who_Pays": "Supplier",
        "Lever": "AltLine/SubC",
        "What_To_Do": "Cover the increase via alternate line or subcontracting (supplier absorbs incremental cost).",
        "Next_Action": "Industrial validates routing; Procurement places subcontract PO; Planner allocates loads; Logistics confirms transport."
    },

    # -------- INCREASE: Out of protocol (Client pays) --------
    "INC_OOP_STOCK": {
        "Who_Pays": "Client",
        "Lever": "Stock",
        "What_To_Do": "Ship from finished-goods stock with off-protocol surcharge.",
        "Next_Action": "Finance applies surcharge; Planner confirms stock; Customer Service obtains approval and confirms ship date."
    },
    "INC_OOP_NORMCAP": {
        "Who_Pays": "Client",
        "Lever": "NormalCap",
        "What_To_Do": "Produce in standard hours and apply off-protocol surcharge.",
        "Next_Action": "Planner schedules production; Finance issues quotation; Customer Service obtains approval and confirms commit date."
    },
    "INC_OOP_OT": {
        "Who_Pays": "Client",
        "Lever": "OT",
        "What_To_Do": "Cover the increase using overtime; charge OT premium plus off-protocol surcharge.",
        "Next_Action": "Ops confirms feasibility; Finance issues OT premium quotation; Customer Service obtains approval; Planner schedules OT."
    },
    "INC_OOP_ALT_SUBC": {
        "Who_Pays": "Client",
        "Lever": "AltLine/SubC",
        "What_To_Do": "Cover the increase via alternate line/subcontracting; charge incremental uplift plus off-protocol surcharge.",
        "Next_Action": "Procurement aligns subcontracting; Finance issues quotation; Customer Service obtains approval; Planner allocates loads."
    },

    # -------- INCREASE: CRITICAL (payer is dynamic based on protocol) --------
    "INC_CRITICAL_AIR": {
        "Who_Pays": "DYNAMIC",
        "Lever": "Air",
        "What_To_Do": "CRITICAL: Expedite production and ship by air to avoid immediate stockout.",
        "Next_Action": "Planner escalates immediately; Logistics books air; Customer Service confirms emergency commit and obtains approval if off-protocol."
    },
    "INC_CRITICAL_EXPEDITE": {
        "Who_Pays": "DYNAMIC",
        "Lever": "ExpediteSupplier",
        "What_To_Do": "CRITICAL: Expedite production/materials immediately to avoid stockout (air not available).",
        "Next_Action": "Planner escalates; Procurement expedites suppliers; Ops prioritizes line; Customer Service confirms commit and obtains approval if off-protocol."
    },

    # -------- DECREASE: Inside protocol --------
    "DEC_IP": {
        "Who_Pays": "Supplier",
        "Lever": "Replan",
        "What_To_Do": "Decrease within protocol: replan the surplus to avoid unnecessary production.",
        "Next_Action": "Planner updates plan and releases/holds production orders accordingly."
    },

    # DEC_OOP / DEC_WIP_RISK are computed dynamically (one lever picked),
    # so they are not fully defined here.
    "INC_SWAP_PARTIAL": {
        "Who_Pays": "Manual",
        "Lever": "Swap",
        "What_To_Do": "Validate SWAP feasibility to cover the increase before committing.",
        "Next_Action": "Planner/Sales identify swap candidate and confirm feasibility; then update plan and commit date."
    },
}


def apply_matrix_decisions_red_only(analysis_result):
    """
    Applies deterministic, precise decisions ONLY to Red sheet rows.
    Adds:
      - Case_ID
      - Who_Pays
      - Lever (single)
      - What_To_Do
      - Next_Action
      - Decision_Summary (concise)
      - Decision_Source
    Leaves rows with no case_id untouched (no decision).
    """
    red = analysis_result.get("red_sheet") or []

    for row in red:
        case_id = compute_case_id(row)
        if not case_id:
            continue

        oop = bool(row.get("OutOfProtocol", row.get("Violation", False)))

        if case_id in ("DEC_OOP", "DEC_WIP_RISK"):
            lever, lever_text = choose_decrease_mitigation(row)
            who_pays = "Client" if case_id == "DEC_OOP" else ("Client" if oop else "Manual")
            row["Case_ID"] = case_id
            row["Who_Pays"] = who_pays
            row["Lever"] = lever
            row["What_To_Do"] = (
                "Decrease is out of protocol: charge liability and apply mitigation. " + lever_text
                if case_id == "DEC_OOP"
                else "Decrease triggers WIP risk: apply mitigation. " + lever_text
            )
            row["Next_Action"] = (
                "Planner quantifies exposure; Finance computes charges; Customer Service issues formal acknowledgement."
                if case_id == "DEC_OOP"
                else "Supply Chain lead reviews exposure and approves mitigation; Planner executes; Finance/CS align commercial handling."
            )
            row["Decision_Summary"] = row["What_To_Do"]
            row["Decision_Source"] = "Matrix"
            continue

        if case_id == "INC_MANUAL":
            blockers = list_increase_blockers(row)
            row["Case_ID"] = case_id
            row["Who_Pays"] = "Manual"
            row["Lever"] = "Escalate"
            row["What_To_Do"] = (
                "Increase cannot be met with available levers; escalate to renegotiate commit date."
                + (f" Blockers: {', '.join(blockers)}." if blockers else "")
            )
            row["Next_Action"] = "Supply Chain lead proposes revised commit/partial acceptance; Customer Service communicates decision to client."
            row["Decision_Summary"] = row["What_To_Do"]
            row["Decision_Source"] = "Matrix"
            continue

        templ = DECISION_MAP.get(case_id)
        if not templ:
            continue

        # Dynamic payer for critical cases
        if templ.get("Who_Pays") == "DYNAMIC":
            who_pays = "Client" if oop else "Supplier"
        else:
            who_pays = templ["Who_Pays"]

        row["Case_ID"] = case_id
        row["Who_Pays"] = who_pays
        row["Lever"] = templ.get("Lever")
        row["What_To_Do"] = templ.get("What_To_Do")
        row["Next_Action"] = templ.get("Next_Action")
        row["Decision_Summary"] = templ.get("What_To_Do")
        row["Decision_Source"] = "Matrix"

    return analysis_result



def list_increase_blockers(row):
    """
    Extract precise blockers for INC_MANUAL decisions.
    Uses doc/PPT primitives if present; falls back to legacy fields.
    """
    blockers = []

    stock_cover = bool(row.get("StockCover", row.get("Coverage_OK", False)))
    cap_norm_cover = bool(row.get("CapNormCover", False))
    ot_cover = bool(row.get("OTCapCover", False))
    alt_subc_cover = bool(row.get("AltOrSubCCover", False))
    material_ok = bool(row.get("Material_OK", False))
    logi_ok = bool(row.get("Logi_OK", False))
    air_ok = bool(row.get("Air_OK", False))

    if not stock_cover:
        blockers.append("no FG stock cover")
    if not cap_norm_cover:
        blockers.append("no normal capacity")
    if not ot_cover:
        blockers.append("no overtime capacity")
    if not alt_subc_cover:
        blockers.append("no alternate/subcontract capacity")
    if not material_ok:
        blockers.append("materials not ready")
    if not (logi_ok or air_ok):
        blockers.append("logistics not feasible (standard/air)")

    return blockers



# ========================= OPENAI CLIENT (ENV ONLY) =========================

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
client = openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def _safe_json_loads(text: str):
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            raise
        return json.loads(m.group(0))


# ========================= AI WORDING ONLY =========================

def rewrite_decisions_with_ai_one_sentence(red_sheet_rows):
    """
    AI is used ONLY to rewrite the already-determined matrix decision into ONE precise client-facing sentence.
    Must NOT add options, must NOT propose multiple levers.
    Returns JSON: { "row_index": "Decision_Detail sentence" }
    """
    if not red_sheet_rows:
        return red_sheet_rows
    if not client:
        logging.warning("OpenAI client not configured; skipping AI wording rewrite.")
        return red_sheet_rows

    payload = []
    idxs = []

    for i, row in enumerate(red_sheet_rows):
        if not row.get("Case_ID"):
            continue
        payload.append({
            "id": i,
            "Reference": row.get("AVOMaterialNo"),
            "Case_ID": row.get("Case_ID"),
            "Interval": row.get("Interval"),
            "Difference": row.get("Difference"),
            "Who_Pays": row.get("Who_Pays"),
            "Lever": row.get("Lever"),
            "What_To_Do": row.get("What_To_Do"),
            "Next_Action": row.get("Next_Action"),
        })
        idxs.append(i)

    if not payload:
        return red_sheet_rows

    system_prompt = (
        "You are rewriting decisions for an EDI exception report.\n"
        "STRICT RULES:\n"
        "- Output EXACTLY ONE sentence per row.\n"
        "- Do NOT use bullet points.\n"
        "- Do NOT include alternatives, options, menus, or 'or'.\n"
        "- Do NOT change Who_Pays, Lever, What_To_Do, Next_Action, or Case_ID.\n"
        "- Your sentence must include: payer + lever + action + next action owner.\n"
        "- If Case_ID starts with 'INC_CRITICAL', the sentence must start with 'CRITICAL:'.\n\n"
        "Return strictly valid JSON mapping id -> sentence:\n"
        '{ "0": "sentence", "5": "sentence" }\n'
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload)},
            ],
            temperature=0.0,  # maximize determinism
        )

        content = (resp.choices[0].message.content or "").strip()
        rewritten = _safe_json_loads(content)

        for i in idxs:
            sent = rewritten.get(str(i)) or rewritten.get(i)
            if isinstance(sent, str) and sent.strip():
                red_sheet_rows[i]["Decision_Detail"] = sent.strip()
                red_sheet_rows[i]["Decision_Source"] = "Matrix+AI"

    except Exception as e:
        logging.error(f"AI rewrite failed: {e}")

    return red_sheet_rows

# ========================= FALLBACK (NO AI) =========================

def apply_fallback_if_missing_details(analysis_result):
    """
    Ensures Decision_Detail exists for rows with decisions, even if AI rewrite fails.
    """
    red = analysis_result.get("red_sheet") or []
    for row in red:
        if not row.get("Case_ID"):
            continue
        if row.get("Decision_Detail"):
            continue

        # Deterministic minimal detail
        who = row.get("Who_Pays", "Manual")
        row["Decision_Detail"] = (
            f"Case: {row.get('Case_ID')}"
            f" | Payer: {who}"
            f" | Action: {row.get('What_To_Do')}"
            f" | Next: {row.get('Next_Action')}"
        )
        row["Decision_Source"] = row.get("Decision_Source", "Matrix")

    return analysis_result


# ========================= REPORTING HELPERS =========================

def generate_excel_bytes(analysis_result):
    output = io.BytesIO()

    # 1. Define the Strict Column Mapping (Internal Key -> Excel Header)
    # Ensure these keys (Left side) exist in your data dictionaries (edi_rows/analysis result)
    # If your internal keys are named differently (e.g., 'Old_Qty' vs 'Qty_W1'), update the Left side.
    red_columns_map = {
    "Site": "Site",
    "Weeks": "Week Comparison",
    "ClientCode": "ClientCode",
    "AVOMaterialNo": "AVOMaterialNo",
    "Interval": "Interval",
    "Quantity_W1": "Quantity W1",
    "Quantity_W2": "Quantity W2",
    "Difference": "Difference",
    "Variation": "Variation %",
    "Threshold": "Allowed %",
    "Violation": "Violation",
    "InTransit": "InStock",
    "Line": "Line",
    "WeeklyCapacity": "Weekly Capacity",

    # final decision column
    "Decision": "Decision",
}


    # 2. Define Green Sheet Mapping (Subset of Red, excluding decision columns)
    # We remove the columns that don't apply to "OK" lines
    exclude_green = ["What_To_Do", "Next_Action", "Decision_Summary", "Violation"]
    green_columns_map = {k: v for k, v in red_columns_map.items() if k not in exclude_green}

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        
        # --- RED SHEET (Action Required) ---
        if analysis_result.get("red_sheet"):
            df_red = pd.DataFrame(analysis_result["red_sheet"])
            
            # Filter and Reorder columns based on the map
            # We use .get to avoid crashes if a key is missing (fills with NaN)
            final_red = pd.DataFrame()
            for key, header in red_columns_map.items():
                if key in df_red.columns:
                    final_red[header] = df_red[key]
                else:
                    final_red[header] = None # Create empty column if missing

            final_red.to_excel(writer, sheet_name="Red Sheet (Action)", index=False)
            
            # Optional: Auto-adjust column widths (Visual Polish)
            worksheet = writer.sheets["Red Sheet (Action)"]
            for i, col in enumerate(final_red.columns):
                max_len = max(
                    final_red[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.set_column(i, i, max_len)

        # --- GREEN SHEET (OK) ---
        if analysis_result.get("green_sheet"):
            df_green = pd.DataFrame(analysis_result["green_sheet"])
            
            final_green = pd.DataFrame()
            for key, header in green_columns_map.items():
                if key in df_green.columns:
                    final_green[header] = df_green[key]
                else:
                    final_green[header] = None

            final_green.to_excel(writer, sheet_name="Green Sheet (OK)", index=False)
            
            # Optional: Auto-adjust column widths
            worksheet = writer.sheets["Green Sheet (OK)"]
            for i, col in enumerate(final_green.columns):
                max_len = max(
                    final_green[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.set_column(i, i, max_len)

        # --- SUMMARY SHEET (Optional, usually good to keep) ---
        if analysis_result.get("summary_per_group"):
            df_sum = pd.DataFrame(analysis_result["summary_per_group"])
            df_sum.to_excel(writer, sheet_name="Summary", index=False)

    return output.getvalue()


def compute_reporting_fields(result, weeks):
    all_rows = (result.get("red_sheet") or []) + (result.get("green_sheet") or [])
    
    # This is the global fallback (e.g. W1 vs W4)
    global_week_str = f"{weeks[0]} vs {weeks[-1]}" if len(weeks) > 1 else str(weeks[0])

    for row in all_rows:
        # FIX: Check if the row already has a specific pair-wise comparison
        # In run_edi_analysis, we set "Week_Comparison" as "2025-W01_vs_2025-W02"
        if row.get("Week_Comparison"):
            # Clean up the underscore if desired, or keep it as is
            row["Weeks"] = row["Week_Comparison"].replace("_vs_", " vs ")
        else:
            # Fallback to the global range if specific data is missing
            row["Weeks"] = global_week_str

        # 1. Quantities
        q1 = row.get("Quantity_W1") if row.get("Quantity_W1") is not None else row.get("Old_Qty")
        q2 = row.get("Quantity_W2") if row.get("Quantity_W2") is not None else row.get("New_Qty")
        row["Quantity_W1"] = q1
        row["Quantity_W2"] = q2

        # 2. Variation
        try:
            q1f = float(q1) if q1 else 0.0
            q2f = float(q2) if q2 else 0.0
            row["Variation"] = round(((q2f - q1f) / q1f) * 100.0, 2) if q1f != 0 else (100.0 if q2f > 0 else 0.0)
        except:
            row["Variation"] = 0.0

        # 3. FIXED: Allowed % (Threshold) logic
        threshold_keys = [
            "Threshold", "Allowed_Change_%", "AllowedChangePct", "Allowed_Variation", 
            "Tolerance", "Allowed", "Protocol_Threshold"
        ]
        
        final_thr = None
        for k in threshold_keys:
            val = row.get(k)
            if val is not None:
                final_thr = val
                break
        
        row["Threshold"] = final_thr

        # 4. Stock & Capacity
        row["Available_Stock"] = row.get("Available_Stock") if row.get("Available_Stock") is not None else row.get("InStock")
        row["WeeklyCapacity"] = row.get("WeeklyCapacity") if row.get("WeeklyCapacity") is not None else row.get("Line_Capacity")

    return result



# ========================= ENDPOINT =========================
# Assumes these exist:
# _extract_body, _coerce_list, fetch_ediglobal, fetch_deliverydetails,
# fetch_productdetails_map, analyze_single_week, run_edi_analysis, mail
def ensure_decision_detail_exists(red_sheet_rows):
    """
    If AI fails, generate a deterministic one-sentence Decision_Detail.
    """
    for row in red_sheet_rows or []:
        if not row.get("Case_ID"):
            continue
        if row.get("Decision_Detail"):
            continue

        payer = row.get("Who_Pays", "Manual")
        lever = row.get("Lever", "Escalate")
        # deterministic single-sentence fallback (no menus)
        row["Decision_Detail"] = (
            f"{row.get('Decision_Summary')} (Payer: {payer}; Lever: {lever}; Next: {row.get('Next_Action')})."
        )
    return red_sheet_rows


def finalize_decision_column_for_excel(analysis_result):
    """
    Creates a single final 'Decision' column for Red sheet only,
    combining the precise decision parts into one string.
    Only sets Decision if Case_ID exists (i.e., row needs action).
    """
    red = analysis_result.get("red_sheet") or []
    for row in red:
        if not row.get("Case_ID"):
            # No decision needed for this row
            row.pop("Decision", None)
            continue

        what = (row.get("What_To_Do") or row.get("Decision_Summary") or "").strip()
        payer = (row.get("Who_Pays") or "").strip()
        nxt = (row.get("Next_Action") or "").strip()

        parts = []
        if what:
            parts.append(what)
        if payer:
            parts.append(f"Payer: {payer}")
        if nxt:
            parts.append(f"Next: {nxt}")

        row["Decision"] = " | ".join(parts)

    return analysis_result



@app.route("/send-report", methods=["POST"])
def send_report_endpoint():
    # 1. Route extracts data from the HTTP request
    body = _extract_body() 
    
    # 2. Passes that data to the logic function
    response_data, status_code = trigger_report_logic(body)
    
    # 3. Returns a JSON response to the caller (Browser/Postman/System)
    return jsonify(response_data), status_code


# --- Configuration: Mapping Owners to Client Codes ---
# In a real scenario, fetch this from a DB table like "UserClients"
CLIENT_OWNERS = {
    "mohamedlaith.benmabrouk@avocarbon.com": ["C00409", "C00250", "C00132"], # Valeo Nevers, Poland, etc.
    "chaima.benyahia@avocarbon.com": ["C00260", "C00113", "C00126", "C00409"], # Nidec sites
    "edi.tunisia@avocarbon.com": None # None means "All Clients"
}

def get_consecutive_weeks():
    """Returns [W-1, W] based on current date."""
    today = datetime.now()
    # Current week
    iso_year, iso_week, _ = today.isocalendar()
    current_w = f"{iso_year}-W{iso_week:02d}"
    
    # Previous week
    last_week_date = today - timedelta(days=7)
    lyear, lweek, _ = last_week_date.isocalendar()
    prev_w = f"{lyear}-W{lweek:02d}"
    
    return [prev_w, current_w]





def get_past_weeks(offset_earlier, offset_later):
    """
    Returns a list of two ISO weeks relative to today.
    Example: get_past_weeks(4, 3) returns [W-4, W-3]
    """
    today = datetime.now()
    
    # Calculate the earlier week (e.g., 4 weeks ago)
    date_earlier = today - timedelta(weeks=offset_earlier)
    year_e, week_e, _ = date_earlier.isocalendar()
    iso_earlier = f"{year_e}-W{week_e:02d}"
    
    # Calculate the later week (e.g., 3 weeks ago)
    date_later = today - timedelta(weeks=offset_later)
    year_l, week_l, _ = date_later.isocalendar()
    iso_later = f"{year_l}-W{week_l:02d}"
    
    return [iso_earlier, iso_later]

def scheduled_analysis_job():
    with app.app_context():
        weeks = get_past_weeks(1,0)
        for email, client_list in CLIENT_OWNERS.items():
            report_payload = {
                "forecastWeeks": weeks,
                "clientCodes": client_list,
                "email_recipient": email,
                "use_ai": True
            }
            # This NO LONGER touches the Flask 'request' object
            trigger_report_logic(report_payload)



# Helper to avoid code duplication between API and Scheduler
def trigger_report_logic(data_dict):
    """
    Returns a tuple: (python_dict, status_code)
    """
    try:
        # 1) Use the passed dictionary
        weeks = _coerce_list(data_dict.get("forecastWeeks") or data_dict.get("ediWeekNumbers"))
        client_codes = _coerce_list(data_dict.get("clientCodes") or data_dict.get("ClientCode"))
        product_codes = _coerce_list(data_dict.get("productCodes") or data_dict.get("ProductCode") or data_dict.get("AVOMaterialNo"))
        sites = _coerce_list(data_dict.get("sites") or data_dict.get("Site"))
        recipient_email = data_dict.get("email_recipient")
        use_ai = data_dict.get("use_ai", True)

        if not recipient_email or not weeks:
            return {"status": "error", "message": "email_recipient and forecastWeeks are required"}, 400

        # 2) Run Core Analysis
        edi_rows = fetch_ediglobal(weeks, client_codes, product_codes, sites)
        delivery_rows = fetch_deliverydetails(weeks, product_codes, sites)

        prods_for_meta = product_codes or sorted({
            str(r.get("AVOMaterialNo") or "").strip()
            for r in edi_rows if r.get("AVOMaterialNo")
        })
        product_info = fetch_productdetails_map(prods_for_meta)

        # Normalize weeks
        for r in edi_rows:
            r["ForecastDate"] = norm_week_str(r.get("ForecastDate"))

        # Check actual data availability
        found_data_weeks = sorted({r["ForecastDate"] for r in edi_rows if r.get("ForecastDate")})

        if len(found_data_weeks) < 2:
            logging.warning(f"Requested {len(weeks)} weeks but found data for {len(found_data_weeks)}. Fallback to single-week analysis.")
            result = analyze_single_week(edi_rows, delivery_rows, product_info)
        else:
            result = run_edi_analysis(edi_rows, delivery_rows, product_info)

        # 3) Enrich RED rows
        red_rows = result.get("red_sheet") or []
        for i, r in enumerate(red_rows):
            prim = build_ai_row(i, r)
            for k, v in prim.items():
                r.setdefault(k, v)

        # 4) Deterministic Decisions
        result = apply_matrix_decisions_red_only(result)

        # 5) AI Rewrite
        if use_ai and client and result.get("red_sheet"):
            result["red_sheet"] = rewrite_decisions_with_ai_one_sentence(result["red_sheet"])

        # 6) Finalize
        result = finalize_decision_column_for_excel(result)
        result = compute_reporting_fields(result, weeks)

        # 7) Generate Excel
        excel_bytes = generate_excel_bytes(result)

        # 8) Send Email
        subject_weeks = f"{weeks[0]}" if len(weeks) == 1 else f"{weeks[0]} vs {weeks[-1]}"
        filename = f"EDI_Report_{subject_weeks}.xlsx"
        
        red_count = len(result.get("red_sheet") or [])
        green_count = len(result.get("green_sheet") or [])
        
        html_body = f"""
        <html>
            <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #333333; line-height: 1.6;">
                <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
                    <div style="background-color: #0056b3; color: #ffffff; padding: 20px; text-align: center;">
                        <h2 style="margin: 0;">EDI Analysis Report</h2>
                        <p style="margin: 5px 0 0; font-size: 14px;">Week(s): {subject_weeks}</p>
                    </div>
                    <div style="padding: 20px;">
                        <p>Hello,</p>
                        <p>Please find attached the latest EDI Analysis Report.</p>
                        <div style="background-color: #f8f9fa; border-left: 4px solid #0056b3; padding: 15px; margin: 20px 0;">
                            <h3 style="margin-top: 0; font-size: 16px; color: #0056b3;">Executive Summary</h3>
                            <ul style="padding-left: 20px; margin-bottom: 0;">
                                <li style="margin-bottom: 8px;">
                                    <strong style="color: #d9534f;">Red Sheet (Action Required):</strong> {red_count} lines
                                </li>
                                <li>
                                    <strong style="color: #28a745;">Green Sheet (OK):</strong> {green_count} lines
                                </li>
                            </ul>
                        </div>
                    </div>
                </div>
            </body>
        </html>
        """

        msg = Message(
            subject=f"EDI Report ({subject_weeks}) - {red_count} Exceptions",
            recipients=[recipient_email],
            cc=["edi.tunisia@avocarbon.com"],
            html=html_body
        )

        msg.attach(
            filename,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            excel_bytes
        )

        mail.send(msg)

        # Return a Python DICT, not a jsonify object
        return {
            "status": "success",
            "message": f"Report sent to {recipient_email}",
            "filename": filename
        }, 200

    except Exception as e:
        logging.exception("Error in trigger_report_logic")
        # Return a Python DICT for error too
        return {"status": "error", "message": str(e)}, 500
    
# ========================= DATA RETRIEVAL APIS =========================

@app.route("/get-delivery-details", methods=["POST"])
def get_delivery_details():
    """Retrieves records from DeliveryDetails with optional filters."""
    try:
        body = _extract_body()
        sites = _coerce_list(body.get("sites"))
        avo_materials = _coerce_list(body.get("AVOMaterialNo"))
        statuses = _coerce_list(body.get("statuses"))

        sql = 'SELECT * FROM public."DeliveryDetails" WHERE 1=1'
        params = []

        if sites:
            sql += ' AND "Site" = ANY(%s)'
            params.append(sites)
        if avo_materials:
            sql += ' AND "AVOMaterialNo" = ANY(%s)'
            params.append(avo_materials)
        if statuses:
            sql += ' AND "Status" = ANY(%s)'
            params.append(statuses)

        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.close()

        return jsonify({"status": "success", "count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get-edi-global", methods=["POST"])
def get_edi_global():
    """Retrieves records from EDIGlobal, typically filtered by ForecastDate."""
    try:
        body = _extract_body()
        forecast_weeks = _coerce_list(body.get("forecastWeeks"))
        client_codes = _coerce_list(body.get("clientCodes"))
        avo_materials = _coerce_list(body.get("AVOMaterialNo"))

        sql = 'SELECT * FROM public."EDIGlobal" WHERE 1=1'
        params = []

        if forecast_weeks:
            sql += ' AND "ForecastDate" = ANY(%s)'
            params.append(forecast_weeks)
        if client_codes:
            sql += ' AND "ClientCode" = ANY(%s)'
            params.append(client_codes)
        if avo_materials:
            sql += ' AND "AVOMaterialNo" = ANY(%s)'
            params.append(avo_materials)

        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.close()

        return jsonify({"status": "success", "count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get-product-details", methods=["GET", "POST"])
def get_product_details_route():
    """
    GET: Returns all products.
    POST: Returns specific products via AVOMaterialNo list.
    """
    try:
        sql = 'SELECT * FROM public."ProductDetails"'
        params = []

        if request.method == "POST":
            body = _extract_body()
            avo_materials = _coerce_list(body.get("AVOMaterialNo"))
            if avo_materials:
                sql += ' WHERE "AVOMaterialNo" = ANY(%s)'
                params.append(avo_materials)

        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.close()

        return jsonify({"status": "success", "count": len(rows), "data": rows}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500




# --- ESCALATION HIERARCHY ---
# Define the email addresses for each escalation level
ESCALATION_CONTACTS = {
    "LogisticManager": "chaima.benyahia@avocarbon.com",
    "GeneralManager": "chaima.benyahia@avocarbon.com",
    "CEO": "chaima.benyahia@avocarbon.com"
}

# Map Client Codes to their specific Customer Service Rep (CS)
# You can reuse CLIENT_OWNERS or define a specific map for alerts
# Map Client Codes to their specific Customer Service Rep (CS)
CLIENT_CS_MAP = {
    # Existing & Updated Mappings
    "C00409": "mohamedlaith.benmabrouk@avocarbon.com", # Valeo Nevers
    "C00250": "mohamedlaith.benmabrouk@avocarbon.com", # Valeo Poland / Pologne
    
    # New Mappings
    "C00126": "mohamedlaith.benmabrouk@avocarbon.com", # Nidec Pologne
    "C00050": "mohamedlaith.benmabrouk@avocarbon.com", # Nidec ESP
    "C00113": "mohamedlaith.benmabrouk@avocarbon.com", # Nidec DCK
    "C00285": "mohamedlaith.benmabrouk@avocarbon.com", # Pierburg
    "C00241": "mohamedlaith.benmabrouk@avocarbon.com", # Inteva GAD
    "C00132": "mohamedlaith.benmabrouk@avocarbon.com", # Valeo Betigheim
    "C00125": "mohamedlaith.benmabrouk@avocarbon.com", # Valeo Madrid
    "C00303": "mohamedlaith.benmabrouk@avocarbon.com", # Valeo Mexique
    "C00410": "mohamedlaith.benmabrouk@avocarbon.com", # Inteva Esson
    "C00072": "mohamedlaith.benmabrouk@avocarbon.com" # Valeo Brasil
}


CLIENT_NAMES = {
    "C00126": "Nidec Pologne",
    "C00050": "Nidec ESP",
    "C00113": "Nidec DCK",
    "C00260": "Nidec Inde",
    "C00285": "Pierburg",
    "C00241": "Inteva GAD",
    "C00132": "Valeo Betigheim",
    "C00125": "Valeo Madrid",
    "C00303": "Valeo Mexique",
    "C00410": "Inteva Esson",
    "C00250": "Valeo Pologne",
    "C00072": "Valeo Brasil",
    "C00409": "Valeo Nevers"
}


def check_edi_compliance_job():
    """
    Checks all clients mapped in CLIENT_CS_MAP.
    Identifies EXACTLY which weeks are missing for each client.
    Groups results by CS Email and sends a consolidated report.
    """
    with app.app_context():
        logging.info("--- STARTING DETAILED COMPLIANCE CHECK ---")
        
        # 1. Prepare Week List (Current + 3 previous)
        # We check these 4 specific weeks for presence
        today = datetime.now()
        weeks_to_check = []
        for i in range(15):
            d = today - timedelta(weeks=i)
            iso_year, iso_week, _ = d.isocalendar()
            weeks_to_check.append(f"{iso_year}-W{iso_week:02d}")
        
        # Structure: { 'email@avo.com': [ {'client': 'C001', 'missing_weeks': ['2026-W02', ...], 'streak': 2}, ... ] }
        violations_by_email = defaultdict(list)

        conn = get_pg_connection()
        try:
            with conn.cursor() as cur:
                for client_code, cs_email in CLIENT_CS_MAP.items():
                    
                    # 2. Check existence for the last 4 weeks
                    cur.execute("""
                        SELECT DISTINCT "ForecastDate" 
                        FROM public."EDIGlobal" 
                        WHERE "ClientCode" = %s AND "ForecastDate" = ANY(%s)
                    """, (client_code, weeks_to_check))
                    
                    found_weeks = {row[0] for row in cur.fetchall()}

                    # 3. Identify Missing Weeks & Calculate Streak
                    missing_weeks = []
                    current_streak = 0
                    streak_broken = False

                    # Iterate from newest to oldest
                    for w in weeks_to_check:
                        if w not in found_weeks:
                            missing_weeks.append(w)
                            if not streak_broken:
                                current_streak += 1
                        else:
                            streak_broken = True # Data found, so the consecutive "missing" streak stops here
                    
                    # If there are ANY missing weeks, record the violation
                    if missing_weeks:
                        violations_by_email[cs_email].append({
                            "client_code": client_code,
                            "client_name": CLIENT_NAMES.get(client_code, "Unknown Client"),
                            "missing_weeks": missing_weeks,
                            "streak": current_streak # Streak determines escalation level
                        })

            # 4. Send Consolidated Emails
            for email, violation_list in violations_by_email.items():
                if violation_list:
                    # Sort list so clients with highest streak appear first
                    violation_list.sort(key=lambda x: x['streak'], reverse=True)
                    send_detailed_escalation_email(email, violation_list, weeks_to_check[0])

        except Exception as e:
            logging.exception("Error in consolidated compliance check")
        finally:
            conn.close()


def send_detailed_escalation_email(cs_email, violation_list, current_week):
    """
    Sends a table listing Client Name, Code, and the specific Missing Weeks.
    Managers are added to CC based on the new timeline (GM at W4-5, CEO at W6).
    """
    # 1. Determine Escalation Level based on the worst streak
    max_streak = max(item['streak'] for item in violation_list)

    # RECIPIENT: Only the CS person is in the "To" field
    recipients = [cs_email]
    
    # CC LIST: Starts with Admin
    cc_list = ["edi.tunisia@avocarbon.com"] 
    
    subject = ""
    severity_color = ""
    escalation_msg = ""

    # --- NEW ESCALATION TIMELINE ---
    
    if max_streak <= 1:
        # Week 1: Reminder Only
        subject = f"REMINDER: Missing EDI Files (Week {current_week})"
        severity_color = "#f0ad4e" # Orange
        escalation_msg = "Reminder (CS Only)"
    
    elif 2 <= max_streak <= 3:
        # Weeks 2 & 3: CC Logistic Manager
        cc_list.append(ESCALATION_CONTACTS["LogisticManager"])
        
        subject = f"ESCALATION L1: Missing EDI Files ({max_streak} Weeks)"
        severity_color = "#d9534f" # Red
        escalation_msg = "Level 1 (Logistic Manager in CC)"

    elif 4 <= max_streak <= 5:
        # Weeks 4 & 5: CC General Manager (+ Logistic Manager)
        
        cc_list.append(ESCALATION_CONTACTS["GeneralManager"])
        cc_list.append(ESCALATION_CONTACTS["LogisticManager"])
        
        subject = f"ESCALATION L2: Missing EDI Files ({max_streak} Weeks)"
        severity_color = "#c9302c" # Dark Red
        escalation_msg = "Level 2 (General Manager in CC)"

    elif max_streak >= 6:
        # Week 6+: CC CEO (+ GM & Logistic Manager)
        
        
        cc_list.append(ESCALATION_CONTACTS["CEO"])
        cc_list.append(ESCALATION_CONTACTS["GeneralManager"])
        cc_list.append(ESCALATION_CONTACTS["LogisticManager"])
        
        subject = f"CRITICAL: EDI PROCESS FAILURES ({max_streak} Weeks)"
        severity_color = "#000000" # Black
        escalation_msg = "CRITICAL (CEO in CC)"

    # Remove duplicates from CC list
    cc_list = list(set(cc_list))

    # 2. Build Table Rows
    table_rows = ""
    for item in violation_list:
        code = item['client_code']
        name = item['client_name']
        missing_weeks_str = ", ".join(item['missing_weeks'])
        streak = item['streak']
        
        # Color the row red if it's a serious streak (2+ weeks), else standard
        row_style = "color: red;" if streak >= 2 else ""
        
        table_rows += f"""
        <tr>
            <td style="padding: 10px; border: 1px solid #ddd;"><b>{name}</b></td>
            <td style="padding: 10px; border: 1px solid #ddd;">{code}</td>
            <td style="padding: 10px; border: 1px solid #ddd; {row_style}">
                {missing_weeks_str}
            </td>
        </tr>
        """

    # 3. Email Body
    html_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="border-top: 5px solid {severity_color}; padding: 20px; background-color: #f9f9f9;">
                <h2 style="color: {severity_color}; margin-top: 0;">{subject}</h2>
                <p>Hello,</p>
                <p>The following clients have missing EDI files.</p>
                <p><b>Status:</b> {escalation_msg}</p>
                
                <table style="width: 100%; border-collapse: collapse; margin-top: 20px; background-color: white; font-size: 14px;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Client Name</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Code</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Missing Weeks</th>
                    </tr>
                    {table_rows}
                </table>

                <p style="margin-top: 20px;"><strong>Action Required:</strong> Please upload the missing files immediately.</p>
                <p style="font-size: 12px; color: #777;">Automated EDI Compliance System</p>
            </div>
        </body>
    </html>
    """

    try:
        msg = Message(subject=subject, recipients=recipients, cc=cc_list, html=html_body)
        mail.send(msg)
        logging.info(f"Detailed email sent to {cs_email} (Streak: {max_streak}, CC Count: {len(cc_list)})")
    except Exception as e:
        logging.error(f"Failed to send detailed email to {cs_email}: {e}")



# ========================= SCHEDULER SETUP (MODULE LEVEL) =========================

def init_scheduler():
    # 1. Use a database connection to try and get an advisory lock
    conn = get_pg_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    # We use a random ID (e.g., 999) to represent the "Scheduler Lock"
    cur.execute("SELECT pg_try_advisory_lock(999);")
    is_master_worker = cur.fetchone()[0]

    if not is_master_worker:
        # This worker didn't get the lock, it remains silent
        return None

    # 2. Only the "Master" worker proceeds to setup the Cron
    tunisia_tz = pytz.timezone('Africa/Tunis')
    scheduler = BackgroundScheduler(timezone=tunisia_tz)
    
    # Tuesday Cron
    scheduler.add_job(
        func=scheduled_analysis_job,
        trigger='cron', day_of_week='tue', hour=14, minute=29,
        id='tuesday_analysis'
    )
    
    # Friday Cron
    scheduler.add_job(
        func=scheduled_analysis_job,
        trigger='cron', day_of_week='fri', hour=14, minute=0,
        id='friday_analysis'
    )
    
    scheduler.add_job(
        func=check_edi_compliance_job,
        trigger='cron', day_of_week='tue', hour=14, minute=28,
        id='compliance_check', name='EDI Compliance Check'
    )

    scheduler.start()
    app.logger.info("✅ CRON SCHEDULER STARTED ON MASTER WORKER")
    return scheduler


# ========================= START SCHEDULER =========================
# This runs when the module is imported (works with Gunicorn/Azure)

app_scheduler = None

# 1. Gunicorn Guard: Only start in one worker
if 'gunicorn' in os.environ.get('SERVER_SOFTWARE', '').lower():
    if not os.environ.get('SCHEDULER_STARTED'):
        try:
            app_scheduler = init_scheduler()
            if app_scheduler:
                os.environ['SCHEDULER_STARTED'] = '1'
        except Exception as e:
            app.logger.error(f"Failed to start scheduler: {e}")

# 2. Local Dev Guard: Prevent Werkzeug double-start
elif os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    app_scheduler = init_scheduler()








@app.route("/test-scheduler", methods=["GET"])
def test_scheduler():
    """Check if scheduler is running and list jobs"""
    try:
        if 'app_scheduler' in globals():
            jobs = app_scheduler.get_jobs()
            return jsonify({
                "status": "running",
                "jobs": [{"id": j.id, "name": j.name, "next_run": str(j.next_run_time)} for j in jobs]
            }), 200
        else:
            return jsonify({"status": "error", "message": "Scheduler not initialized"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route("/trigger-compliance-check", methods=["POST"])
def trigger_compliance_check():
    """Manually trigger the compliance check"""
    try:
        app.logger.info("Manual compliance check triggered via API")
        check_edi_compliance_job()
        return jsonify({"status": "success", "message": "Compliance check completed"}), 200
    except Exception as e:
        app.logger.exception("Error in manual compliance trigger")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========================= MAIN BLOCK (FOR LOCAL DEV ONLY) =========================
if __name__ == "__main__":
    # This block ONLY runs when you execute: python App.py
    # It does NOT run in Azure/Gunicorn
    app.logger.info("Running in development mode (python App.py)")
    app.run(host='0.0.0.0', port=5001, debug=True)

