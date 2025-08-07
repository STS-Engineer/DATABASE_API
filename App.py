import base64
import csv
import io
import logging
import fitz  # PyMuPDF
from flask import Flask, request, jsonify
import psycopg2
import pdfplumber
import PyPDF2
from psycopg2 import errors
from datetime import datetime ,date
import re
from collections import defaultdict
import os
app = Flask(__name__)

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

def detect_company_and_prepare(rows):
    if not rows:
        return None, []
    header = rows[0]
    if 'Org_Name_Customer' in header:
        org_col = header.index('Org_Name_Customer')
        if any(r[org_col].strip() == 'Valeo' for r in rows[1:] if len(r) > org_col):
            return 'Valeo', header
    if 'Site/Building' in header:
        site_col = header.index('Site/Building')
        if any(r[site_col].strip() in ['ESS2', 'GAD1'] for r in rows[1:] if len(r) > site_col):
            return 'Inteva', header
    if 'Plant' in header:
        plant_col = header.index('Plant')
        if any(r[plant_col].strip() in ['BI01', 'ZI01', 'SPER'] for r in rows[1:] if len(r) > plant_col):
            return 'Nidec', header
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
    processed = []
    idx = {col: header.index(col) for col in header}
    for row in rows[1:]:
        try:
            if 'Customer_No' in idx and not row[idx['Customer_No']].isnumeric():
                continue
            plant = row[idx['Plant_No']].strip()
            client_code = plant_to_client.get(plant, None)
            if not client_code:
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
            material_code = row[idx['Material_No_Customer']].strip()
            if not material_code.startswith("V"):
                AVOmaterial_code = "V" + material_code
            if client_code == "C00250":
                AVOmaterial_code = AVOmaterial_code + "POL"
            elif client_code == "C00303":
                AVOmaterial_code = AVOmaterial_code + "SLP"
            else : 
                AVOmaterial_code = AVOmaterial_code
            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": to_forecast_week(delivery_date),
                "DateUntil": to_forecast_week(delivery_date),
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






def process_nidec_rows(rows, header):
    plant_to_client = {
        "ZI01": "C00126",
        "SPER": "C00050",
        "BI01": "C00113",
    }
    processed = []
    idx = {col: header.index(col) for col in header}

    def make_avo_material_code(code):
        code = code.strip()
        # If contains any letter, just add V in front
        if re.search(r'[a-zA-Z]', code):
            return "V" + code
        # Else, expect format like 503-996-99-99, take first two parts
        parts = code.split('-')
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            return f"V{parts[0]}.{parts[1]}"
        # Fallback: just add V
        return "V" + code
    # Check all required fields exist
    required_fields = ['Plant', 'CallOffDate', 'Material', 'DateFrom', 'DateUntil', 'DespatchQty', 'LastDeliveryDate', 'LastDeliveryQuantity', 'CumQuantity', 'Status', 'LastDeliveryNo']
    for f in required_fields:
        if f not in idx:
            logging.error(f"Column missing in Nidec CSV: {f}")
            return []  # Exit early, can't process
    for i, row in enumerate(rows[1:], 1):
        if len(row) != len(header):
            logging.warning(f"Row length mismatch at row {i}: {len(row)} fields vs header {len(header)}. Row: {row}")
            continue
    for i, row in enumerate(rows[1:]):
        try:
            if len(row) < len(header):
                row += [""] * (len(header) - len(row))
            plant = row[idx['Plant']].strip()
            if plant not in plant_to_client:
                logging.warning(f"SKIP row {i+1}: Plant '{plant}' not recognized in plant_to_client")
                continue
            client_code = plant_to_client.get(plant, None)
            if not client_code:
                logging.warning(f"SKIP row {i+1}: No client_code for plant '{plant}'")
                continue
            call_off_date_str = row[idx['CallOffDate']].strip()
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
            # All OK - continue with record as before
            week_num = date_obj.isocalendar()[1]
            if date_obj.weekday() > 1:
                week_num += 1
            forecast_date = f"{date_obj.year}-W{week_num:02d}"
            material_code = row[idx['Material']].strip()
            avo_material_code = make_avo_material_code(material_code)
            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,      # as is from the file
                "AVOMaterialNo": avo_material_code,     # transformed as per rule
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
    is_pdf = file_type == "pdf" or file_name.lower().endswith('.pdf')

    try:
        file_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

    extracted_records = []
    company = None
    header = None

    if is_pdf:
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

    success_count, error_details = save_to_postgres_with_conflict_reporting(extracted_records)
    return jsonify({
        "message": "Processing completed.",
        "file_processed": file_name,
        "company_detected": company,
        "records_processed": len(extracted_records),
        "records_inserted": success_count,
        "records_failed": len(error_details),
        "errors": error_details
    }), (200 if success_count > 0 else 400)






@app.route("/detect-client-info", methods=["POST"])
def detect_client_info():
    data = request.get_json()
    required_keys = ['file_name', 'file_content_base64']
    if not data or not all(k in data for k in required_keys):
        missing_keys = [k for k in required_keys if k not in data]
        return jsonify({"error": f"Missing keys in request body: {', '.join(missing_keys)}"}), 400

    file_name = data['file_name']
    file_content_base64 = data['file_content_base64']
    is_pdf = file_name.lower().endswith('.pdf')

    try:
        file_bytes = base64.b64decode(file_content_base64)
    except Exception as e:
        logging.error(f"Failed to decode Base64 string for file {file_name}: {e}")
        return jsonify({"error": f"Invalid Base64 content. Detail: {e}"}), 400

    extracted_records = []
    client_code = None
    company_name = None

    # PDF HANDLING
    if is_pdf:
        pdf_format = detect_pdf_format(file_bytes)
        if not pdf_format:
            return jsonify({"error": "Unknown or unsupported PDF format."}), 400

        # Run only enough of each parser to get client code
        if pdf_format == "valeo_campinas":
            extracted_records = process_valeo_campinas_pdf(file_bytes, file_name)
        elif pdf_format == "valeo_nevers":
            extracted_records = process_valeo_nevers_pdf(file_bytes, file_name)
        elif pdf_format == "pierburg":
            extracted_records = process_pierburg_pdf(file_bytes, file_name)
        elif pdf_format == "nidec":
            extracted_records = process_nidec_pdf(file_bytes, file_name)
        else:
            return jsonify({"error": "Unrecognized PDF format."}), 400

    # CSV HANDLING
    else:
        try:
            csv_text = decode_and_clean_csv(file_content_base64)
            csv_io = io.StringIO(csv_text)
            rows = list(csv.reader(csv_io, delimiter=';'))
            header = [col.strip() for col in rows[0]]
            rows_cleaned = [[cell.strip() for cell in row] for row in rows]
            rows_cleaned[0] = header
            rows = rows_cleaned
        except Exception as e:
            return jsonify({"error": f"CSV decoding failed: {e}"}), 400

        company, header = detect_company_and_prepare(rows)
        if company == "Valeo":
            extracted_records = process_valeo_rows(rows, header)
        elif company == "Inteva":
            extracted_records = process_inteva_rows(rows, header)
        elif company == "Nidec":
            extracted_records = process_nidec_rows(rows, header)
        else:
            return jsonify({"error": "Unrecognized company type."}), 400

    # Extract client code from first record
    if extracted_records:
        client_code = extracted_records[0].get("ClientCode", None)

    # Map client code to company name
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
    company_name = code_to_company.get(client_code, "Unknown")

    forecast_date = None
    if extracted_records:
        forecast_date = extracted_records[0].get("ForecastDate")

    # Sanitize for filename (YYYY-WXX or fallback)
    safe_forecast = forecast_date if forecast_date else "unknown_week"

    # Build suggested filename
    suggested_name = f"{company_name.replace(' ', '_')}_edi_{safe_forecast}".lower()

    return jsonify({
        "file_processed": file_name,
        "client_code": client_code,
        "company_detected": company_name,
        "forecast_week": forecast_date,
        "records_detected": len(extracted_records),
        "suggested_filename": suggested_name
    }), 200







if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001,debug=True)
