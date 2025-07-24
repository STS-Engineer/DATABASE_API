import base64
import csv
import io
import logging
import fitz  # PyMuPDF
from flask import Flask, request, jsonify
import psycopg2
import pdfplumber
from psycopg2 import errors
from datetime import datetime
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
            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": delivery_date,
                "DateUntil": delivery_date,
                "Quantity": int(row[idx['Despatch_Qty']].strip() or 0),
                "ForecastDate": forecast_date,
                "LastDeliveryDate": row[idx['Last_Delivery_Note_Date']].strip(),
                "LastDeliveredQuantity": int(row[idx['Last_Delivery_Quantity']].strip() or 0),
                "CumulatedQuantity": int(row[idx['Cum_Quantity']].strip() or 0),
                "EDIStatus": row[idx['Commitment_Level']].strip(),
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
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d/%m/%y"):
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
        
            processed.append({
                "Site": "Tunisia",
                "ClientCode": client_code,
                "ClientMaterialNo": material_code,
                "AVOMaterialNo": AVOmaterial_code,
                "DateFrom": row[idx['Due Date']].strip(),
                "DateUntil": row[idx['Due Date']].strip(),
                "Quantity": qty,
                "ForecastDate": forecast_date,
                "LastDeliveryDate": last_receipt_date,
                "LastDeliveredQuantity": parse_euro_number(row[idx['Last Receipt Quantity']].strip()),
                "CumulatedQuantity": cumulated,
                "EDIStatus": row[idx['Release Status']].strip(),
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
                "DateFrom": row[idx['DateFrom']].strip(),
                "DateUntil": row[idx['DateUntil']].strip(),
                "Quantity": parse_euro_number(row[idx['DespatchQty']].strip()),
                "ForecastDate": forecast_date,
                "LastDeliveryDate": row[idx['LastDeliveryDate']].strip(),
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
        # Combine all text (if you want to search whole doc)
        all_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
        lines = all_text.splitlines()
        
        # 1. Nidec: First line, first word
        for line in lines[:10]:
            if "NIDEC" in line.upper():
                return "nidec"
        
        # 2. Pierburg: Look for "Organization: PIERB"
        if any("Organization: PIERB" in line for line in lines):
            return "pierburg"
        
        # 3. Valeo: Look for "DELIVERY IN: VWS Campinas" or "DELIVERY IN: VALEO CIE Nevers"
        if "DELIVERY IN:" in line:
            value = line.split("DELIVERY IN:")[1].strip()
            if value.startswith("VWS Campinas"):
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
            results.append({
                "Site": "Tunisia",
                "ClientCode": "C00285",
                "ClientMaterialNo": material_customer,
                "AVOMaterialNo": f"V{material_customer}",
                "DateFrom": delivery_date,
                "DateUntil": delivery_date,
                "Quantity": parse_euro_number(dispatch_qty),
                "ForecastDate": forecast_date,
                "LastDeliveryNo": last_delivery_no,
                "LastDeliveryDate": last_delivery_date,
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
                "DateFrom": req_date,
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





def process_valeo_campinas_pdf(file_bytes, file_name):
    
    data = []
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    site = "Tunisia"
    client_code = "C00072"
    product_name = None

    # 1. Extract ForecastDate
    full_text = "\n".join([page.get_text() for page in doc])
    logging.warning(f"[VALEO CAMPINAS] Extracted full_text (first 500 chars):\n{full_text[:500]}")
    forecast_match = re.search(r"DATE[: ]+(\d{2}/\d{2}/\d{4})", full_text)
    if forecast_match:
        logging.warning(f"[VALEO CAMPINAS] Matched forecast date string: {forecast_match.group(1)}")
        dt = parse_date_flexible(forecast_match.group(1))
        logging.warning(f"[VALEO CAMPINAS] Parsed forecast date to: {dt}")
        if dt:
            week_num = dt.isocalendar()[1]
            if dt.weekday() > 1:  # Not Mon/Tue
                week_num += 1
            forecast_date = f"{dt.year}-W{week_num:02d}"
            logging.warning(f"[VALEO CAMPINAS] Final forecast_date: {forecast_date}")
        else:
            forecast_date = forecast_match.group(1)
    else:
        logging.error("[VALEO CAMPINAS] Forecast date pattern not found!")
        forecast_date = ""

    last_client_mat = None
    last_delivery_date = None
    last_delivery_qty = None
    last_delivery_no = None

    for idx, page in enumerate(doc):
        text = page.get_text()
        logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] Text (first 300 chars): {text[:300]}")

        # Extract client material number
        material_match = re.search(r"Material\s+([A-Z0-9]+)", text)
        if material_match:
            last_client_mat = material_match.group(1)
            logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] Matched ClientMaterialNo: {last_client_mat}")
        else:
            logging.error(f"[VALEO CAMPINAS][PAGE {idx}] No ClientMaterialNo found.")

        # Extract LAST DELIVERY block
        delivery_match = re.search(
            r"LAST DELIVERY\s+DEL DATE\s+DOCUMENT\s+QUANTITY\s+(\d{2}/\d{2}/\d{4})\s+(\S+)\s+([\d.,]+)", text)
        if delivery_match:
            last_delivery_date = parse_date_flexible(delivery_match.group(1))
            last_delivery_no = delivery_match.group(2)
            last_delivery_qty = pars_euro_number(delivery_match.group(3))
            logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] LastDeliveryDate: {last_delivery_date}, LastDeliveryNo: {last_delivery_no}, LastDeliveredQty: {last_delivery_qty}")
        else:
            logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] No LAST DELIVERY block found.")

        if not last_client_mat:
            logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] Skipping page due to missing client mat.")
            continue

        avo_mat = f"V{last_client_mat}"

        # Find all EDI schedule lines
        delivery_lines = re.findall(
            r"(PAST DUE|FIRM AUTHORIZED SHIPPMENTS|PLANNED SHIPPMENTS|FORECAST)\s+([0-9/]+)[D]?\s+([\d.,]+)\s+([\d.,]+)",
            text
        )
        logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] Found {len(delivery_lines)} delivery lines.")

        for status, date_str, qty, cum in delivery_lines:
            parsed_date = parse_date_flexible(date_str)
            if not parsed_date:
                logging.error(f"[VALEO CAMPINAS][PAGE {idx}] Could not parse date {date_str} for status {status}")
                continue

            qty_val = pars_euro_number(qty)
            cum_val = pars_euro_number(cum)
            logging.warning(f"[VALEO CAMPINAS][PAGE {idx}] EDIStatus: {status}, DateFrom: {parsed_date.strftime('%Y-%m-%d')}, Qty: {qty_val}, Cum: {cum_val}")

            data.append({
                "Site": site,
                "ClientCode": client_code,
                "ClientMaterialNo": last_client_mat,
                "AVOMaterialNo": avo_mat,
                "DateFrom": parsed_date.strftime("%Y-%m-%d"),
                "DateUntil": parsed_date.strftime("%Y-%m-%d"),
                "Quantity": int(qty_val),
                "ForecastDate": forecast_date,
                "CumulatedQuantity": int(cum_val),
                "EDIStatus": status,
                "LastDeliveryDate": last_delivery_date.strftime("%Y-%m-%d") if last_delivery_date else "",
                "LastDeliveredQuantity": int(last_delivery_qty) if last_delivery_qty is not None else None,
                "LastDeliveryNo": last_delivery_no,
                "ProductName": product_name
            })

    logging.warning(f"[VALEO CAMPINAS] Returning {len(data)} extracted rows.")
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
        #    extracted_records = process_valeo_nevers_pdf(file_bytes, file_name)
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



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001,debug=True)
