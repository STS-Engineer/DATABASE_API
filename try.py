def process_valeo_pdf(pdf_bytes: io.BytesIO, customer_code: str, customer_name: str, site="Tunisia"):
# --- Extract ForecastDate with multiple patterns
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = "".join(page.get_text() for page in doc)
    except Exception as e:
        logging.error(f"Failed to parse PDF file with PyMuPDF: {e}")
        raise ValueError(f"Failed to parse PDF file: {e}")
    forecast_date = None
        
        # Pattern 1: Original pattern (Date d'impression)
    forecast_match = re.search(
        r"Date d['']impression\s*:\s*(\d{1,2})\s+(janv\.|févr\.|mars|avr\.|mai|juin|juil\.|août|sept\.|oct\.|nov\.|déc\.)\s+(\d{4})",
        text,
        re.IGNORECASE
    )
        
    if forecast_match:
        day, month_abbr, year = forecast_match.groups()
        month_map = {
            "janv.": "01", "févr.": "02", "mars": "03", "avr.": "04", "mai": "05", "juin": "06",
            "juil.": "07", "août": "08", "sept.": "09", "oct.": "10", "nov.": "11", "déc.": "12"
        }
        forecast_date = f"{int(day):02d}.{month_map[month_abbr.lower()]}.{year}"
    else:
        # Pattern 2: Try without colon
        forecast_match = re.search(
            r"Date d['']impression\s*(\d{1,2})\s+(janv\.|févr\.|mars|avr\.|mai|juin|juil\.|août|sept\.|oct\.|nov\.|déc\.)\s+(\d{4})",
            text,
            re.IGNORECASE
        )
            
        if forecast_match:
            day, month_abbr, year = forecast_match.groups()
            month_map = {
                "janv.": "01", "févr.": "02", "mars": "03", "avr.": "04", "mai": "05", "juin": "06",
                "juil.": "07", "août": "08", "sept.": "09", "oct.": "10", "nov.": "11", "déc.": "12"
            }
            forecast_date = f"{int(day):02d}.{month_map[month_abbr.lower()]}.{year}"
        else:
            # Pattern 3: Try with English "Print date" or "Date"
            forecast_match = re.search(
                r"(?:Print date|Date)\s*:?\s*(\d{1,2})\s+(janv\.|févr\.|mars|avr\.|mai|juin|juil\.|août|sept\.|oct\.|nov\.|déc\.)\s+(\d{4})",text,
                re.IGNORECASE
            )
                
            if forecast_match:
                day, month_abbr, year = forecast_match.groups()
                month_map = {
                    "janv.": "01", "févr.": "02", "mars": "03", "avr.": "04", "mai": "05", "juin": "06",
                    "juil.": "07", "août": "08", "sept.": "09", "oct.": "10", "nov.": "11", "déc.": "12"
                }
                forecast_date = f"{int(day):02d}.{month_map[month_abbr.lower()]}.{year}"
            else:
                # Pattern 4: Try to find any date in DD.MM.YYYY format
                date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", text)
                if date_match:
                    forecast_date = date_match.group(1)
                    print(f"Warning: Using first found date as forecast date: {forecast_date}")
                else:
                    # Pattern 5: Try to find date in DD/MM/YYYY format
                    date_match = re.search(r"(\d{2}/\d{2}/\d{4})", text)
                    if date_match:
                        forecast_date = date_match.group(1).replace('/', '.')
                        print(f"Warning: Using first found date as forecast date: {forecast_date}")

    if not forecast_date:
         # Show all possible date patterns found in the text for debugging
        print("Debug: Searching for date patterns in the text...")
            
        # Search for "Date d'impression" variants
        date_impression_matches = re.findall(r"Date d['']impression.*?(\d{1,2}.*?\d{4})", text, re.IGNORECASE)
        if date_impression_matches:
            print(f"Found 'Date d'impression' patterns: {date_impression_matches}")
            
        # Search for any date-like patterns
        all_dates = re.findall(r"(\d{1,2}[./\-\s]+(?:janv\.|févr\.|mars|avr\.|mai|juin|juil\.|août|sept\.|oct\.|nov\.|déc\.|\d{1,2})[./\-\s]+\d{4})", text, re.IGNORECASE)
        if all_dates:
            print(f"Found date patterns: {all_dates}")
            
        # Search for lines containing "Date"
        date_lines = [line.strip() for line in text.split('\n') if 'date' in line.lower()]
        if date_lines:
            print(f"Lines containing 'date': {date_lines[:5]}")  # Show first 5 lines
            
        raise ValueError("ForecastDate (Date d'impression) could not be extracted. Please check the PDF content and the debug output above.")


    # 3. Material mappings
    material_map = {
        "1023093": "190313",
        "1023645": "191663",
        "1026188": "187144",
        "1026258": "194470",
        "1026540": "202066",
        "1026629": "214188"
    }
    reverse_map = {v: k for k, v in material_map.items()}

    sections = re.split(r"N° de référence:\s+", text)[1:]
    results = []

    for section in sections:
        # Extract delivery info from current section only
        last_deliv_match = re.search(r"13/06/2025.*?Date:\s*(\d{2}\.\d{2}\.\d{4}).*?Qté:\s*(\d+)", section, re.DOTALL)
        last_delivery_date = last_deliv_match.group(1) if last_deliv_match else None
        last_delivered_qty = int(last_deliv_match.group(2)) if last_deliv_match else None

        section = re.sub(r"Date d['']impression:\s*\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}", "", section)
        section = re.sub(r"Page:\s*\d+\s+von\s+\d+", "", section)

        # article (optional, for trace/debug/future use)
        article_match = re.search(r"Désignation:\s*(.*?)\s+Remplace", section)
        article = article_match.group(1).strip() if article_match else "UNKNOWN"

        mat_desc_match = re.search(
            r"Materialbeschreibung\s+\(Kunde\):\s*(.*?)\s+(?:Remplace|Sachnr\.|Liefer|\d{2}\.\d{2}\.\d{4})",
            section, re.DOTALL
        )
        material_description = mat_desc_match.group(1).strip() if mat_desc_match else article

        # Valeo logic
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

        # Daily and week-based deliveries
        delivery_lines = re.findall(
            r"((?:\d{2}\.\d{2}\.\d{4})|(?:20\d{2}\s*w\d{2}\s*-\s*20\d{2}\s*w\d{2}))\s+(\d+)\s+(\d+)",
            section
        )
        for date_or_range, qty_str, cum_qty_str in delivery_lines:
            if "w" in date_or_range:
                from_to_match = re.match(r"(20\d{2}\s*w\d{2})\s*-\s*(20\d{2}\s*w\d{2})", date_or_range)
                if from_to_match:
                    date_from = from_to_match.group(1)
                    date_until = from_to_match.group(2)
                else:
                    date_from = date_until = date_or_range
            else:
                date_from = date_until = date_or_range

            results.append({
                "Site": site,
                "ClientCode": "100442",
                "ClientMaterialNo": customer_code_val,
                "AVOMaterialNo": avo_code,
                "DateFrom": date_from,
                "DateUntil": date_until,
                "Quantity": int(qty_str),
                "ForecastDate": forecast_date,
                "LastDeliveryDate": last_delivery_date,
                "LastDeliveredQuantity": last_delivered_qty,
                "CumulatedQuantity": int(cum_qty_str),
                "EDIStatus": "Forecast"
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