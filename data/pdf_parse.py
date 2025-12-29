import pdfplumber
import json
import re

def clean_text(text):
    if not text:
        return ""
    # Normalize whitespace and remove common PDF artifacts
    text = text.replace('\uf0dc', '').strip()
    return re.sub(r'\s+', ' ', text)

def parse_fishing_synopsis_regex(pdf_path, output_path):
    data = {
        "metadata": {
            "source": "BC Freshwater Fishing Regulations Synopsis",
            "years": "2025-2027"
        },
        "regions": {}
    }

    print(f"Scanning {pdf_path} using Regex Anchor Strategy...")

    # 1. Regex to find Region Headers (Tier 2)
    #    Matches: "REGION 6 - Skeena" or "REGION 7A - Omineca"
    region_header_re = re.compile(r"^REGION\s+(\d+[A-Z]?)\s*[-–]\s*(.+)", re.IGNORECASE)

    # 2. Regex to find the Management Unit Anchor (The "Golden Key")
    #    Matches: "8-12", "6-9", "7-32" 
    #    Looks for a digit, a hyphen, and digits, ensuring it's distinct words.
    #    Captures: (Group 1: Water Name) (Group 2: M.U.) (Group 3: Remainder/Regs)
    #    NOTE: We search specifically for the M.U. to split the string.
    mu_pattern = re.compile(r"(\b\d{1,2}-[0-9]{1,2}\b)")

    current_region_id = None
    
    # "text" strategy allows us to get the raw layout without forcing strict columns
    table_settings = {
        "vertical_strategy": "text", 
        "horizontal_strategy": "text",
        "snap_tolerance": 4,
    }

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue

            # --- Detect Region ---
            lines = text.split('\n')
            for line in lines[:5]:
                match = region_header_re.match(line.strip())
                if match:
                    current_region_id = match.group(1)
                    region_name = match.group(2)
                    
                    if current_region_id not in data["regions"]:
                        data["regions"][current_region_id] = {
                            "name": region_name,
                            "general_regulations": [],
                            "water_specific_regulations": []
                        }
                    print(f"Page {i+1}: Found Region {current_region_id} ({region_name})")

            # --- Process Water Tables ---
            if current_region_id and "WATER BODY" in text:
                # Extract the table "loosely" so we don't miss text
                tables = page.extract_tables(table_settings)
                
                target_list = data["regions"][current_region_id]["water_specific_regulations"]

                for table in tables:
                    for row in table:
                        # 1. FLATTEN THE ROW
                        # Instead of trusting the columns, we join them into one string
                        # and let Regex decide where the split is.
                        full_row_text = " ".join([clean_text(cell) for cell in row if cell])
                        
                        # Skip headers/garbage
                        if "WATER BODY" in full_row_text or "EXCEPTIONS" in full_row_text:
                            continue
                        if not full_row_text or "Stocked Lake" in full_row_text:
                            continue

                        # 2. FIND THE ANCHOR (Management Unit e.g. "8-12")
                        # We look for the last occurrence of the pattern to be safe, 
                        # though usually there's only one.
                        match = None
                        for m in mu_pattern.finditer(full_row_text):
                            match = m
                        
                        if match:
                            # --- CASE A: NEW ENTRY DETECTED ---
                            # Split the string using the M.U. location
                            
                            # Name is everything before the M.U.
                            raw_name = full_row_text[:match.start()].strip()
                            
                            # M.U. is the match itself
                            mu_unit = match.group(1)
                            
                            # Regulations are everything after the M.U.
                            regs = full_row_text[match.end():].strip()

                            # Create new entry
                            new_entry = {
                                "water_body": raw_name,
                                "mgmt_unit": mu_unit,
                                "regulations": [regs] if regs else []
                            }
                            target_list.append(new_entry)

                        else:
                            # --- CASE B: CONTINUATION / NO M.U. ---
                            # If no "8-12" pattern is found, this text belongs to the previous lake.
                            # It could be part of the name (e.g. "(formerly known as...)")
                            # or part of the regulations.
                            
                            if len(target_list) > 0:
                                last_entry = target_list[-1]
                                
                                # Heuristic: Does it look like a Regulation or a Name continuation?
                                # If it starts with parentheses '(', it's often a name clarifier.
                                # If it starts with keywords like "No Fishing", "Trout", "Bait", it's a regulation.
                                
                                if full_row_text.startswith("(") or "located" in full_row_text.lower():
                                    last_entry["water_body"] += " " + full_row_text
                                else:
                                    last_entry["regulations"].append(full_row_text)

            # --- Capture General Regulations (Tier 2) ---
            # If we are in a region page but it DOESN'T have a water table, 
            # or if it has "Daily Quotas" in the text
            elif current_region_id and ("Daily Quotas" in text or "Regional Regulations" in text):
                target_reg_list = data["regions"][current_region_id]["general_regulations"]
                # Only capture lines that look like regulations
                for line in lines:
                    clean = clean_text(line)
                    if len(clean) > 10 and not any(x in clean for x in ["2025-2027", "Page", "Regional Regulations"]):
                         # Avoid duplicates
                         if clean not in target_reg_list:
                            target_reg_list.append(clean)

    # Save output
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    print(f"\nParsing complete. Data saved to {output_path}")

# --- Execute ---
if __name__ == "__main__":
    # Replace filename with your local path
    parse_fishing_synopsis_regex('fishing_synopsis.pdf', 'bc_fishing_regs_regex.json')