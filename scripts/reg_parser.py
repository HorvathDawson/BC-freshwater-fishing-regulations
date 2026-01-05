import re

class RegParser:
    # START_KEYWORDS triggers a new regulation line.
    START_KEYWORDS = [
        "No Fishing", "Closed", "No Ice Fishing", "Bait ban", "Fly fishing", 
        "Artificial fly", "Barbless", "Quota", "Daily", "Catch and release", 
        "Release", "Class", "Steelhead", "Trout", "Char", "Kokanee", "Chinook", 
        "Coho", "Rainbow", "Cutthroat", "Salmon", "Bass", "Walleye", "Pike", 
        "Perch", "Burbot", "Crayfish", "Single", "No", "The standard"
    ]

    PATTERNS = {
        "Advisory": [r"WARNING", r"Mercury", r"Thin ice", r"NOTICE", r"consumption"],
        "Fishing Closure": [r"No Fishing", r"Closed", r"No Ice Fishing", "The standard"],
        "Classified Waters": [r"Class I", r"Class II", r"Steelhead Stamp"],
        "Access Restriction": [r"Youth", r"Disabled", r"Permit"],
        "Boating Restriction": [r"boat", r"motor", r"speed", r"towing", r"vessel", r"power"],
        "Gear Restriction": [r"barbless", r"hook", r"bait ban", r"fly only", r"artificial fly", r"set line", r"spear"],
        "Quota / Catch Limit": [r"quota", r"limit", r"daily", r"possession", r"catch and release", r"release", r"retain", r"trout", r"char", r"salmon", r"bass", r"walleye", r"pike"]
    }
    DATE_PATTERN = r"([A-Z][a-z]{2,8}\s+\d{1,2}\s*[-–]\s*[A-Z][a-z]{2,8}\s+\d{1,2})"

    @staticmethod
    def classify(text):
        for category, patterns in RegParser.PATTERNS.items():
            for pat in patterns:
                if re.search(pat, text, re.IGNORECASE): return category
        return "General Restriction"

    @staticmethod
    def pre_clean(text):
        text = text.replace('\n', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        
        # --- DEFINE COMMON PATTERNS ---
        adjectives = r'Bull|Lake|Brook|Brown|Golden|Dolly|Rainbow|Cutthroat|Summer|Winter|Wild|Hatchery|Northern|No|Smallmouth|Largemouth'
        targets = r'Trout|Char|Varden|Steelhead|Salmon|Pike|Bass|Rainbow|Cutthroat|Kokanee'

        # 1. Insert semicolons before ALL start keywords
        keyword_pattern = r'(?<!;)\s+\b(' + '|'.join(RegParser.START_KEYWORDS) + r')\b'
        text = re.sub(keyword_pattern, r'; \1', text, flags=re.IGNORECASE)
        
        # 2. Fix: Remove semicolons inside parentheses/includes
        text = re.sub(r'(\bincluding|\bincludes|\bexcept|\(|\[)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)

        # 3. Fix: Remove semicolons after Forward Slashes
        text = re.sub(r'/\s*;\s*', '/', text)

        # 3.5 FIX: Specific Healer for "Wild/Hatchery" + Species
        text = re.sub(r'\b(Wild|Hatchery)\s*;\s*(Rainbow|Cutthroat|Steelhead|Trout|Char)', r'\1 \2', text, flags=re.IGNORECASE)

        # --- NEW FIX FOR "Cultus Lake" ---
        # 3.6 FIX: Split distinct quotas that start with a number (e.g. "...50 cm, 1 bull trout...")
        # Since "1" isn't a START_KEYWORD, we force a split if we see: Comma -> Number -> Fish Name
        quota_split_pattern = rf',\s+(\d+\s+(?:{adjectives}|{targets}))'
        text = re.sub(quota_split_pattern, r'; \1', text, flags=re.IGNORECASE)
        # ---------------------------------

        # 4. Fix: Compound Species Names (Bull Trout, Smallmouth Bass, etc)
        # This fixes cases where keywords inserted a semicolon inside a name (e.g., "Bull; Trout")
        compound_pattern = rf'\b({adjectives})\s*;\s*({targets})'
        text = re.sub(compound_pattern, r'\1 \2', text, flags=re.IGNORECASE)
        text = re.sub(compound_pattern, r'\1 \2', text, flags=re.IGNORECASE)

        # 5. FIX: "Single Barbless"
        text = re.sub(r'(\bsingle)\s*;\s*barbless', r'\1 barbless', text, flags=re.IGNORECASE)

        # 6. FIX: Fish Daily Quotas
        fish_names = r'(Walleye|Pike|Perch|Bass|Trout|Char|Salmon|Steelhead|Kokanee|Burbot|Crayfish)'
        quota_merge_pattern = rf'\b{fish_names}\s*;\s*(Daily|Quota|Limit)'
        text = re.sub(quota_merge_pattern, r'\1 \2', text, flags=re.IGNORECASE)

        # 7. FIX: Coquihalla Healer (Mandatory Comma)
        text = re.sub(r'(catch\s+and\s+release),\s*;\s*(bait\s+ban)', r'\1, \2', text, flags=re.IGNORECASE)

        # 8. Generic Species + Quota Action (Backup)
        species_merge_pattern = r'\b(Trout|Char|Steelhead|Salmon|Kokanee|Chinook|Coho|Rainbow|Cutthroat|Bass|Walleye|Pike)(.*?);\s*(catch|limit|quota|daily|release)'
        def merge_match(match):
            species = match.group(1)
            middle = match.group(2)
            action = match.group(3)
            if len(middle) < 40: return f"{species}{middle} {action}"
            return match.group(0)
        text = re.sub(species_merge_pattern, merge_match, text, flags=re.IGNORECASE)

        # 9. FIX: "Exempt From Single"
        text = re.sub(r'(\bfrom)\s*;\s*(single)', r'\1 \2', text, flags=re.IGNORECASE)

        # 10. FIX: "Trout and Kokanee"
        text = re.sub(r'(\band)\s*;\s*', r'\1 ', text, flags=re.IGNORECASE)

        # 11. FIX: "Is Closed" (Buttle Lake)
        text = re.sub(r'(\bis|\bare|\bremain)\s*;\s*(closed)', r'\1 \2', text, flags=re.IGNORECASE)

        # 12. FIX: "A No Fishing Area" (Little Qualicum)
        text = re.sub(r'(\ba)\s*;\s*(no\s+fishing)', r'\1 \2', text, flags=re.IGNORECASE)

        return text

    @staticmethod
    def clean_and_split(text):
        initial_chunks = [c.strip() for c in text.split(';') if c.strip()]
        final_items = []
        for chunk in initial_chunks:
            sentences = re.split(r'(?<=[a-z])\.\s+(?=[A-Z])', chunk)
            for sentence in sentences:
                clean_sentence = sentence.strip(' ,;.') 
                if clean_sentence:
                    final_items.append(clean_sentence)
        return final_items

    @staticmethod
    def parse_reg(text):
        text = RegParser.pre_clean(text)
        chunks = RegParser.clean_and_split(text)
        results = []
        for chunk in chunks:
            if not chunk: continue
            res = { "type": RegParser.classify(chunk), "details": chunk, "date_ranges": [] }
            
            date_matches = re.findall(RegParser.DATE_PATTERN, chunk)
            if date_matches:
                res["date_ranges"] = date_matches
                
            results.append(res)
        return results

def merge_orphaned_details(regs_list):
    if not regs_list: return []
    merged = [regs_list[0]]
    for i in range(1, len(regs_list)):
        current = regs_list[i]
        prev = merged[-1]
        txt = current['details'].strip()
        should_merge = False
        
        if txt.startswith('(') and txt.endswith(')'): should_merge = True
        elif re.match(r'^(and|but|or|includes)\b', txt, re.IGNORECASE): should_merge = True
        elif current['type'] == "General Restriction":
             is_short = len(txt) < 60
             has_start_keyword = any(txt.lower().startswith(k.lower()) for k in RegParser.START_KEYWORDS)
             if is_short and not has_start_keyword: should_merge = True
             
        if should_merge:
            prev['details'] += " " + txt
            if current['date_ranges']:
                prev['date_ranges'].extend(current['date_ranges'])
        else: merged.append(current)
    return merged