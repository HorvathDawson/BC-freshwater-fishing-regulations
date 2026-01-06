import pdfplumber
import pytesseract
from PIL import Image, ImageDraw, ImageOps, ImageFont
import numpy as np
import random
import sys
import os

# --- CONFIGURATION ---
PDF_PATH = 'output/fishing_synopsis.pdf'
PAGE_NUM = 17  # Uses 1-based indexing for print output, 0-based for code (16)
OUTPUT_IMG = 'output/ocr_debug_view.png'

# If on Windows, you MUST point to your tesseract.exe
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def get_char_pixels(char, page_image, scale):
    """
    Extract the pixel data from a character's bounding box.
    Uses EXACT coordinates from PDF metadata (scaled), no padding or shrinking.
    """
    try:
        # 1. Get raw coordinates and scale them
        x0 = int(char['x0'] * scale)
        y0 = int(char['top'] * scale)
        x1 = int(char['x1'] * scale)
        y1 = int(char['bottom'] * scale)
        
        # 2. Boundary Check (Just to ensure we don't crash if PDF floats are slightly off)
        x0 = max(0, x0)
        y0 = max(0, y0)
        x1 = min(page_image.width, x1)
        y1 = min(page_image.height, y1)
        
        # 3. Validation: Ensure width/height > 0
        if x1 <= x0 or y1 <= y0:
            return None, None
        
        # 4. Crop exactly
        char_region = page_image.crop((x0, y0, x1, y1))
        pixels = np.array(char_region)
        return char_region, pixels

    except Exception as e:
        print(f"Error getting pixels for char: {e}")
        return None, None

def check_char_with_ocr(char, page_image, scale):
    """
    Runs Tesseract on a single character crop to see if it matches expected text.
    """
    if not char['text'].strip():
        return True, "SPACE", None

    char_region, _ = get_char_pixels(char, page_image, scale)
    if char_region is None:
        return False, "ERR_CROP", None

    # --- Pre-processing ---
    # 1. Grayscale
    img = char_region.convert('L')

    # 2. Resize (Upscale is critical for single chars)
    base_height = 60
    w_percent = (base_height / float(img.size[1]))
    w_size = int((float(img.size[0]) * float(w_percent)))
    w_size = max(w_size, 30) # Minimum width
    img = img.resize((w_size, base_height), Image.Resampling.LANCZOS)

    # 3. Binarize (Force high contrast)
    # Threshold < 200 becomes black, else white.
    img = img.point(lambda p: 255 if p > 200 else 0)

    # 4. Massive Padding (Tesseract needs whitespace around the letter)
    img = ImageOps.expand(img, border=50, fill='white')

    # --- OCR ---
    # --psm 10: Treat image as a single character
    config = '--psm 10 --oem 3'
    
    # Whitelist helps if we know it's alphanumeric
    if char['text'].isalnum():
        config += ' -c tessedit_char_whitelist=abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

    try:
        ocr_text = pytesseract.image_to_string(img, config=config).strip()
    except Exception as e:
        return False, f"ERR_TESS: {str(e)[:10]}", img

    # --- Validation ---
    expected = char['text'].strip()
    
    # Logic: Match?
    is_match = False
    if ocr_text == expected:
        is_match = True
    elif ocr_text.lower() == expected.lower():
        is_match = True
    
    # Common confusions
    confusions = {'l': ['1', 'I'], '1': ['l', 'I'], '0': ['O'], 'O': ['0']}
    if expected in confusions and ocr_text in confusions[expected]:
        is_match = True

    result_text = ocr_text if ocr_text else "[Empty]"
    
    return is_match, result_text, img

def create_ocr_visualization(chars_to_check, page_image, scale):
    print(f"STEP 4: Generating visualization for {len(chars_to_check)} characters...")
    
    if not chars_to_check:
        print("!! WARNING: No characters passed to visualization.")
        return

    cell_w, cell_h = 150, 150
    cols = 5
    rows = (len(chars_to_check) + cols - 1) // cols
    
    grid_img = Image.new('RGB', (cols * cell_w, rows * cell_h), (240, 240, 240))
    draw = ImageDraw.Draw(grid_img)
    
    try:
        font = ImageFont.truetype("arial.ttf", 16)
    except:
        font = ImageFont.load_default()

    for idx, char in enumerate(chars_to_check):
        # Run OCR Check
        match, ocr_result, processed_img = check_char_with_ocr(char, page_image, scale)
        
        # Grid Maths
        col = idx % cols
        row = idx // cols
        x = col * cell_w
        y = row * cell_h
        
        # Color: Green = Good, Red = Ghost
        color = (200, 255, 200) if match else (255, 200, 200)
        draw.rectangle([x, y, x + cell_w - 2, y + cell_h - 2], fill=color)
        
        # Paste Image
        if processed_img:
            # Shrink to fit cell
            disp_img = processed_img.copy()
            disp_img.thumbnail((130, 80))
            grid_img.paste(disp_img, (x + 10, y + 10))
            
        # Write Text
        draw.text((x + 10, y + 100), f"Exp: {char['text']}", fill="black", font=font)
        draw.text((x + 10, y + 120), f"OCR: {ocr_result}", fill="blue", font=font)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_IMG), exist_ok=True)
    grid_img.save(OUTPUT_IMG)
    print(f"STEP 5: Success! Saved visualization to {OUTPUT_IMG}")

def main():
    print("STEP 1: Script started. Loading PDF...")
    
    if not os.path.exists(PDF_PATH):
        print(f"!! ERROR: File not found at {PDF_PATH}")
        return

    with pdfplumber.open(PDF_PATH) as pdf:
        try:
            # Get page (index 16 is page 17)
            page = pdf.pages[PAGE_NUM - 1]
        except IndexError:
            print(f"!! ERROR: PDF does not have {PAGE_NUM} pages.")
            return

        print("STEP 2: Rendering page image for OCR...")
        scale = 300 / 72  # 300 DPI for good OCR
        im = page.to_image(resolution=300)
        page_image = im.original
        
        all_chars = page.chars
        print(f"   Found {len(all_chars)} characters total.")
        
        # Filter for testing (only non-empty text)
        non_empty_chars = [c for c in all_chars if c['text'].strip()]
        print(f"   Found {len(non_empty_chars)} non-empty characters.")

        if not non_empty_chars:
            print("!! ERROR: No text characters found on this page.")
            return

        print("STEP 3: Selecting sample characters...")
        # Take a random sample of 25 characters to visualize
        sample_size = 25
        if len(non_empty_chars) > sample_size:
            sample_chars = random.sample(non_empty_chars, sample_size)
        else:
            sample_chars = non_empty_chars
            
        # Run the visualization
        create_ocr_visualization(sample_chars, page_image, scale)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n!!!! CRASHED !!!!\nError: {e}")
        # This helps see errors if the window closes immediately
        import traceback
        traceback.print_exc()