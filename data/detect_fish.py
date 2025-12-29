import pdfplumber

# Open the PDF
with pdfplumber.open("fishing_synopsis.pdf") as pdf:
    # Get the specific page where the fish is (e.g., page 6 based on your document)
    page = pdf.pages[16] 
    
    # Create the image
    im = page.to_image(resolution=150)

    # 1. RED BOXES: Text Characters
    # (If the fish is a font character, it will be red)
    im.draw_rects(page.chars, stroke="red", stroke_width=1, fill=None)

    # 2. GREEN BOXES: Images
    # (If the fish is a picture/bitmap, it will be green)
    im.draw_rects(page.images, stroke="green", stroke_width=2, fill=None)

    # 3. BLUE BOXES: Vector Rectangles
    # (If the fish is inside a colored box/container, it might be blue)
    im.draw_rects(page.rects, stroke="blue", stroke_width=2, fill=None)

    # 4. YELLOW LINES: Vector Lines
    # (Straight lines used in drawing)
    im.draw_lines(page.lines, stroke="yellow", stroke_width=2)

    # 5. ORANGE BOXES: Vector Curves (The likely winner!)
    # (Complex shapes like fish icons are usually made of curves)
    im.draw_rects(page.curves, stroke="orange", stroke_width=2, fill=None)

    # Show the result
    im.show()
    im.save("debug_fish_boxes.png")