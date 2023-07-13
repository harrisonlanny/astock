import fitz

texts = []
doc = fitz.open("test.pdf") # open a document
# for page in doc: # iterate the document pages
#     # texts.append(page.get_text())

print(doc[0])
# count = 0
# for page in doc:
#     for chars in page.get_texttrace():
#         text = ''
#         for char in chars['chars']:
#             text += chr(char[0])
#         count+=1
#         if count <= 10:
#             print(f"【**{text}**】", chars)
#             pix = page.get_pixmap(clip=chars['bbox'])
#             print('unicolor: ', pix.is_unicolor)
#             print('topusage: ', pix.color_topusage())
#             print("\n")
        

# 【**告**】 {'dir': (1.0, 0.0), 'font': 'å®\x8bä½\x93', 'wmode': 0, 'flags': 4, 'bidi_lvl': 0, 'bidi_dir': 2, 'ascender': 0.859375, 'descender': -0.140625, 'colorspace': 3, 'color': (0.0, 0.0, 0.0), 'size': 9.0, 'opacity': 1.0, 'linewidth': 0.45, 'spacewidth': 4.5, 'type': 0, 'chars': ((21578, 2682, (334.0299987792969, 51.9599609375), (334.0299987792969, 44.2255859375, 343.0299987792969, 53.2255859375)),), 'bbox': (334.0299987792969, 44.2255859375, 343.0299987792969, 53.2255859375), 'layer': '', 'seqno': 3}
# unicolor:  False
# topusage:  (0.58, b'\xff\xff\xff')


# 【**告**】 {'dir': (1.0, 0.0), 'font': 'å®\x8bä½\x93', 'wmode': 0, 'flags': 4, 'bidi_lvl': 0, 'bidi_dir': 2, 'ascender': 0.859375, 'descender': -0.140625, 'colorspace': 3, 'color': (0.0, 0.0, 0.0), 'size': 9.0, 'opacity': 1.0, 'linewidth': 0.45, 'spacewidth': 4.5, 'type': 0, 'chars': ((21578, 2682, (277.3699951171875, 63.239990234375), (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375)),), 'bbox': (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375), 'layer': '', 'seqno': 4}
# unicolor:  False
# topusage:  (0.8, b'\xff\xff\xff')

# {'text': '告', 'x0': 277.37, 'top': 55.509000000000015, 'x1': 286.37, 'bottom': 64.50900000000001, 'chars': [{'matrix': (1, 0, 0, 1, 277.37, 778.68), 'fontname': 'ABCDEE+宋体', 'adv': 9.0, 'upright': True, 'x0': 277.37, 'y0': 777.411, 'x1': 286.37, 'y1': 786.411, 'width': 9.0, 'height': 9.0, 'size': 9.0, 'object_type': 'char', 'page_number': 249, 'text': '告', 'stroking_color': 0, 'non_stroking_color': 0, 'top': 55.509000000000015, 'bottom': 64.50900000000001, 'doctop': 194302.269000001}]}

# 'bbox': (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375)
# 'bbox': x0, top, x1, bottom