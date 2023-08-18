import fitz
import pdfplumber

from utils.index import _map

texts = []
pdf_url = "test.pdf"
doc = fitz.open(pdf_url) # open a document
pdf = pdfplumber.open(pdf_url)
# for page in doc: # iterate the document pages
#     # texts.append(page.get_text())

# print(doc[0])
# count = 0
# for index, page in enumerate(doc):
#     ppage = pdf.pages[index]
#     for chars in page.get_texttrace():
#         text = ''
#         for char in chars['chars']:
#             text += chr(char[0])
#         count+=1
#         bbox = chars['bbox']
#         pix = page.get_pixmap(clip=bbox)
#         # print('unicolor: ', pix.is_unicolor)
#         print(f"【**{text}**】", 'topusage: ', pix.color_topusage()[0])
#         objects = ppage.crop(bbox=bbox).objects
#         print('crop rect count: ', len(objects['rect']), 'crop chars: ', _map(objects['char'], lambda item: item['text']))
#         print("\n")


for index, page in enumerate(pdf.pages):
    fixz_page = doc[index]

    def keep_visible_char(obj):
        type = obj['object_type']
        top = obj["top"]
        bottom = obj["bottom"]
        x0 = obj["x0"]
        x1 = obj["x1"]
        bbox = (x0, top, x1, bottom)

        if type == 'char':
            text = obj['text']
            pix = fixz_page.get_pixmap(clip=bbox)
            topusage = pix.color_topusage()[0]
            # page.crop(bbox=bbox)
            objects = page.crop(bbox=bbox).objects
            # rects = objects['rect']
            # chars = objects['char']
            # chars_text = _map(chars, lambda item: item['text'])

            print(f"【{text}】", f"topuseage: {topusage}")
            # print(f"rect count: {len(rects)}", f"char count: {chars_text}")

        return True

    # 过滤掉不可见的文本
    # page = page.filter(keep_visible_char)
    # page.find_tables()
    # objects = page.objects

    objs = page.objects

    for type in objs:
        objects = objs[type]
        if type == 'char':
            for obj in objects:
                text = obj['text']
                top = obj["top"]
                bottom = obj["bottom"]
                x0 = obj["x0"]
                x1 = obj["x1"]
                w = x1 - x0
                h = bottom - top
                bbox = (x0, top, x1, bottom)
                pix = fixz_page.get_pixmap(clip=bbox)
                topusage = pix.color_topusage()[0]
                print(f"【{text}】", f"topuseage: {topusage}")
                if w > 0 and h > 0:
                    crop_objects = page.crop(bbox=bbox).objects
                    rects = crop_objects['rect']
                    chars = crop_objects['char']
                    texts = _map(chars, lambda char: char['text'])
                    print('crop rect count: ', len(rects), 'crop char count: ', len(chars))
                    print('crop chars: ', texts)

        

# 【**告**】 {'dir': (1.0, 0.0), 'font': 'å®\x8bä½\x93', 'wmode': 0, 'flags': 4, 'bidi_lvl': 0, 'bidi_dir': 2, 'ascender': 0.859375, 'descender': -0.140625, 'colorspace': 3, 'color': (0.0, 0.0, 0.0), 'size': 9.0, 'opacity': 1.0, 'linewidth': 0.45, 'spacewidth': 4.5, 'type': 0, 'chars': ((21578, 2682, (334.0299987792969, 51.9599609375), (334.0299987792969, 44.2255859375, 343.0299987792969, 53.2255859375)),), 'bbox': (334.0299987792969, 44.2255859375, 343.0299987792969, 53.2255859375), 'layer': '', 'seqno': 3}
# unicolor:  False
# topusage:  (0.58, b'\xff\xff\xff')


# 【**告**】 {'dir': (1.0, 0.0), 'font': 'å®\x8bä½\x93', 'wmode': 0, 'flags': 4, 'bidi_lvl': 0, 'bidi_dir': 2, 'ascender': 0.859375, 'descender': -0.140625, 'colorspace': 3, 'color': (0.0, 0.0, 0.0), 'size': 9.0, 'opacity': 1.0, 'linewidth': 0.45, 'spacewidth': 4.5, 'type': 0, 'chars': ((21578, 2682, (277.3699951171875, 63.239990234375), (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375)),), 'bbox': (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375), 'layer': '', 'seqno': 4}
# unicolor:  False
# topusage:  (0.8, b'\xff\xff\xff')

# {'text': '告', 'x0': 277.37, 'top': 55.509000000000015, 'x1': 286.37, 'bottom': 64.50900000000001, 'chars': [{'matrix': (1, 0, 0, 1, 277.37, 778.68), 'fontname': 'ABCDEE+宋体', 'adv': 9.0, 'upright': True, 'x0': 277.37, 'y0': 777.411, 'x1': 286.37, 'y1': 786.411, 'width': 9.0, 'height': 9.0, 'size': 9.0, 'object_type': 'char', 'page_number': 249, 'text': '告', 'stroking_color': 0, 'non_stroking_color': 0, 'top': 55.509000000000015, 'bottom': 64.50900000000001, 'doctop': 194302.269000001}]}

# 'bbox': (277.3699951171875, 55.505615234375, 286.3699951171875, 64.505615234375)
# 'bbox': x0, top, x1, bottom

# text = '第(cid:1212)节 (cid:1948)(cid:2600)简(cid:1275)和(cid:1131)要(cid:17234)务指标 ................................................................................................. 5'
# format = "第二节 公司简介和主要财务指标"
# print(chr(int(17234)))