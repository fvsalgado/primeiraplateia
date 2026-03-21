"""
Scraper: São Luiz Teatro Municipal
URL listagem: https://www.teatrosaoluiz.pt/programacao/
URLs eventos:  /espetaculo/slug/
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from scrapers.utils import (
    make_id, parse_date_range, parse_date, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)
from scrapers.schema import normalize_category

BASE       = "https://www.teatrosaoluiz.pt"
AGENDA     = f"{BASE}/programacao/"
IMG_DOMAIN = "www.teatrosaoluiz.pt"

# SVG oficial do São Luiz (fundo preto, letras brancas).
# Guardado inline para não depender de URLs externas que podem mudar.
_LOGO_SVG = (
    '<svg id="_1-LINHA" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 2053.52 354.36">'
    '<rect width="2053.52" height="354.36" fill="#000"/>'
    '<path style="fill:#FFF" d="M1770.29.32h-177.02v35.39h0s0-.01,0-.01v.05-.02s0,0,0,0v35.39h35.4c19.55,0,35.4,15.85,35.4,35.4h0s0,.01,0,.01h0v70.79h0s0,.01,0,.01h0v70.8h0s0,.01,0,.01h0c0,19.56-15.85,35.4-35.4,35.4h-35.4v35.39h0s0-.01,0-.01v.05-.02s0,0,0,0v35.39h212.42v-35.39h0v-.02h0v-35.4h-35.4c-19.55,0-35.4-15.84-35.4-35.39h0v-.02h0v-70.79h0v-.02h0v-70.79h0v-.02h0c0-19.55,15.85-35.4,35.4-35.4h35.4v-35.39h0v-.02h0V.32h-35.4Z"/>'
    '<path style="fill:#FFF" d="M1911.94,248.16c0,.3,0,.6.01.9,0,.31.02.61.04.91,0,.15.01.29.02.43.02.27.04.54.06.8.03.36.07.7.1,1.05.04.29.08.59.12.88.05.36.11.72.17,1.07.05.32.11.63.17.94.13.67.29,1.33.45,1.98.09.31.17.63.27.94.08.29.17.57.26.86.06.2.13.39.19.58.09.28.19.55.29.82.06.17.12.34.19.51.11.3.23.6.36.9.25.6.51,1.19.79,1.77.2.4.4.8.61,1.2l.05.1c.25.47.52.94.79,1.4.21.35.42.69.64,1.03l.11.17c.19.31.4.62.62.92.23.34.48.68.72,1.01.34.46.69.9,1.05,1.34s.73.86,1.12,1.28c.38.42.77.83,1.17,1.23l1.23,1.17c.42.38.85.76,1.28,1.12.44.36.88.71,1.34,1.05.46.34.92.67,1.39.98.47.32.95.63,1.44.93.48.29.98.58,1.48.85s1.01.53,1.52.78c4.64,2.24,9.85,3.49,15.35,3.49h106.22v70.81h-212.43v-212.43h106.22c.3,0,.6,0,.9-.01.3-.01.61-.02.91-.04.16,0,.32,0,.48-.03.24-.01.48-.03.71-.05.39-.03.77-.07,1.16-.11.25-.03.49-.06.73-.1.16-.02.33-.04.49-.07.29-.04.57-.09.85-.14,1.75-.31,3.46-.75,5.11-1.31.28-.09.55-.18.82-.29,1.09-.39,2.15-.84,3.19-1.34.51-.25,1.02-.51,1.51-.78.26-.14.51-.28.76-.42.44-.25.87-.51,1.29-.78.53-.34,1.06-.69,1.57-1.06.23-.16.46-.33.69-.5.46-.34.91-.69,1.35-1.05.42-.35.84-.71,1.24-1.08.85-.77,1.67-1.59,2.44-2.44.38-.41.75-.84,1.11-1.27h0c.35-.45.7-.89,1.04-1.34.19-.25.37-.5.55-.76.15-.2.29-.42.43-.63.17-.24.33-.48.48-.72.31-.47.6-.96.88-1.45.15-.25.29-.5.43-.76.27-.49.53-1,.77-1.51.13-.26.25-.52.36-.78.12-.26.24-.52.35-.79.34-.79.64-1.61.92-2.43.09-.27.18-.54.27-.82.09-.28.17-.56.25-.84.13-.45.25-.91.36-1.37.06-.25.12-.5.17-.75.06-.26.11-.52.16-.77.09-.41.16-.82.22-1.24.02-.1.04-.2.05-.3.08-.56.16-1.12.21-1.68.06-.56.1-1.14.13-1.71.03-.62.05-1.24.05-1.87v-.02c0-.58-.02-1.17-.05-1.74v-.13c-.03-.57-.07-1.15-.13-1.71-.05-.56-.13-1.12-.21-1.68l-.05-.3c-.06-.42-.13-.83-.22-1.24-.1-.51-.21-1.01-.33-1.52-.11-.46-.23-.92-.36-1.37-.08-.28-.16-.56-.25-.84s-.18-.55-.27-.82c-.46-1.37-1.01-2.71-1.63-4-.24-.51-.5-1.02-.77-1.51l-.43-.76c-.28-.49-.57-.98-.88-1.45l-.08-.11c-.43-.68-.9-1.35-1.38-2-.34-.45-.69-.89-1.04-1.33h0c-.36-.44-.73-.87-1.11-1.28l-1.17-1.23h0c-.4-.42-.8-.8-1.23-1.18l-.03-.03c-.4-.37-.82-.73-1.24-1.08-1.1-.9-2.25-1.74-3.45-2.51-.47-.31-.96-.6-1.45-.88h-.01c-.73-.43-1.49-.83-2.26-1.2-.52-.25-1.04-.49-1.58-.71-.33-.14-.66-.28-1-.41-.74-.29-1.49-.55-2.25-.78-.27-.09-.55-.17-.82-.25-.29-.08-.57-.16-.86-.23-.56-.15-1.13-.28-1.71-.39-.29-.07-.58-.12-.87-.17-.58-.11-1.17-.2-1.76-.27-.3-.04-.59-.08-.89-.1-.3-.03-.6-.06-.9-.08-.23-.02-.45-.04-.68-.05-.2-.02-.41-.03-.61-.03-.1-.01-.19-.01-.29-.01h-.21c-.1-.01-.21-.01-.32-.02h-106.82V.32h106.22c.3,0,.6,0,.9.01,12.09.09,23.7,2.22,34.5,6.04,3.55,1.25,7.01,2.69,10.38,4.3h.01c21.86,10.46,39.6,28.19,50.07,50.06h0c1.61,3.38,3.05,6.84,4.3,10.4,3.82,10.79,5.94,22.39,6.04,34.47v.02c0,.3,0,.6,0,.9v106.22h-106.22c-4.89,0-9.54.99-13.78,2.78-.53.22-1.05.46-1.57.71-1.03.5-2.03,1.04-3,1.63-.73.44-1.45.91-2.14,1.41-.23.16-.46.33-.69.5-.45.34-.9.69-1.34,1.05-.21.18-.43.36-.64.55-.65.56-1.27,1.14-1.87,1.74-.61.6-1.19,1.23-1.74,1.87-.28.33-.57.67-.84,1.01-2.96,3.68-5.19,7.96-6.5,12.63-.14.5-.27,1.01-.38,1.52-.59,2.56-.91,5.24-.91,7.99v.03Z"/>'
    '<path style="fill:#FFF" d="M1557.84.32v35.39h0v212.46c0,.31,0,.62,0,.92-.1,12.08-2.22,23.67-6.04,34.46-1.25,3.56-2.69,7.02-4.3,10.39-5.23,10.93-12.28,20.83-20.76,29.31-8.48,8.48-18.38,15.53-29.31,20.76-3.37,1.61-6.83,3.05-10.39,4.3-10.8,3.82-22.4,5.94-34.49,6.04-.3.01-.6.01-.91.01s-.61,0-.91-.01c-12.09-.1-23.7-2.22-34.5-6.05-3.56-1.24-7.02-2.68-10.39-4.29-21.87-10.47-39.6-28.21-50.06-50.07-1.61-3.37-3.05-6.83-4.3-10.39-3.82-10.8-5.94-22.4-6.04-34.49-.01-.3-.01-.6-.01-.9V35.73h0V.32h70.8v35.39h0v70.81h0v70.8h0v.02h0v70.79h0v.02h0c.02,19.56,15.86,35.4,35.41,35.4s35.4-15.84,35.4-35.39V.32h70.81Z"/>'
    '<path style="fill:#FFF" d="M1274.59,283.55h-70.81c-19.55,0-35.4-15.85-35.4-35.4h0v-70.8h0v-.02h0v-70.79h0v-.02h0V35.73h0v-.02h0V.32h-70.81v354.04h212.42v-35.39h0v-.02h0v-35.4h-35.4Z"/>'
    '<path style="fill:#FFF" d="M212.47.32v70.8h-106.67c-.16,0-.32.01-.47.01-2.73.07-5.38.45-7.92,1.11-.29.07-.57.15-.86.23-1.39.39-2.75.87-4.07,1.44-.54.22-1.06.46-1.58.71-.44.21-.88.43-1.31.67-4.29,2.37-8.2,5.3-11.62,8.72-3.42,3.42-6.35,7.33-8.72,11.62-1.61,3.37-3.05,6.83-4.3,10.39C.15,119.53.05,131.13.05,143.22v-.02s-.01.6-.01.9v.02s0,.6,0,.9c.1,12.09,2.22,23.69,6.04,34.49,1.25,3.56,2.69,7.02,4.3,10.39,10.47,21.87,28.2,39.6,50.07,50.07,3.37,1.61,6.83,3.05,10.39,4.3,10.8,3.82,22.41,5.94,34.5,6.04.3.01.6.01.9.01h.01c1.54,0,3.05-.1,4.54-.3.49-.06.98-.14,1.47-.22.03,0,.06-.01.09-.01.61-.11,1.22-.23,1.82-.37.05-.01.1-.02.15-.03.59-.14,1.18-.3,1.76-.46.04-.01.08-.02.11-.03.46-.13.92-.28,1.37-.43.01-.01.03-.01.04-.02.45-.15.9-.31,1.35-.49.4-.15.79-.31,1.18-.47.35-.15.7-.31,1.04-.47.15-.06.29-.13.43-.2.14-.07.28-.14.42-.21.62-.31,1.23-.63,1.83-.98.01,0,.01-.01.02-.01s.02-.01.02-.01c1.53-.88,3-1.88,4.37-2.98.17-.13.34-.26.51-.4.35-.29.7-.59,1.04-.9.38-.34.75-.69,1.11-1.04.16-.15.33-.31.48-.48.29-.29.56-.57.83-.87.08-.07.15-.15.22-.23.14-.15.27-.3.4-.45.05-.06.1-.12.16-.18.19-.21.37-.42.55-.64.18-.26.41-.53.62-.8.21-.27.42-.53.62-.8.15-.17.28-.35.42-.53.08-.1.15-.21.22-.31.22-.28.42-.58.61-.87.2-.29.4-.58.59-.88.02-.02.03-.05.04-.05.02-.02.03-.05.05-.08.27-.43.54-.86.79-1.31.01-.02.03-.04.04-.06.04-.07.08-.14.12-.21.19-.33.37-.66.55-1,.19-.33.37-.66.55-1,.16-.31.32-.62.47-.94.07-.14.13-.27.19-.41.11-.21.2-.43.3-.65.14-.31.28-.63.41-.95.04-.08.07-.16.1-.25.24-.6.47-1.21.68-1.83,0-.01.01-.02.01-.03.01-.02.01-.03.02-.05.13-.4.26-.81.38-1.22.06-.2.12-.4.17-.59.1-.35.2-.7.28-1.05.01-.04.02-.08.03-.12.02-.09.04-.18.06-.27.09-.41.19-.82.27-1.25h0c.02-.1.03-.2.05-.3.08-.4.15-.79.21-1.19,0-.01.01-.03.01-.05.06-.4.11-.81.15-1.21.02-.15.04-.29.06-.44.04-.46.08-.93.1-1.4.01-.11.02-.22.03-.34.01-.25.02-.51.03-.76.01-.36.02-.72.02-1.09v-.03h0v-.14c-.01-.44-.01-.87-.05-1.3v-.44c0-.02,0-.05-.01-.07-.01-.19-.03-.38-.04-.57-.03-.46-.07-.91-.12-1.36-.02-.21-.05-.41-.07-.62-.04-.25-.06-.51-.1-.77l-.05-.3c-.01-.07-.03-.13-.04-.2-.03-.23-.07-.45-.12-.67-.02-.13-.03-.25-.06-.37l-.12-.57c-.06-.28-.12-.56-.19-.84l-.02-.11c-.06-.24-.12-.48-.18-.72-.07-.28-.15-.56-.23-.83-.04-.16-.1-.31-.15-.46-.1-.34-.2-.68-.33-1.02v-.03c-.46-1.36-1-2.68-1.61-3.96-.21-.44-.43-.87-.66-1.3-.03-.05-.06-.1-.08-.15-.02-.02-.03-.05-.04-.07l-.43-.76c-.01-.02-.03-.04-.04-.06-.21-.37-.44-.74-.66-1.1-.06-.1-.12-.2-.18-.29l-.04-.06c-.02-.04-.05-.07-.07-.11-.19-.31-.4-.62-.62-.92-.23-.34-.48-.68-.72-1.02h-.01c-.34-.46-.68-.89-1.04-1.34h-.01c-.36-.44-.72-.85-1.11-1.27l-.06-.06-1.11-1.17c-.4-.41-.8-.8-1.23-1.17-.01-.02-.03-.03-.04-.04-.24-.22-.48-.44-.73-.65-.17-.15-.34-.29-.51-.43-.05-.04-.09-.07-.14-.11s-.09-.07-.14-.11c-.21-.17-.42-.34-.64-.5-.14-.11-.28-.22-.42-.33-.07-.05-.15-.1-.22-.15-.18-.14-.37-.26-.55-.39-.21-.15-.41-.3-.62-.44-.11-.07-.22-.14-.33-.21-.13-.09-.26-.19-.39-.27-.13-.08-.26-.15-.39-.23-.11-.07-.22-.15-.33-.22-.13-.08-.27-.15-.4-.23-.06-.03-.11-.06-.16-.1-.06-.04-.12-.07-.17-.1h-.01c-.04-.02-.08-.04-.11-.06-.1-.06-.21-.12-.31-.18-.11-.06-.21-.12-.32-.18-.07-.04-.14-.07-.21-.11-.43-.24-.87-.46-1.31-.67-.52-.25-1.04-.49-1.58-.71-1.32-.57-2.68-1.05-4.07-1.44-.29-.08-.57-.16-.86-.23-2.54-.66-5.19-1.04-7.92-1.11-.15,0-.31-.01-.47-.01H212.47v212.43H.04v-70.8h106.67c1.54,0,3.05-.1,4.54-.3.49-.06.98-.14,1.47-.22.03,0,.06-.01.09-.01.61-.11,1.21-.23,1.82-.37.05-.01.1-.02.15-.03.59-.14,1.18-.3,1.76-.46.04-.01.08-.02.11-.03.46-.13.92-.28,1.37-.43.01-.01.03-.01.04-.02.45-.15.9-.31,1.35-.49.45-.15.9-.3,1.35-.49.4-.15.79-.31,1.18-.47.35-.15.7-.31,1.04-.47.15-.06.29-.13.43-.2.14-.07.28-.14.42-.21.62-.31,1.23-.63,1.83-.98.01,0,.01-.01.02-.01s.02-.01.02-.01c1.53-.88,3-1.88,4.37-2.98.17-.13.34-.26.51-.4.35-.29.7-.59,1.04-.9.31-.28.63-.57.94-.87l.17-.17c.29-.29.56-.57.83-.87.08-.07.15-.15.22-.23.14-.15.27-.3.4-.45.05-.06.1-.12.16-.18.19-.21.37-.42.55-.64.21-.27.41-.53.62-.8.2-.27.42-.53.62-.8.14-.17.28-.35.42-.53.08-.1.15-.21.22-.31.22-.28.42-.58.61-.87.2-.29.4-.58.59-.88.02-.02.03-.05.05-.08.27-.43.54-.86.79-1.31.01-.02.03-.04.04-.06.04-.07.08-.14.12-.21.19-.33.37-.66.55-1,.16-.31.32-.62.47-.94.07-.14.13-.27.19-.41.11-.21.2-.43.3-.65.14-.31.28-.63.41-.95.04-.08.07-.16.1-.25.24-.6.47-1.21.68-1.83,0-.01.01-.02.01-.03.01-.02.01-.03.02-.05.13-.4.26-.81.38-1.22.06-.2.12-.4.17-.59.1-.35.2-.7.28-1.05.01-.04.02-.08.03-.12.02-.09.04-.18.06-.27.09-.41.19-.82.27-1.25h0c.02-.1.03-.2.05-.3.08-.4.15-.79.21-1.19,0-.01.01-.03.01-.05.06-.4.11-.81.15-1.21.02-.15.04-.29.06-.44.04-.46.08-.93.1-1.4.01-.11.02-.22.03-.34.01-.25.02-.51.03-.76.01-.36.02-.72.02-1.09v-.03h0v-.14c-.01-.44-.01-.87-.05-1.3v-.44c0-.02,0-.05-.01-.07-.01-.19-.03-.38-.04-.57-.03-.46-.07-.91-.12-1.36-.02-.21-.05-.41-.07-.62-.04-.25-.06-.51-.1-.77l-.05-.3c-.01-.07-.03-.13-.04-.2-.03-.23-.07-.45-.12-.67-.02-.13-.03-.25-.06-.37l-.12-.57c-.06-.28-.12-.56-.19-.84l-.02-.11c-.06-.24-.12-.48-.18-.72-.07-.28-.15-.56-.23-.83-.05-.15-.1-.3-.14-.45-.1-.34-.21-.69-.34-1.03v-.03c-.46-1.36-1-2.68-1.61-3.96-.21-.44-.43-.87-.66-1.3-.03-.05-.06-.1-.08-.15-.02-.02-.03-.05-.04-.07l-.43-.76c-.01-.02-.03-.04-.04-.06-.21-.37-.44-.74-.66-1.1-.06-.1-.12-.2-.18-.29l-.04-.06c-.02-.04-.05-.07-.07-.11-.19-.31-.4-.62-.62-.92-.23-.34-.48-.68-.72-1.02h-.01c-.34-.46-.68-.89-1.04-1.34h-.01c-.36-.44-.72-.82-1.11-1.27l-.06-.06-1.11-1.17c-.4-.41-.8-.8-1.23-1.17-.01-.02-.03-.03-.04-.04-.24-.22-.48-.44-.73-.65-.17-.15-.34-.29-.51-.43-.04-.04-.09-.07-.14-.11s-.09-.07-.14-.11c-.21-.17-.42-.34-.64-.5-.14-.11-.28-.22-.42-.33-.07-.05-.15-.1-.22-.15-.18-.14-.37-.26-.55-.39-.21-.15-.41-.3-.62-.44-.11-.07-.22-.14-.33-.21-.13-.09-.26-.19-.39-.27-.13-.08-.26-.15-.39-.23-.11-.07-.22-.15-.33-.22-.13-.08-.27-.15-.4-.23-.06-.03-.11-.06-.16-.1-.06-.04-.12-.07-.17-.1h-.01c-.04-.02-.08-.04-.11-.06-.1-.06-.21-.12-.31-.18-.11-.06-.21-.12-.32-.18-.07-.04-.14-.07-.21-.11-.43-.24-.87-.46-1.31-.67-.52-.25-1.04-.49-1.58-.71-1.32-.57-2.68-1.05-4.07-1.44-.29-.08-.57-.16-.86-.23-2.54-.66-5.19-1.04-7.92-1.11-.15,0-.31-.01-.47-.01H.04C.15,119.53,2.27,131.13,6.09,141.93c1.25,3.56,2.69,7.02,4.3,10.39,10.47,21.87,28.2,39.6,50.07,50.07Z"/>'
    '<path style="fill:#FFF" d="M637.31,70.81c-19.55,0-37.25-7.93-50.06-20.74-12.82-12.81-20.74-30.51-20.74-50.07h-70.81v141.61h0s70.81,0,70.81,0c19.55,0,37.25,7.93,50.06,20.74,12.82,12.81,20.74,30.51,20.74,50.07h70.81V70.81h0s-70.81,0-70.81,0Z"/>'
    '<path style="fill:#FFF" d="M849.75,0h0c-58.66,0-106.21,47.55-106.21,106.21v141.62c0,58.66,47.55,106.21,106.21,106.21h0c58.66,0,106.21-47.55,106.21-106.21V106.21C955.96,47.55,908.41,0,849.75,0ZM849.75,283.23c-19.55,0-35.4-15.85-35.4-35.4h0s0-.02,0-.02v-70.78s0,0,0,0h0s0-.02,0-.02v-70.78s0,0,0,0h0c0-19.56,15.85-35.41,35.4-35.41h0c19.55,0,35.4,15.84,35.4,35.39v141.64c0,19.55-15.85,35.39-35.4,35.39h0Z"/>'
    '<path style="fill:#FFF" d="M460.29,105.61c-.1-12.08-2.22-23.69-6.04-34.48-1.25-3.56-2.69-7.03-4.3-10.4-5.24-10.93-12.29-20.83-20.76-29.3-8.48-8.48-18.38-15.53-29.31-20.76-3.37-1.61-6.83-3.05-10.39-4.3-10.8-3.82-22.4-5.94-34.49-6.04-.3-.01-.61-.01-.91-.01s-.61,0-.91.01c-12.09.1-23.7,2.22-34.5,6.05-3.55,1.24-7.02,2.68-10.39,4.29-10.93,5.24-20.83,12.29-29.31,20.76-8.48,8.48-15.53,18.38-20.76,29.31-1.61,3.37-3.05,6.83-4.3,10.39-3.82,10.8-5.94,22.4-6.04,34.49-.01.3-.01.6-.01.9v247.84h70.81v-106.22c0-19.55,15.85-35.4,35.4-35.4,9.78,0,18.63,3.96,25.04,10.37,6.41,6.41,10.37,15.25,10.37,25.03v106.22h70.81V106.52c0-.3,0-.61-.01-.91ZM389.49,106.53c0,19.56-15.85,35.4-35.4,35.4s-35.41-15.85-35.41-35.4h0c.01-19.56,15.85-35.4,35.41-35.4s35.4,15.84,35.4,35.39h0Z"/>'
    '</svg>'
)

THEATER = {
    "id":          "saoluiz",
    "name":        "São Luiz Teatro Municipal",
    "short":       "São Luiz",
    "color":       "#1a73e8",
    "city":        "Lisboa",
    "address":     "Rua António Maria Cardoso, 38, 1200-027 Lisboa",
    "site":        "https://www.teatrosaoluiz.pt",
    "programacao": "https://www.teatrosaoluiz.pt/programacao/",
    "lat":         38.7098,
    "lng":         -9.1421,
    "salas":       ["Grande Sala", "Sala Estúdio"],
    "aliases":     ["são luiz", "sao luiz", "teatro são luiz", "teatro municipal são luiz", "saoluiz"],
    "description": "O São Luiz Teatro Municipal é um dos mais emblemáticos teatros de Lisboa, com programação diversa de teatro, dança e performance. Situado no Chiado.",
    "logo_svg":    _LOGO_SVG,   # SVG inline (definido abaixo como constante)
    "favicon_url": "https://www.teatrosaoluiz.pt/favicon.ico",
    "facade_url":  None,        # confirmar URL real antes de preencher
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]


def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen, events = set(), []

    for a in soup.find_all("a", href=re.compile(r"/espetaculo/")):
        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)

        # Extrair dados disponíveis no card da listagem
        card_data = _extract_card_data(a)

        ev = _scrape_event(full, card_data)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[São Luiz] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Extracção de dados do card na listagem
# ─────────────────────────────────────────────────────────────

def _extract_card_data(a_tag) -> dict:
    """
    Extrai campos disponíveis directamente no card da listagem:
    title, subtitle, category (raw), dates_label, schedule.

    Suporta duas estruturas:
      1. Cards (div.card.event-item): todos os spans dentro do <a>
      2. Calendário inline (div.calendar-day): spans irmãos do <a>
    """
    data = {}

    # Título
    title_el = a_tag.select_one("span.title, h2, h3")
    if title_el:
        data["title"] = title_el.get_text(strip=True)

    # Subtítulo / companhia (span.subtitle ou span.company dentro do card)
    sub_el = a_tag.select_one("span.subtitle, span.company, span.author")
    if sub_el:
        data["subtitle"] = sub_el.get_text(strip=True)

    # Categoria raw — dentro do <a> (caso 1)
    cat_el = a_tag.select_one("span.category")
    if cat_el:
        data["category_raw"] = cat_el.get_text(strip=True).lower()
    else:
        # Caso 2 — span.category fora do <a> (estrutura calendário)
        parent = a_tag.parent
        if parent:
            container = parent.parent
            if container:
                sibling = container.select_one("span.category")
                if sibling:
                    data["category_raw"] = sibling.get_text(strip=True).lower()

    # Datas no card
    date_el = a_tag.select_one("span.dates, span.date, [class*='date']")
    if date_el:
        data["dates_label_raw"] = date_el.get_text(strip=True)

    # Horário no card
    time_el = a_tag.select_one("span.time, span.hour, span.horario, [class*='hour'], [class*='time']")
    if time_el:
        data["schedule_raw"] = time_el.get_text(strip=True)

    return data


# ─────────────────────────────────────────────────────────────
# Scraping da página individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str, card_data: dict) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    raw  = r.text

    # ── Título ──────────────────────────────────────────────
    title = card_data.get("title", "")
    if not title:
        title_el = soup.select_one("h1")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # ── Subtítulo / autor / companhia ───────────────────────
    subtitle = card_data.get("subtitle", "")
    if not subtitle:
        h1 = soup.select_one("h1")
        if h1:
            sub_el = h1.find_next_sibling()
            if sub_el:
                sub = sub_el.get_text(strip=True)
                if sub and len(sub) < 120 and not re.match(
                    r"^(©|COMPRAR|BILHETE|DATAS|LOCAL|DURA|PRE[ÇC]O|CLASSI|ACESSI)",
                    sub, re.IGNORECASE,
                ):
                    subtitle = sub

    # ── Categoria ───────────────────────────────────────────
    # Prioridade: card → breadcrumb da página → "Outro"
    category_raw = card_data.get("category_raw", "")
    if not category_raw:
        bc = soup.select_one(".breadcrumbs, [class*='breadcrumb']")
        if bc:
            m = re.search(
                r"\b(teatro|m[uú]sica|dan[çc]a|circo|performance|[oó]pera|"
                r"pensamento|exposi[çc][aã]o|visita|espa[çc]o p[uú]blico|"
                r"literatura|infanto.juvenil|teatro musical)\b",
                bc.get_text(" "), re.IGNORECASE,
            )
            if m:
                category_raw = m.group(1).lower()
    category = normalize_category(category_raw) if category_raw else "Outro"

    # ── Campos estruturados (spans.subtitle na página) ──────
    fields      = _parse_subtitle_fields(soup)
    dates_label = fields.get("datas_label", card_data.get("dates_label_raw", ""))
    schedule    = fields.get("schedule", card_data.get("schedule_raw", ""))
    sala        = fields.get("local", "")
    duration    = fields.get("duracao", "")
    price_info  = fields.get("preco", "")
    age_rating  = fields.get("classificacao", "")
    accessibility_raw = fields.get("acessibilidade", "")

    # ── Datas ───────────────────────────────────────────────
    date_start, date_end = _parse_dates_from_field(dates_label)

    # fallback: datas do card
    if not date_start and card_data.get("dates_label_raw"):
        date_start, date_end = _parse_dates_from_field(card_data["dates_label_raw"])

    # ── Duração ─────────────────────────────────────────────
    duration_min = None
    if duration:
        m = re.search(r"(\d+)\s*min", duration, re.IGNORECASE)
        if m:
            duration_min = int(m.group(1))
    if not duration_min:
        # tentar no corpo da página
        m = re.search(r"(\d+)\s*min(?:utos)?", raw, re.IGNORECASE)
        if m:
            duration_min = int(m.group(1))
            if not duration:
                duration = f"{duration_min} min"

    # ── Classificação etária ─────────────────────────────────
    age_min = None
    if age_rating:
        m = re.search(r"[Mm]\s*/?\s*(\d+)", age_rating)
        if m:
            age_min = int(m.group(1))
    if not age_rating:
        m = re.search(r"[Mm]\s*/?\s*(\d+)", raw)
        if m:
            age_rating = m.group(0)
            age_min = int(m.group(1))

    # ── Preços ──────────────────────────────────────────────
    price_min = price_max = None
    if not price_info:
        # procurar no corpo
        m_free = re.search(r"entrada\s+livre|gratuito|free\s+entry", raw, re.IGNORECASE)
        if m_free:
            price_info = "Entrada livre"
        else:
            prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", raw)
            if prices:
                vals = sorted({float(p.replace(",", ".")) for p in prices})
                price_min = vals[0]
                price_max = vals[-1]
                price_info = f"{price_min:.0f}€" if price_min == price_max else f"{price_min:.0f}€ – {price_max:.0f}€"
    else:
        # extrair min/max do price_info já capturado
        m_free = re.search(r"entrada\s+livre|gratuito", price_info, re.IGNORECASE)
        if m_free:
            price_min = 0.0
        else:
            prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", price_info)
            if prices:
                vals = sorted({float(p.replace(",", ".")) for p in prices})
                price_min = vals[0]
                price_max = vals[-1]

    # ── Acessibilidade ──────────────────────────────────────
    accessibility = []
    if accessibility_raw:
        accessibility = [a.strip() for a in re.split(r"[,/|;\n]+", accessibility_raw) if a.strip()]

    # ── Imagem ──────────────────────────────────────────────
    image = None
    raw_img = _get_image_url(soup, raw)
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Bilhetes ────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("saoluiz.bol.pt", "bol.pt/Comprar", "ticketline", "bilhete", "comprar")):
            ticket_url = href
            break
    if not ticket_url:
        m = re.search(r"href='(https?://[^']*(?:saoluiz\.bol\.pt|bol\.pt/Comprar|ticketline)[^']*)'", raw)
        if m:
            ticket_url = m.group(1)

    # ── Sinopse ─────────────────────────────────────────────
    synopsis = ""
    # Tentar og:description primeiro (mais limpa)
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        og_text = og_desc.get("content", "").strip()
        # Rejeitar descrições genéricas do site
        if og_text and len(og_text) > 60 and "programação" not in og_text.lower()[:40]:
            synopsis = og_text

    # fallback: parágrafos do <main> / .event-description
    if not synopsis:
        desc_el = soup.select_one(".event-description, section.event-description, main article, .entry-content")
        if desc_el:
            for p in desc_el.select("p"):
                t = p.get_text(strip=True)
                if len(t) > 60:
                    synopsis += (" " if synopsis else "") + t
                    if len(synopsis) > 1000:
                        break

    # ── Ficha técnica ────────────────────────────────────────
    technical_sheet = _parse_ficha(soup)

    # ── Montar evento ────────────────────────────────────────
    ev = {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
        "source_url":      url,
        "date_start":      date_start,
    }

    # Campos opcionais — só incluir se tiverem valor
    if subtitle:
        ev["subtitle"] = subtitle
    if date_end:
        ev["date_end"] = date_end
    if dates_label:
        ev["dates_label"] = dates_label
    if schedule:
        ev["schedule"] = schedule
    if date_start:
        ev["sessions"] = build_sessions(date_start, date_end, schedule)
    if synopsis:
        ev["synopsis"] = truncate_synopsis(synopsis)
    if image:
        ev["image"] = image
    if ticket_url:
        ev["ticket_url"] = ticket_url
    if price_info:
        ev["price_info"] = price_info
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max
    if duration:
        ev["duration"] = duration
    if duration_min is not None:
        ev["duration_min"] = duration_min
    if age_rating:
        ev["age_rating"] = age_rating
    if age_min is not None:
        ev["age_min"] = age_min
    if sala:
        ev["sala"] = sala
    if accessibility:
        ev["accessibility"] = accessibility
    if technical_sheet:
        ev["technical_sheet"] = technical_sheet

    return ev


# ─────────────────────────────────────────────────────────────
# Parsing dos campos estruturados (spans.subtitle na página)
# ─────────────────────────────────────────────────────────────

def _parse_subtitle_fields(soup) -> dict:
    result    = {}
    LABEL_MAP = {
        "DATAS E HORÁRIOS": "datas_label",
        "DATAS":            "datas_label",
        "LOCAL":            "local",
        "DURAÇÃO":          "duracao",
        "PREÇO":            "preco",
        "CLASSIFICAÇÃO":    "classificacao",
        "ACESSIBILIDADE":   "acessibilidade",
    }
    for span in soup.select("span.subtitle"):
        label_raw = span.get_text(strip=True).upper()
        key       = LABEL_MAP.get(label_raw)
        if not key:
            continue
        container = span.parent
        if not container:
            continue
        full_text = container.get_text("\n", strip=True)
        value     = full_text[len(span.get_text(strip=True)):].strip()
        value     = re.sub(r"\n{3,}", "\n\n", value).strip()
        if value:
            result[key] = value

    # Separar horário das datas quando vêm juntos no mesmo campo
    if "datas_label" in result:
        lines = [l.strip() for l in result["datas_label"].splitlines() if l.strip()]
        if lines:
            result["datas_label"] = lines[0]
            if len(lines) > 1:
                result["schedule"] = "\n".join(lines[1:])

    return result


def _parse_dates_from_field(dates_label: str) -> tuple[str, str]:
    if not dates_label:
        return "", ""
    date_start, date_end = parse_date_range(dates_label)
    if date_start:
        return date_start, date_end
    d = parse_date(dates_label)
    return d, d


# ─────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(soup) -> dict:
    ficha   = {}
    tech_el = soup.select_one(".event-tech-details")
    if not tech_el:
        return ficha

    text  = tech_el.get_text(" ")
    spans = tech_el.select("span.subtitle")
    if not spans:
        return ficha

    positions = []
    for span in spans:
        label = span.get_text(strip=True)
        key   = _normalise_ficha_key(label)
        if not key:
            continue
        idx = text.find(label)
        if idx >= 0:
            positions.append((idx, idx + len(label), key))

    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        if key not in ("coprodução", "parceria", "apoio"):
            value = re.split(
                r"\s+COPRODUÇÃO\b|\s+PARCERIA\b|\s+APOIO\b|\s+AGRADECIMENTOS\b",
                value, flags=re.IGNORECASE,
            )[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _normalise_ficha_key(label: str) -> str | None:
    label_up = label.upper().strip()
    KEY_MAP  = [
        ("TEXTO E ENCENAÇÃO",        "texto_encenação"),
        ("TEXTO",                    "texto"),
        ("ENCENAÇÃO",                "encenação"),
        ("DRAMATURGIA",              "dramaturgia"),
        ("DIREÇÃO ARTÍSTICA",        "direção"),
        ("DIREÇÃO DE PRODUÇÃO",      "direção_produção"),
        ("DIREÇÃO",                  "direção"),
        ("TRADUÇÃO",                 "tradução"),
        ("ADAPTAÇÃO",                "adaptação"),
        ("CENOGRAFIA E FIGURINOS",   "cenografia"),
        ("ESPAÇO CÉNICO",            "cenografia"),
        ("CENOGRAFIA",               "cenografia"),
        ("FIGURINOS",                "figurinos"),
        ("DESENHO DE LUZ",           "luz"),
        ("ILUMINAÇÃO",               "luz"),
        ("MÚSICA E ESPAÇO SONORO",   "música"),
        ("MÚSICA E DESENHO DE SOM",  "música"),
        ("DESENHO DE SOM",           "som"),
        ("SONOPLASTIA",              "som"),
        ("MÚSICA",                   "música"),
        ("COMPOSIÇÃO",               "música"),
        ("COREOGRAFIA",              "coreografia"),
        ("INTERPRETAÇÃO",            "interpretação"),
        ("ELENCO",                   "interpretação"),
        ("PRODUÇÃO EXECUTIVA",       "produção"),
        ("PRODUÇÃO E COMUNICAÇÃO",   "produção"),
        ("PRODUÇÃO",                 "produção"),
        ("COPRODUÇÃO",               "coprodução"),
        ("ASSISTENTE DE ENCENAÇÃO",  "ass_encenação"),
        ("ASSISTÊNCIA DE ENCENAÇÃO", "ass_encenação"),
    ]
    for label_key, mapped in KEY_MAP:
        if label_up == label_key:
            return mapped
    return None


# ─────────────────────────────────────────────────────────────
# Imagem
# ─────────────────────────────────────────────────────────────

def _get_image_url(soup, raw: str) -> str:
    # og:image é a fonte mais fiável
    og = soup.find("meta", property="og:image")
    if og:
        src = og.get("content", "")
        if src.startswith("http"):
            return src

    # Imagens inline, excluindo assets do tema
    skip = {"blank", "logo", "tsl/icons", "tsl/assets", "lgp.svg", "ad.svg"}
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and src.startswith("http") and not any(s in src for s in skip) and len(src) > 30:
            return src

    # Lazy-load attributes
    for img in soup.find_all("img"):
        for attr in ["data-lazysrc", "data-src", "data-original"]:
            src = img.get(attr, "")
            if src and "blank" not in src and len(src) > 20:
                return src if src.startswith("http") else BASE + src

    # Fallback: regex no HTML cru
    pattern = r"https?://" + re.escape(IMG_DOMAIN) + r"/wp-content/uploads/[\w/._-]+\.(?:jpg|jpeg|png|webp)"
    m = re.search(pattern, raw)
    return m.group(0) if m else ""
