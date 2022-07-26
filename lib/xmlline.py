import html
import re

def getAttributes(tag):
    return {name:html.unescape(value) for name, value in re.findall(r'([a-zA-Z0-9]+)="([^"]*)"',tag)}
