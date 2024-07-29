from googletrans import Translator
translator = Translator()

def translate(input_text):
    translated = translator.translate(input_text, dest='en')
    return translated.text