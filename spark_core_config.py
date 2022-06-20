from googletrans import Translator
import pandas as pd

if __name__ == '__main__':
    # translator = Translator()
    # df = pd.DataFrame({'Spanish': ['piso', 'cama']})
    # df
    # # dataset['English'] = dataset['Spanish'].apply(translator.translate, src='en', dest='es').apply(getattr, args=('text',))
    # # df
    #
    # df = ['How are you doing today', 'Good morning, How are you ', 'I hope you are doing great']
    # translations = translator.translate(df, dest='hi')
    # for translation in translations:  # print every translation
    #     print(translation.text)

    # create a translator object
    translator = Translator()

    # use translate method to translate a string - by default, the destination language is english
    translated = translator.translate('Hola Mundo', src='es', dest='en')

    # the translate method returns an object
    print(translated)
    # Translated(src=es, dest=en, text=Hello World, pronunciation=Hello World, extra_data="{'translat...")

    # obtain translated string by using attribute .text
    translated.text
    # 'Hello World'
