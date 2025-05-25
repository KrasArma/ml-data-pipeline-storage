import pandas as pd


def prepare_wiki(wiki):

    wiki.columns = ['Area', 'Area_km2', 'Area_percentage', 'Rank_Area', 'Population_2024', 
                    'Population_percentage', 'Rank_Population', 'Density_2024', 'Rank_Density']
    wiki['Area_km2'] = wiki['Area_km2'].str.replace(',', '.').astype(float)
    wiki['Area_percentage'] = wiki['Area_percentage'].str.replace(' %', '').str.replace(',', '.').str.replace('\xa0', '').str.replace('%', '').astype(float)
    wiki['Population_2024'] = wiki['Population_2024'].str.extract('(\d+)').replace({'\s+': ''}, regex=True).astype(int)
    wiki['Density_2024'] = wiki['Density_2024'].str.replace(r'[^\d.]', '', regex=True).astype(float)

    rename_and_map_dict = {
        'Центральный': ('ЦАО', 'CAO'),
        'Северный': ('САО', 'SAO'),
        'Северо-Восточный': ('СВАО', 'SVAO'),
        'Восточный': ('ВАО', 'VAO'),
        'Юго-Восточный': ('ЮВАО', 'UVAO'),
        'Южный': ('ЮАО', 'UAO'),
        'Юго-Западный': ('ЮЗАО', 'UZAO'),
        'Западный': ('ЗАО', 'ZAO'),
        'Северо-Западный': ('СЗАО', 'SZAO'),
        'Зеленоградский': ('ЗелАО', 'ZelAO'),
        'Троицкий': ('ТАО', 'TAO'),
        'Новомосковский': ('НАО', 'NAO')
    }

    wiki['Area'] = wiki['Area'].replace({k: v[0] for k, v in rename_and_map_dict.items()})
    wiki['lat_area'] = wiki['Area'].map({v[0]: v[1] for v in rename_and_map_dict.values()})

    area_dummies = pd.get_dummies(wiki['Area'], prefix='Area', drop_first=True)
    wiki = pd.concat([wiki, area_dummies], axis=1)
    return wiki


def prepare_cian(cian):
    cian['cargoLiftsCount']= cian['cargoLiftsCount'].fillna(0).astype(int)
    cian = cian.drop_duplicates()
    house_material_dummies = pd.get_dummies(cian['houseMaterialType'], prefix='houseMaterialType', drop_first=True)
    cian = cian.drop(columns=['houseMaterialType'])  
    cian = pd.concat([cian, house_material_dummies], axis=1) 
    areas = {
        'ЦАО': 'CAO',
        'САО': 'SAO',
        'СВАО': 'SVAO',
        'ВАО': 'VAO',
        'ЮВАО': 'UVAO',
        'ЮАО': 'UAO',
        'ЮЗАО': 'UZAO',
        'ЗАО': 'ZAO',
        'СЗАО': 'SZAO',
        'ЗелАО': 'ZelAO',
        'ТАО': 'TAO',
        'НАО': 'NAO'
    }
    cian['lat_area'] = cian['Area'].map(areas)
    return cian 