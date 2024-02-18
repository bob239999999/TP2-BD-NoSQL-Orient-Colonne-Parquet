import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq 
import pyarrow.compute as pc 

# Import des données depuis les fichiers CSV
villes_df = pd.read_csv('villes_virgule.csv')
academies_df = pd.read_csv('academies_virgule.csv')

print("--------------------------------1---------------------------------")
# 1. Convertions
# Fonction pour convertir une table en DataFrame
def table_to_dataframe(table):
    return table.to_pandas()

# Fonction pour convertir un DataFrame en table
def dataframe_to_table(dataframe):
    return pa.Table.from_pandas(dataframe)

# Fonction pour écrire une table au format Parquet
def table_to_parquet(table, file_path):
    pq.write_table(table, file_path)

# Fonction pour lire une table depuis un fichier Parquet
def parquet_to_table(file_path):
    return pq.read_table(file_path)

print("From DataFrame -> Table et vice versa : ")
# Conversion de DataFrame en Table
table_ville  = dataframe_to_table(villes_df)
table_academi = dataframe_to_table(academies_df)

print("From Table -> DataFrame et vice versa : ")
# Conversion de Table en DataFrame
dataframe_ville =  table_to_dataframe(table_ville)
dataframe_academy =  table_to_dataframe(table_academi)

# Test 
print(villes_df.equals(dataframe_ville)) 
print(academies_df.equals(dataframe_academy)) 

print("From Table -> Paquet et vice versa : ")
# Écriture des tables au format Parquet
paquet = table_to_parquet(table_ville, 'villes.parquet')
paquet = table_to_parquet(table_academi, 'academy.parquet')

# Lecture des tables depuis les fichiers Parquet
print("From Paquet-> Table et vice versa :")
table_ville_df = parquet_to_table('villes.parquet')
table_academi_df = parquet_to_table('academy.parquet')

# Test 
print(table_academi.equals(table_academi_df))
print(table_ville.equals(table_ville_df))

print("------------------------------2-----------------------------------")
# 2. Afficher le schéma d'une table
# Fonction pour afficher le schéma d'une table
def afficher_schema(table):
    print(table.schema)

# Test 
print('Shema de la table villes :')
afficher_schema(table_ville)
print('\nShema de la table academie :')
afficher_schema(table_academi)

print("--------------------------------3---------------------------------")
# 3. Afficher le contenu d'une colonne
# Fonction pour obtenir le contenu d'une colonne d'une table
def obtenir_colonne(table, col):
    return table[col]

# Test
print(obtenir_colonne(table_ville, 'nom'))  # Cambiado a cadena para corresponder con el nombre real de la columna

print("--------------------------------4---------------------------------")
# 4. Opérations ensemblistes et jointures
# Fonction pour calculer les statistiques d'une colonne
def statistiques_colonne(table, col):
    count = pc.count(table[col])
    count_distinct = pc.count_distinct(table[col])
    sum_value = pc.sum(table[col])
    min_value = pc.min(table[col])
    max_value = pc.max(table[col])
    return count, count_distinct, sum_value, min_value, max_value

# Test 
stats_population = statistiques_colonne(table_ville, 'nb_hab_2012')  # Cambiado a cadena para corresponder con el nombre real de la columna
print("Statistiques de la colonne 'population':")
print("Nombre d'éléments :", stats_population[0])
print("Nombre d'éléments distincts :", stats_population[1])
print("Somme des valeurs :", stats_population[2])
print("Valeur minimale :", stats_population[3])
print("Valeur maximale :", stats_population[4])

print("------------------------------------5---A--------------------------")
def filtrer_ville(table, ville):
    return table.filter(pa.compute.equal(table['nom'], ville))  # Cambiado a cadena para corresponder con el nombre real de la columna

def filtrer_par_departement(table, departement):
   filtered_table = table.filter(pa.compute.equal(table['dep'], departement))  # Cambiado a cadena para corresponder con el nombre real de la columna
   sorted_indices = pa.compute.sort_indices(filtered_table['nom'])
   sorted_table = filtered_table.take(sorted_indices)
   return sorted_table

# Test 
print(filtrer_ville(table_ville, "Annecy")) 
print("------------------------------------5-----B------------------------")
print(filtrer_par_departement(table_ville, "74"))  

print("--------------------------------------6---------------------------")
def nombre_moyen_habitants_2012(table):
    return pa.compute.mean(table['nb_hab_2012'])  # Cambiado a cadena para corresponder con el nombre real de la columna

def nombre_moyen_habitants_par_departement(table):
    table = table_to_dataframe(table)
    return table.groupby('dep').agg({'nb_hab_2012': 'mean'})

def nombre_moyen_habitants_departement_74(table):
    table = table_to_dataframe(table)
    return table[table['dep'] == '74'][['dep', 'nom', 'nb_hab_2012']] 

# Test 
print("---- Nombre Moyen habitants 2012 ----")
print(nombre_moyen_habitants_2012(table_ville))
print("---------- Nombre par departement --------")
print(nombre_moyen_habitants_par_departement(table_ville))
print("---------- Nombre Moyen Habitants Departement 74 --------")
print(nombre_moyen_habitants_departement_74(table_ville))


print("--------------------------------------7---------------------------")
# 7. Opérations ensemblistes et jointures
def villes_zone_vacances_A(table_villes, table_academies):
    try:
        # Convertir las tablas a DataFrames
        villes_df = table_to_dataframe(table_villes)
        academies_df = table_to_dataframe(table_academies)
        
        print(villes_df.head())
        print(academies_df.head())

        # Realizar la operación de join utilizando pandas
        joined_df = pd.merge(villes_df, academies_df, on='dep', how='inner')
        
        
        # Filtrar las villes de la zone de vacances A
        villes_zone_A = joined_df[joined_df['vacances'] == 'Zone A']
        
        return villes_zone_A
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def departements_zone_vacances_A_B(table_villes, table_academies):
    try:
        # Convertir la tabla de villes a DataFrame
        villes_df = table_to_dataframe(table_villes)
        academies_df = table_to_dataframe(table_academies)
        
        # Realizar la operación de join utilizando pandas
        joined_df = pd.merge(villes_df, academies_df, on='dep', how='inner')
        
        # Filtrar los départements de las zones de vacances A y B
        departements_A_B = joined_df[(joined_df['vacances'] == 'Zone A') | (joined_df['vacances'] == 'Zone B')]['dep'].drop_duplicates()
        
        return departements_A_B
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def nombre_villes_par_academie(table_villes, table_academies):
    try:
        # Convertir les tables en DataFrames
        villes_df = table_to_dataframe(table_villes)
        academies_df = table_to_dataframe(table_academies)
        
        # Réaliser l'opération de jointure en utilisant pandas
        joined_df = pd.merge(villes_df, academies_df, on='dep', how='inner')
        
        # Calculer le nombre de villes par académie
        resu = joined_df.groupby('academie').size()
        print(resu)
        return resu 
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Test pour la fonction villes_zone_vacances_A
print("Test pour villes_zone_vacances_A:")
print(villes_zone_vacances_A(table_ville, table_academi))  # Llamada a la fonction avec les tables comme arguments

# Test pour la fonction departements_zone_vacances_A_B
print("\nTest pour departements_zone_vacances_A_B:")
departements_A_B = departements_zone_vacances_A_B(table_ville, table_academi)  # Llamada a la fonction avec les tables comme arguments
print(departements_A_B)

# Test pour la fonction nombre_villes_par_academie
print("\nTest pour nombre_villes_par_academie:")
nombre_villes_academie = nombre_villes_par_academie(table_ville, table_academi)
print(nombre_villes_academie)




print("--------------------------------------8---------------------------")
import matplotlib.pyplot as plt

def nombre_villes_par_academie1(table_villes, table_academies):
    try:
        # Convertir les tables en DataFrames
        villes_df = table_to_dataframe(table_villes)
        academies_df = table_to_dataframe(table_academies)
        
        # Réaliser l'opération de jointure en utilisant pandas
        joined_df = pd.merge(villes_df, academies_df, on='dep', how='inner')
        
        # Calculer le nombre de villes par académie
        nombre_villes_par_academie = joined_df.groupby('academie').size()
        
        return nombre_villes_par_academie
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Test pour la fonction nombre_villes_par_academie
nombre_villes_academie = nombre_villes_par_academie1(table_ville, table_academi)

# Tracer l'histogramme
plt.figure(figsize=(10, 6))
nombre_villes_academie.plot(kind='bar', color='skyblue')
plt.title('Distribution du nombre de villes par académie')
plt.xlabel('Académie')
plt.ylabel('Nombre de villes')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
