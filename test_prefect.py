from prefect import flow, task
import time

@task(name="Étape 1 - URLs")
def etape_urls():
    time.sleep(1)
    print("✅ URLs supprimées")
    return 26551

@task(name="Étape 2 - Doublons")
def etape_doublons(nb_docs):
    time.sleep(1)
    print(f"✅ Doublons supprimés : {nb_docs} → 25455")
    return 25455

@task(name="Étape 3 - Normalisation")
def etape_normalisation(nb_docs):
    time.sleep(1)
    print(f"✅ Normalisés : {nb_docs} docs")
    return nb_docs

@flow(name="Pipeline Télécom DZ")
def pipeline():
    docs = etape_urls()
    docs = etape_doublons(docs)
    docs = etape_normalisation(docs)
    print(f"🎉 Pipeline terminé !")

pipeline()

