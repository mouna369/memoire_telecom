# Analyse de sentiment
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

def preparer_features(df, colonne_texte):
    '''Prépare les features pour l'analyse de sentiment'''
    tokenizer = Tokenizer(inputCol=colonne_texte, outputCol="mots")
    df_tokenized = tokenizer.transform(df)
    
    hashingTF = HashingTF(inputCol="mots", outputCol="tf", numFeatures=10000)
    df_tf = hashingTF.transform(df_tokenized)
    
    idf = IDF(inputCol="tf", outputCol="features")
    df_features = idf.fit(df_tf).transform(df_tf)
    
    return df_features
