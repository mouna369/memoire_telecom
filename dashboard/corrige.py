# nettoyage_complet.py
import os
import re

print("🔧 NETTOYAGE COMPLET DES DOUBLONS")
print("=" * 60)

pages_dir = "pages"
files_modified = []

# Parcourir tous les fichiers pages
for filename in os.listdir(pages_dir):
    if filename.endswith('.py'):
        filepath = os.path.join(pages_dir, filename)
        
        # Lire le fichier
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Vérifier si le fichier contient st.sidebar
        if 'st.sidebar' in content:
            print(f"\n📝 Nettoyage de {filename}...")
            
            # Supprimer TOUTES les lignes avec st.sidebar
            lines = content.split('\n')
            new_lines = []
            in_sidebar_block = False
            
            for line in lines:
                # Détecter le début d'un bloc sidebar
                if 'st.sidebar' in line or 'with st.sidebar' in line:
                    in_sidebar_block = True
                    print(f"   Suppression: {line[:50]}...")
                    continue
                
                # Si on est dans un bloc sidebar, ignorer jusqu'à la fin
                if in_sidebar_block:
                    if line.strip() == '' or line.strip().startswith('#') or 'st.markdown' in line:
                        continue
                    if 'st.sidebar' not in line and 'with st.sidebar' not in line:
                        # Sortie du bloc
                        in_sidebar_block = False
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            # Réécrire le fichier
            new_content = '\n'.join(new_lines)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            files_modified.append(filename)
            print(f"   ✅ {filename} nettoyé")

print("\n" + "=" * 60)
print(f"📊 RÉSUMÉ: {len(files_modified)} fichier(s) modifié(s)")
for f in files_modified:
    print(f"   - {f}")

print("\n" + "=" * 60)
print("🚀 PROCHAINES ÉTAPES:")
print("1. Redémarrez Streamlit")
print("2. Videz le cache: streamlit cache clear")
print("3. Relancez: streamlit run app.py")