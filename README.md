# 1
Nous avons commencé ce projet par prendre connaissance des bases de données. Nous devons travailler avec 3 bases de données différentes :
- alco-restuarant-violations.csv : liste des restaurants et des différentes inspections ayant eu lieu
-	geocoded_food_facilities.csv : liste des positions géographiques par restaurant
-	Food_facility_restaurant_violations.csv : liste des différents rapports ayant été écrit pour chaque inspection.
Selon les consignes, les deux dernières bases nécessitent un chargement local via notre système. Nous avons donc utilisé un chargement classique via pySpark. Cela nous a fait utiliser une première technologie vue en cours.

# 2
Concernant le 1er fichier, alco-restuarant-violations.csv, sont chargement à nécessité un chargement via Kafka. Cela n’a pas été simple pour nous, car nous n’avons pas beaucoup utilisé Kafka durant nos différents cursus. Nous avons passé beaucoup de temps sur ce sujet pour setup notre flux producer/consumer avec de simples messages. Une fois cela fait, nous avons dû envoyer des lignes de notre base de données et non plus des messages unitaires.
Une décision à été prise, nous avons choisi d’envoyer un csv par batch et de merge ensuite chaque batch suivant au batch précédent. Cela permet d’enregistrer chaque batch au format csv, et d’enregistrer un gros csv contenant tout les batch. Il est donc possible de revenir en arrière si l’on observe qu’un batch n’est pas passé. (Pour faire ces tests nous avons setup 10000 lignes par batch, avec un intervalle de 10 secondes). La techno Kafka a donc bien été utilisé. Cette méthode aurait pu être utilisé en situation réel, si le flux était continu, chaque fichier aurait été enregistré dans un répertoire toutes les 10 secondes et ainsi de suite, …

# 3
La dernière étape a été d’utiliser Spark afin de faire nos traitements de données. Créer une base d’une ligne par restaurant avec quelques infos utiles, notamment le type de restaurant, la ville, le code postal, ainsi que le nombre d’inspections que le restaurant à subit (métric utile)
Plusieurs bases structurées ont été générer via spark dans un répertoire nommé structured table.

No
Si nous avions eu le choix de la base de données, nous aurions surement pris une base de données lié à un jeu vidéo, comme League of Legends. 

Apache KAFKA et Spark ont été utilisé pour mener a bien ce projet.


# Rapport 


Nous avons commencé ce projet par prendre connaissance des bases de données. Nous devons travailler avec 3 bases de données différentes :
- alco-restuarant-violations.csv : liste des restaurants et des différentes inspections ayant eu lieu
-	geocoded_food_facilities.csv : liste des positions géographiques par restaurant
-	Food_facility_restaurant_violations.csv : liste des différents rapports ayant été écrit pour chaque inspection.
Selon les consignes, les deux dernières bases nécessitent un chargement local via notre système. Nous avons donc utilisé un chargement classique via pySpark. Cela nous a fait utiliser une première technologie vue en cours.
Concernant le 1er fichier, alco-restuarant-violations.csv, sont chargement à nécessité un chargement via Kafka. Cela n’a pas été simple pour nous, car nous n’avons pas beaucoup utilisé Kafka durant nos différents cursus. Nous avons passé beaucoup de temps sur ce sujet pour setup notre flux producer/consumer avec de simples messages. Une fois cela fait, nous avons dû envoyer des lignes de notre base de données et non plus des messages unitaires.
Une décision à été prise, nous avons choisi d’envoyer un csv par batch et de merge ensuite chaque batch suivant au batch précédent. Cela permet d’enregistrer chaque batch au format csv, et d’enregistrer un gros csv contenant tout les batch. Il est donc possible de revenir en arrière si l’on observe qu’un batch n’est pas passé. (Pour faire ces tests nous avons setup 10000 lignes par batch, avec un intervalle de 10 secondes). La techno Kafka a donc bien été utilisé. Cette méthode aurait pu être utilisé en situation réel, si le flux était continu, chaque fichier aurait été enregistré dans un répertoire toutes les 10 secondes et ainsi de suite, …

La dernière étape a été d’utiliser Spark afin de faire nos traitements de données. Créer une base d’une ligne par restaurant avec quelques infos utiles, notamment le type de restaurant, la ville, le code postal, ainsi que le nombre d’inspections que le restaurant à subit.
Plusieurs bases structurées ont été générer via spark dans un répertoire nommé structured table.
