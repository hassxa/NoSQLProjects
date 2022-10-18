//Analizar la colección
var filter = {}
var projection = {}
db.movies.find(filter, projection)

//Contar el número de películas
db.movies.count()

//Insertar una película
var movie = ({
    "title":"The Devil Made Me Do It", 
    "year":2021,
    "cast":[
        "Patrick Wilson",
        "Vera Farmiga", 
        "Ruairi O'Connor",
        "Sarah Catherine", 
        "Julian Hilliard", 
        "Sterling Jerins"
        ],
    "genres":["Biographical", "Supernatural horror"]
})
//db.movies.insertOne(movie)

//Eliminar la película insertada anteriormente
db.movies.deleteOne(movie)

//Contar número de películas que tienen el valor "and" entre sus actores 
var filter = {cast:{$eq:"and"}}
db.movies.count(filter)

//Extraer el valor "and" de los arrays que lo contengan 
var filter = {cast:{$eq:"and"}}
var update = {$pull:{cast:{$eq:"and"}}}
var options = {multi:true}
db.movies.update(filter, update, options)

//Contar cuántas películas no tienen actores 
var filter = {cast:{$size:0}}
db.movies.count(filter)

//Actualizar los arrays cast vacíos con valores "Undefined" 
var filter = {cast:{$size:0}}
var update = {$push:{cast:"Undefined"}}
var options = {multi:true}
db.movies.update(filter, update, options)

//Contar las películas que no tienen género 
var filter = {genres:{$size:0}}
db.movies.count(filter)

//Actualizar los arrays genres vacíos con valores "Undefined" 
var filter = {genres:{$size:0}}
var update = {$push:{genres:"Undefined"}}
var options = {multi:true}
db.movies.update(filter, update, options)

//Mostrar el año más reciente sobre el total de películas
var filter = {}
var projection = {_id:0, year:1}
db.movies.find(filter, projection).sort({year:-1}).limit(1)

//Mostrar la cantidad de películas realizadas en los últimos veinte años
var variable_1 = {$max:"$year"}
var variable_2 = {$max:{$subtract:[2018, 20]}}
var phase_1 = {$match:{$expr:{$gt:[variable_1, variable_2]}}}
var phase_2 = {$group:{"_id":null, "total":{$sum:1}}}
var stage = [phase_1, phase_2]
db.movies.aggregate(stage)

//Mostrar la cantidad de películas realizadas en la década de los sesenta
var phase_1 = {$match:{"year":{$gte:1960, $lte:1969}}}
var phase_2 = {$group:{"_id":null, "total":{$sum:1}}}
var stage = [phase_1, phase_2]
db.movies.aggregate(stage)

//Obtener el año en el que más películas se han realizado
var phase_1 = {$group:{"_id":"$year", "pelis":{$sum:1}}}
var phase_2 = {$sort:{"pelis":-1}}
var phase_3 = {$limit:1}
var stage = [phase_1, phase_2, phase_3]
db.movies.aggregate(stage)

//Obtener el año en el que menos películas se han realizado
var phase_1 = {$group:{"_id":"$year", "pelis":{$sum:1}}}
var phase_2 = {$sort:{"pelis":1}}
var phase_3 = {$limit:3}
var stage = [phase_1, phase_2, phase_3]
db.movies.aggregate(stage)

//Crear una nueva colección con el nombre actors 
var phase_1 = {$unwind:"$cast"}
var phase_2 = {$project:{_id:0, title:1, year:1, cast:1, genres:1}}
var phase_3 = {$out:"actors"}
var stage = [phase_1, phase_2, phase_3]
db.movies.aggregate(stage)

db.actors.count()

//Mostrar los cinco actores que han participado en mayor cantidad de películas 
var phase_1 = {$match:{cast:{$ne:"Undefined"}}}
var phase_2 = {$group:{_id:"$cast", cuenta:{$sum:1}}}
var phase_3 = {$sort:{"cuenta":-1}}
var phase_4 = {$limit:5}
var stage = [phase_1, phase_2, phase_3, phase_4]
db.actors.aggregate(stage)

//Mostrar las cinco películas en las que han participado mayor cantidad de actores y el total de actores en cada una de esas películas 
var phase_1 = {$group:{_id:{title:"$title", year:"$year"}, cuenta:{$sum:1}}}
var phase_2 = {$sort:{"cuenta":-1}}
var phase_3 = {$limit:5}
var stage = [phase_1, phase_2, phase_3]
db.actors.aggregate(stage)

//Mostrar los cinco primeros actores con la carrera más larga y la duración en años de la misma 
var phase_1 = {$match:{cast:{$ne:"Undefined"}}}
var phase_2 = {$group:{_id:"$cast", comienza:{$min:"$year"}, termina:{$max:"$year"}}}
var phase_3 = {$project:{_id:1, comienza:1, termina:1, años:{$subtract:["$termina", "$comienza"]}}}
var phase_4 = {$sort:{"años":-1}}
var phase_5 = {$limit:6}
var stage = [phase_1, phase_2, phase_3, phase_4, phase_5]
db.actors.aggregate(stage)

//Guardar una nueva colección con el nombre genres y contar la cantidad de documentos en ella 
var phase_1 = {$unwind:"$genres"}
var phase_2 = {$project:{_id:0, title:1, year:1, cast:1, genres:1}}
var phase_3 = {$out:"genres"}
var stage = [phase_1, phase_2, phase_3]
db.actors.aggregate(stage)

db.genres.count()

//Agrupar por año y género y mostrar los cinco primeros documentos que tienen mayor cantidad de películas distintas 
var phase_1 = {$group:{_id:{year:"$year", genre:"$genres"}, total:{$addToSet:"$title"}}}
var phase_2 = {$project:{_id:1, pelis:{$size:"$total"}}}
var phase_3 = {$sort:{pelis:-1}}
var phase_4 = {$limit:5}
var stage = [phase_1, phase_2, phase_3, phase_4]
db.genres.aggregate(stage)

//Mostrar los cinco primeros actores que han participado en mayor cantidad de géneros de películas y la lista de estos géneros por actor 
var phase_1 = {$match:{cast:{$ne:"Undefined"}}}
var phase_2 = {$group:{_id:"$cast", generos:{$addToSet:"$genres"}}}
var phase_3 = {$project:{_id:1, numgeneros:{$size:"$generos"}, generos:1}}
var phase_4 = {$sort:{numgeneros:-1}}
var phase_5 = {$limit:11}
var stage = [phase_1, phase_2, phase_3, phase_4, phase_5]
db.genres.aggregate(stage)

//Mostrar las cinco primeras películas y su año que han sido catalogadas en mayor cantidad de géneros mostrando estos géneros y el total 
var phase_1 = {$group:{_id:{title:"$title", year:"$year"}, generos:{$addToSet:"$genres"}}}
var phase_2 = {$project:{_id:1, numgeneros:{$size:"$generos"}, generos:1}}
var phase_3 = {$sort:{numgeneros:-1}}
var phase_4 = {$limit:6}
var stage = [phase_1, phase_2, phase_3, phase_4]
db.genres.aggregate(stage)

//Query libre sobre el pipeline de agregación 
var phase_1 = {$match:{cast:{$ne:"Undefined"}}}
var phase_2 = {$group:{_id:{actor:"$cast", year:"$year"}, pelis:{$sum:1}, titulos:{$addToSet:"$title"}}}
var phase_3 = {$sort:{pelis:-1}}
var phase_4 = {$limit:1}
var stage = [phase_1, phase_2, phase_3, phase_4]
db.actors.aggregate(stage)

//Query libre sobre el pipeline de agregación 
var phase_1 = {$match:{cast:"Clint Eastwood"}}
var phase_2 = {$project:{_id:0, title:1, edad:{$subtract:["$year", 1930]}}}
var stage = [phase_1, phase_2]
db.movies.aggregate(stage)

//Query libre sobre el pipeline de agregación 
var phase_1 = {$match:{cast:"Angelina Jolie"}}
var phase_2 = {$group:{_id:"$cast", año:{$addToSet:"$year"}}}
var phase_3 = {$project:{_id:1, pelicula_en_1998:{$in:[1998, "$año"]}, pelicula_en_2018:{$in:[2018, "$año"]}}}
var stage = [phase_1, phase_2, phase_3]
db.actors.aggregate(stage)