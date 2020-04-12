# practica1_sistemes_distribuits
Codi per al desenvolupament de la pràctica 1 de Sistemes Distribuïts

El programa implementat deixa decidir amb 4 accions diferents:
-	Treballar seqüencialment amb matrius de valors donat
-	Treballar amb matrius de 3x3 amb 3 workers
-	Treballar amb matrius de 3x3 amb 9 workers 
-	Treballar amb matrius de mida donada per l’usuari i workers = files de la matriu

1.-  El treball de manera seqüencial es genera una matriu aleatòria de valors, aquestes matrius senceres  es pujen al COS del ibm, es crida amb la funció call_async() que aquesta es descarrega les dues matrius i les multiplica.

 

2.- El treball de manera que agafem dos matrius de 3x3 i es multipliquen per 3 “workers”, en aquest cas divideixo les multiplicacions per a que ho faci 3. La matriu iterdata la he definit de la següent manera: 
iterdata = [(0,0,workers), (1,0,workers), (2,0,workers)]
En aquest cas passo en primera opció la fila que s’ha de multiplicar, la segona la columna i la tercera el numero de “workers”. En aquest cas les columnes no importen ja que en aquest cas cada “workers”calcula una fila de la matriu final. 
 

3.- Per a calcular matrius de 3x3 amb 9 workers es passa a la funció multiplicació la següent iterdata:

iterdata = [(0,0,workers), (0,1,workers), (0,2,workers), (1, 0,workers), (1, 1,workers), (1, 2,workers), (2, 0,workers), (2, 1,workers), (2, 2,workers)]


En que cada “worker” li passa la fila i la columna per a obtenir un valor de la matriu final, en aquest cas tant la fila com la columna son importants. En la multiplicació s’agafa cada valor de la matriu i s’ajunta en la funció juntar() explicada més endavant. 
En aquest cas el codi per multiplicar.

 

4.- Quan es demana al usuari el valor de les matrius es l’opció més útil. En aquest cas demana al usuari un sol valor, del qual es creen dos matrius aleatòries de mida valor x valor. Quan es crida a la funció map_reduce() es genera una funció d’iterdata que pot anar d’acord amb els valors introduïts amb el usuari, es fa la multiplicació com en el cas de 3x3 amb 3 workers, cada “worker ” es responsable de calcular una fila de la matriu final. En aquest cas la funció juntar () junta totes les solucions que retorna la funció multiplicació:

 

